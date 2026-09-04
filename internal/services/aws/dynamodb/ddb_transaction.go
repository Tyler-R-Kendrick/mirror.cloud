package dynamodb

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb/expr"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

type transactionWrite struct {
	kind, table, key string
	old, item        map[string]any
}

func (p *Pack) transactGetItems(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	items := asSlice(req.Input["TransactItems"])
	responses := make([]any, len(items))
	err := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(tx spi.ScopeTx) error {
		for i, raw := range items {
			get := asMap(asMap(raw)["Get"])
			table := tableFromStreamARN(str(get["TableName"]))
			definition, err := transactionTable(tx, table)
			if err != nil {
				return err
			}
			key := itemKeyFromDefinition(definition, asMap(get["Key"]))
			stored, ok, err := tx.Collection("items:" + table).Get(key)
			if err != nil {
				return err
			}
			response := map[string]any{}
			if ok {
				var item map[string]any
				_ = json.Unmarshal(stored, &item)
				response["Item"] = expr.Project(str(get["ProjectionExpression"]), item, asMap(get["ExpressionAttributeNames"]))
			}
			responses[i] = response
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{"Responses": responses}}, nil
}

func (p *Pack) transactWriteItems(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	items := asSlice(req.Input["TransactItems"])
	writes := make([]transactionWrite, len(items))
	replayed := false
	scope := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region)
	err := scope.Txn(ctx, func(tx spi.ScopeTx) error {
		payload, _ := json.Marshal(items)
		token := str(req.Input["ClientRequestToken"])
		if token != "" {
			if stored, ok, err := tx.Collection("ddbtx-tokens").Get(token); err != nil {
				return err
			} else if ok {
				var record map[string]any
				_ = json.Unmarshal(stored, &record)
				if int64(asInt(record["Expires"])) > p.deps.Clock.Now().Unix() {
					if str(record["Payload"]) != string(payload) {
						return &spi.Fault{Code: "IdempotentParameterMismatchException", Message: "A client request token was reused with different parameters", HTTPStatus: 400, Fault: "client"}
					}
					replayed = true
					return nil
				}
			}
		}

		reasons, failed := make([]any, len(items)), false
		seen := map[string]bool{}
		for i, raw := range items {
			reasons[i] = map[string]any{"Code": "None"}
			kind, input := transactionAction(asMap(raw))
			if input == nil {
				return &spi.Fault{Code: "ValidationException", Message: "TransactItems must contain exactly one action", HTTPStatus: 400, Fault: "client"}
			}
			table := tableFromStreamARN(str(input["TableName"]))
			definition, err := transactionTable(tx, table)
			if err != nil {
				return err
			}
			attributes := asMap(input["Key"])
			if kind == "Put" {
				attributes = asMap(input["Item"])
			}
			if err := validateItemKeyDefinition(definition, attributes); err != nil {
				return err
			}
			key := itemKeyFromDefinition(definition, attributes)
			if identity := table + "\x00" + key; seen[identity] {
				return &spi.Fault{Code: "ValidationException", Message: "Transaction request cannot include multiple operations on one item", HTTPStatus: 400, Fault: "client"}
			} else {
				seen[identity] = true
			}
			var old map[string]any
			if stored, ok, err := tx.Collection("items:" + table).Get(key); err != nil {
				return err
			} else if ok {
				_ = json.Unmarshal(stored, &old)
			}
			ok, err := conditionOK(input, old)
			if err != nil {
				return &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
			}
			if !ok {
				reason := map[string]any{"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"}
				if str(input["ReturnValuesOnConditionCheckFailure"]) == "ALL_OLD" && old != nil {
					reason["Item"] = old
				}
				reasons[i], failed = reason, true
			}
			item := cloneMap(old)
			switch kind {
			case "Put":
				item = cloneMap(asMap(input["Item"]))
			case "Update":
				if item == nil {
					item = cloneMap(asMap(input["Key"]))
				}
				if _, err := expr.ApplyUpdate(str(input["UpdateExpression"]), item, asMap(input["ExpressionAttributeNames"]), asMap(input["ExpressionAttributeValues"])); err != nil {
					return &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
				}
			case "Delete", "ConditionCheck":
				item = nil
			}
			writes[i] = transactionWrite{kind: kind, table: table, key: key, old: old, item: item}
		}
		if failed {
			codes := make([]string, len(reasons))
			for i, raw := range reasons {
				codes[i] = str(asMap(raw)["Code"])
			}
			return &spi.Fault{Code: "TransactionCanceledException", Message: "Transaction cancelled, please refer cancellation reasons for specific reasons [" + strings.Join(codes, ", ") + "]", HTTPStatus: 400, Fault: "client", Fields: map[string]any{"CancellationReasons": reasons}}
		}
		for _, write := range writes {
			if write.kind == "ConditionCheck" {
				continue
			}
			collection := tx.Collection("items:" + write.table)
			if write.kind == "Delete" {
				if err := collection.Delete(write.key); err != nil {
					return err
				}
			} else {
				encoded, _ := json.Marshal(write.item)
				if err := collection.Put(write.key, encoded); err != nil {
					return err
				}
			}
		}
		if token != "" {
			record, _ := json.Marshal(map[string]any{"Payload": string(payload), "Expires": p.deps.Clock.Now().Add(10 * time.Minute).Unix()})
			return tx.Collection("ddbtx-tokens").Put(token, record)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !replayed {
		for _, write := range writes {
			event := "MODIFY"
			if write.old == nil {
				event = "INSERT"
			}
			if write.kind == "Delete" {
				event = "REMOVE"
			}
			if write.kind != "ConditionCheck" {
				p.emitStream(ctx, req, write.table, event, write.item, write.old)
			}
		}
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

func transactionAction(item map[string]any) (string, map[string]any) {
	kind := ""
	var action map[string]any
	for _, candidate := range []string{"ConditionCheck", "Put", "Update", "Delete"} {
		if raw, exists := item[candidate]; exists {
			value, ok := raw.(map[string]any)
			if !ok {
				return "", nil
			}
			if action != nil {
				return "", nil
			}
			kind, action = candidate, value
		}
	}
	return kind, action
}

func transactionTable(tx spi.ScopeTx, table string) (map[string]any, error) {
	stored, ok, err := tx.Collection("tables").Get(table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "Requested resource not found", HTTPStatus: 400, Fault: "client"}
	}
	var definition map[string]any
	_ = json.Unmarshal(stored, &definition)
	return definition, nil
}
