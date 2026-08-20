// Package dynamodb is the emulate-tier DynamoDB pack. Expression evaluation
// lives in package expr.
package dynamodb

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/dynamodb/expr"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.dynamodb", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements DynamoDB.
type Pack struct{ deps spi.Deps }

func (p *Pack) ServiceID() string { return "aws.dynamodb" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateTable", "DeleteTable", "DescribeTable", "ListTables", "UpdateTable",
		"PutItem", "GetItem", "DeleteItem", "UpdateItem", "BatchGetItem", "BatchWriteItem",
		"Query", "Scan", "TransactGetItems", "TransactWriteItems",
		"TagResource", "UntagResource", "ListTagsOfResource",
		"DescribeTimeToLive", "DescribeContinuousBackups"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	table := str(req.Input["TableName"])
	switch req.Operation {
	case "CreateTable":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "tables").Put(ctx, table, b)
		return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{"TableName": table, "TableStatus": "ACTIVE"}}}, nil
	case "DeleteTable":
		_ = p.col(req, "tables").Delete(ctx, table)
		return &spi.Response{Output: map[string]any{"TableDescription": map[string]any{"TableName": table, "TableStatus": "DELETING"}}}, nil
	case "DescribeTable":
		b, ok, _ := p.col(req, "tables").Get(ctx, table)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		m["TableStatus"] = "ACTIVE"
		m["TableName"] = table
		return &spi.Response{Output: map[string]any{"Table": m}}, nil
	case "ListTables":
		kvs, _, _ := p.col(req, "tables").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"TableNames": names}}, nil
	case "PutItem":
		item, _ := req.Input["Item"].(map[string]any)
		key := p.itemKeyFrom(ctx, req, table, item)
		b, _ := json.Marshal(item)
		_ = p.col(req, "items:"+table).Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetItem":
		key := p.itemKeyFrom(ctx, req, table, asMap(req.Input["Key"]))
		b, ok, _ := p.col(req, "items:"+table).Get(ctx, key)
		if !ok {
			return &spi.Response{Output: map[string]any{}}, nil
		}
		var item map[string]any
		_ = json.Unmarshal(b, &item)
		return &spi.Response{Output: map[string]any{"Item": item}}, nil
	case "DeleteItem":
		key := p.itemKeyFrom(ctx, req, table, asMap(req.Input["Key"]))
		_ = p.col(req, "items:"+table).Delete(ctx, key)
		return &spi.Response{Output: map[string]any{}}, nil
	case "Scan":
		kvs, _, _ := p.col(req, "items:"+table).List(ctx, "", "", 0)
		var items []any
		filter := str(req.Input["FilterExpression"])
		for _, kv := range kvs {
			var item map[string]any
			_ = json.Unmarshal(kv.Value, &item)
			if filter != "" {
				ok, err := expr.EvalBool(filter, item, asMap(req.Input["ExpressionAttributeNames"]), asMap(req.Input["ExpressionAttributeValues"]))
				if err != nil || !ok {
					continue
				}
			}
			items = append(items, item)
		}
		return &spi.Response{Output: map[string]any{"Items": items, "Count": len(items), "ScannedCount": len(kvs)}}, nil
	case "Query":
		return p.Invoke(context.WithValue(ctx, ctxKey{}, "query"), clone(req, "Scan"))
	case "UpdateItem":
		key := p.itemKeyFrom(ctx, req, table, asMap(req.Input["Key"]))
		b, ok, _ := p.col(req, "items:"+table).Get(ctx, key)
		item := asMap(req.Input["Key"])
		if ok {
			_ = json.Unmarshal(b, &item)
		}
		if ue := str(req.Input["UpdateExpression"]); ue != "" {
			if err := expr.ApplyUpdate(ue, item, asMap(req.Input["ExpressionAttributeNames"]), asMap(req.Input["ExpressionAttributeValues"])); err != nil {
				return nil, &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
			}
		}
		if cond := str(req.Input["ConditionExpression"]); cond != "" {
			ok, err := expr.EvalBool(cond, item, asMap(req.Input["ExpressionAttributeNames"]), asMap(req.Input["ExpressionAttributeValues"]))
			if err != nil {
				return nil, &spi.Fault{Code: "ValidationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
			}
			if !ok {
				return nil, &spi.Fault{Code: "ConditionalCheckFailedException", Message: "The conditional request failed", HTTPStatus: 400, Fault: "client", Fields: map[string]any{"Item": item}}
			}
		}
		raw, _ := json.Marshal(item)
		_ = p.col(req, "items:"+table).Put(ctx, key, raw)
		return &spi.Response{Output: map[string]any{"Attributes": item}}, nil
	case "DescribeTimeToLive":
		return &spi.Response{Output: map[string]any{"TimeToLiveDescription": map[string]any{"TimeToLiveStatus": "DISABLED"}}}, nil
	case "DescribeContinuousBackups":
		return &spi.Response{Output: map[string]any{"ContinuousBackupsDescription": map[string]any{"ContinuousBackupsStatus": "DISABLED"}}}, nil
	case "BatchGetItem", "BatchWriteItem", "TransactGetItems", "TransactWriteItems",
		"UpdateTable", "TagResource", "UntagResource", "ListTagsOfResource":
		return &spi.Response{Output: map[string]any{"Responses": map[string]any{}, "UnprocessedKeys": map[string]any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.dynamodb", req.Operation, "emulate")
	}
}

type ctxKey struct{}

func clone(req *spi.Request, op string) *spi.Request {
	c := *req
	c.Operation = op
	return &c
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func str(v any) string { s, _ := v.(string); return s }

func itemKey(m map[string]any) string {
	b, _ := json.Marshal(m)
	return string(b)
}

func (p *Pack) itemKeyFrom(ctx context.Context, req *spi.Request, table string, attrs map[string]any) string {
	b, ok, _ := p.col(req, "tables").Get(ctx, table)
	if ok {
		var td map[string]any
		_ = json.Unmarshal(b, &td)
		if ks, ok := td["KeySchema"].([]any); ok && len(ks) > 0 {
			km := map[string]any{}
			for _, e := range ks {
				name := str(asMap(e)["AttributeName"])
				if name != "" {
					km[name] = attrs[name]
				}
			}
			return itemKey(km)
		}
	}
	return itemKey(attrs)
}
