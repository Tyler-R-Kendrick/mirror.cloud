// Package states is Step Functions-lite: SM records plus a Pass/Succeed/Fail/Wait/Task/Choice/Parallel walker.
package states

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	jsonata "github.com/jsonata-go/jsonata/v206"
	"github.com/parquet-go/parquet-go"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	internalrand "github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.states", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Step Functions-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.states" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateStateMachine", "UpdateStateMachine", "DeleteStateMachine", "DescribeStateMachine", "ListStateMachines",
		"PublishStateMachineVersion", "ListStateMachineVersions", "DeleteStateMachineVersion",
		"CreateStateMachineAlias", "DescribeStateMachineAlias", "ListStateMachineAliases", "UpdateStateMachineAlias", "DeleteStateMachineAlias",
		"StartExecution", "StartSyncExecution", "StopExecution", "DescribeExecution", "ListExecutions", "GetExecutionHistory",
		"DescribeStateMachineForExecution", "RedriveExecution",
		"DescribeMapRun", "ListMapRuns", "UpdateMapRun",
		"CreateActivity", "DeleteActivity", "DescribeActivity", "ListActivities", "GetActivityTask",
		"SendTaskSuccess", "SendTaskFailure", "SendTaskHeartbeat",
		"TestState", "ValidateStateMachineDefinition",
		"TagResource", "UntagResource", "ListTagsForResource",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	now := p.deps.Clock.Now().Unix()
	switch req.Operation {
	case "CreateStateMachine":
		name := first(req.Input, "name", "Name")
		if !validResourceName(name) {
			return nil, &spi.Fault{Code: "InvalidName", HTTPStatus: 400, Fault: "client"}
		}
		machineType := first(req.Input, "type", "Type")
		if machineType == "" {
			machineType = "STANDARD"
		}
		definition := first(req.Input, "definition", "Definition")
		if len(definition) < 1 || len(definition) > 1048576 || len(validateDefinition(definition, machineType)) > 0 {
			return nil, &spi.Fault{Code: "InvalidDefinition", HTTPStatus: 400, Fault: "client"}
		}
		roleARN := first(req.Input, "roleArn", "RoleArn")
		if !validRoleARN(roleARN) {
			return nil, &spi.Fault{Code: "InvalidArn", HTTPStatus: 400, Fault: "client"}
		}
		publish := inputBool(req.Input, "publish", "Publish")
		versionDescription := first(req.Input, "versionDescription", "VersionDescription")
		if len(versionDescription) > 256 || versionDescription != "" && !publish {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := p.smARN(req, name)
		if machineType != "STANDARD" && machineType != "EXPRESS" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		logging, _ := inputValue(req.Input, "loggingConfiguration", "LoggingConfiguration")
		tracing, _ := inputValue(req.Input, "tracingConfiguration", "TracingConfiguration")
		encryption, _ := inputValue(req.Input, "encryptionConfiguration", "EncryptionConfiguration")
		if logging != nil && !validLoggingConfiguration(logging) {
			return nil, &spi.Fault{Code: "InvalidLoggingConfiguration", HTTPStatus: 400, Fault: "client"}
		}
		if tracing != nil && !validTracingConfiguration(tracing) {
			return nil, &spi.Fault{Code: "InvalidTracingConfiguration", HTTPStatus: 400, Fault: "client"}
		}
		if encryption != nil && !validEncryptionConfiguration(encryption) {
			return nil, &spi.Fault{Code: "InvalidEncryptionConfiguration", HTTPStatus: 400, Fault: "client"}
		}
		rawTags, validTags := optionalSlice(req.Input, "tags", "Tags")
		if !validTags {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		tags, tagFault := validatedTags(rawTags)
		if tagFault != "" {
			return nil, &spi.Fault{Code: tagFault, HTTPStatus: 400, Fault: "client"}
		}
		if existing, found := getRecord(ctx, p.col(req, "sm"), name); found {
			if first(existing, "definition") != definition || first(existing, "type") != machineType ||
				!reflect.DeepEqual(existing["_logging"], logging) || !reflect.DeepEqual(existing["_tracing"], tracing) ||
				!reflect.DeepEqual(existing["_encryption"], encryption) || toBool(existing["_publish"]) != publish || first(existing, "_versionDescription") != versionDescription {
				return nil, &spi.Fault{Code: "StateMachineAlreadyExists", HTTPStatus: 400, Fault: "client"}
			}
			output := map[string]any{"stateMachineArn": arn, "creationDate": existing["creationDate"]}
			if versionARN := first(existing, "_publishedVersionArn"); versionARN != "" {
				output["stateMachineVersionArn"] = versionARN
			}
			return &spi.Response{Output: output}, nil
		}
		rec := map[string]any{
			"stateMachineArn": arn, "name": name, "definition": definition,
			"roleArn": roleARN, "type": machineType,
			"status": "ACTIVE", "creationDate": float64(now), "revisionId": p.deps.Rand.UUID(), "updateDate": float64(now),
			"_logging": logging, "_tracing": tracing, "_encryption": encryption, "_publish": publish, "_versionDescription": versionDescription,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sm").Put(ctx, name, b)
		if tags != nil {
			tagsJSON, _ := json.Marshal(tags)
			_ = p.col(req, "tag").Put(ctx, arn, tagsJSON)
		}
		output := map[string]any{"stateMachineArn": arn, "creationDate": float64(now)}
		if publish {
			published, err := p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "PublishStateMachineVersion", Input: map[string]any{"stateMachineArn": arn, "description": versionDescription}})
			if err != nil {
				return nil, err
			}
			output["stateMachineVersionArn"] = published.Output["stateMachineVersionArn"]
			rec["_publishedVersionArn"] = published.Output["stateMachineVersionArn"]
			b, _ = json.Marshal(rec)
			_ = p.col(req, "sm").Put(ctx, name, b)
		}
		return &spi.Response{Output: output}, nil
	case "UpdateStateMachine":
		name := smName(first(req.Input, "stateMachineArn", "StateMachineArn"))
		b, ok, _ := p.col(req, "sm").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		publish := inputBool(req.Input, "publish", "Publish")
		versionDescription := first(req.Input, "versionDescription", "VersionDescription")
		if len(versionDescription) > 256 || versionDescription != "" && !publish {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		changed := false
		if d := first(req.Input, "definition", "Definition"); d != "" {
			if len(d) > 1048576 || len(validateDefinition(d, first(rec, "type"))) > 0 {
				return nil, &spi.Fault{Code: "InvalidDefinition", HTTPStatus: 400, Fault: "client"}
			}
			rec["definition"] = d
			changed = true
		}
		if r := first(req.Input, "roleArn", "RoleArn"); r != "" {
			if !validRoleARN(r) {
				return nil, &spi.Fault{Code: "InvalidArn", HTTPStatus: 400, Fault: "client"}
			}
			rec["roleArn"] = r
			changed = true
		}
		for _, field := range []struct {
			lower, upper, stored, fault string
			valid                       func(any) bool
		}{
			{"loggingConfiguration", "LoggingConfiguration", "_logging", "InvalidLoggingConfiguration", validLoggingConfiguration},
			{"tracingConfiguration", "TracingConfiguration", "_tracing", "InvalidTracingConfiguration", validTracingConfiguration},
			{"encryptionConfiguration", "EncryptionConfiguration", "_encryption", "InvalidEncryptionConfiguration", validEncryptionConfiguration},
		} {
			if value, exists := inputValue(req.Input, field.lower, field.upper); exists {
				if !field.valid(value) {
					return nil, &spi.Fault{Code: field.fault, HTTPStatus: 400, Fault: "client"}
				}
				rec[field.stored], changed = value, true
			}
		}
		if changed {
			rec["revisionId"] = p.deps.Rand.UUID()
		}
		rec["updateDate"] = float64(now)
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "sm").Put(ctx, name, nb)
		output := map[string]any{"updateDate": float64(now), "revisionId": rec["revisionId"]}
		if publish {
			published, err := p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "PublishStateMachineVersion", Input: map[string]any{
				"stateMachineArn": rec["stateMachineArn"], "revisionId": rec["revisionId"], "description": versionDescription,
			}})
			if err != nil {
				return nil, err
			}
			output["stateMachineVersionArn"] = published.Output["stateMachineVersionArn"]
		}
		return &spi.Response{Output: output}, nil
	case "DeleteStateMachine":
		arn := first(req.Input, "stateMachineArn", "StateMachineArn")
		name := baseSMName(arn)
		if smName(arn) != name {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		if _, found := getRecord(ctx, p.col(req, "sm"), name); !found {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		_ = p.col(req, "sm").Delete(ctx, name)
		for _, collection := range []string{"ver", "alias"} {
			items, _, _ := p.col(req, collection).List(ctx, "", "", 0)
			for _, item := range items {
				if strings.HasPrefix(item.Key, name+":") || strings.HasPrefix(item.Key, arn+":") {
					_ = p.col(req, collection).Delete(ctx, item.Key)
				}
			}
		}
		_ = p.col(req, "tag").Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeStateMachine":
		arn := first(req.Input, "stateMachineArn", "StateMachineArn")
		name := baseSMName(arn)
		collection, key := "sm", name
		if versionNumber(arn) > 0 {
			collection, key = "ver", name+":"+strconv.Itoa(versionNumber(arn))
		}
		b, ok, _ := p.col(req, collection).Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if collection == "ver" {
			rec["stateMachineArn"], rec["name"], rec["status"] = arn, name, "ACTIVE"
		}
		for key := range rec {
			if strings.HasPrefix(key, "_") {
				delete(rec, key)
			}
		}
		return &spi.Response{Output: rec}, nil
	case "ListStateMachines":
		items := listRecords(ctx, p.col(req, "sm"), func(record map[string]any) (map[string]any, bool) {
			return map[string]any{"stateMachineArn": record["stateMachineArn"], "name": record["name"], "type": record["type"], "creationDate": record["creationDate"]}, true
		})
		return pagedResponse(req.Input, items, "stateMachines")
	case "PublishStateMachineVersion":
		arn := first(req.Input, "stateMachineArn", "StateMachineArn")
		name := baseSMName(arn)
		if smName(arn) != name || len(first(req.Input, "description", "Description")) > 256 {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := p.col(req, "sm").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var machine map[string]any
		_ = json.Unmarshal(b, &machine)
		revision := first(machine, "revisionId")
		if wanted := first(req.Input, "revisionId", "RevisionId"); wanted != "" && wanted != revision {
			return nil, &spi.Fault{Code: "ConflictException", HTTPStatus: 409, Fault: "client"}
		}
		versions, _, _ := p.col(req, "ver").List(ctx, name+":", "", 0)
		if len(versions) >= 1000 {
			return nil, &spi.Fault{Code: "ServiceQuotaExceededException", HTTPStatus: 400, Fault: "client"}
		}
		next := 1
		for _, version := range versions {
			var record map[string]any
			_ = json.Unmarshal(version.Value, &record)
			if first(record, "revisionId") == revision {
				return &spi.Response{Output: map[string]any{"stateMachineVersionArn": record["stateMachineVersionArn"], "creationDate": record["creationDate"]}}, nil
			}
			if number := versionNumber(first(record, "stateMachineVersionArn")); number >= next {
				next = number + 1
			}
		}
		baseARN := first(machine, "stateMachineArn")
		versionARN := baseARN + ":" + strconv.Itoa(next)
		record := map[string]any{
			"stateMachineVersionArn": versionARN, "creationDate": float64(now), "description": first(req.Input, "description", "Description"),
			"revisionId": revision, "definition": machine["definition"], "roleArn": machine["roleArn"], "type": machine["type"],
		}
		encoded, _ := json.Marshal(record)
		_ = p.col(req, "ver").Put(ctx, name+":"+strconv.Itoa(next), encoded)
		return &spi.Response{Output: map[string]any{"stateMachineVersionArn": versionARN, "creationDate": float64(now)}}, nil
	case "ListStateMachineVersions":
		name := baseSMName(first(req.Input, "stateMachineArn", "StateMachineArn"))
		if _, found := getRecord(ctx, p.col(req, "sm"), name); !found {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		versions, _, _ := p.col(req, "ver").List(ctx, name+":", "", 0)
		items := make([]any, 0, len(versions))
		for _, version := range versions {
			var record map[string]any
			_ = json.Unmarshal(version.Value, &record)
			items = append(items, map[string]any{"stateMachineVersionArn": record["stateMachineVersionArn"], "creationDate": record["creationDate"], "description": record["description"]})
		}
		sort.Slice(items, func(i, j int) bool {
			return versionNumber(first(items[i].(map[string]any), "stateMachineVersionArn")) > versionNumber(first(items[j].(map[string]any), "stateMachineVersionArn"))
		})
		return pagedResponse(req.Input, items, "stateMachineVersions")
	case "DeleteStateMachineVersion":
		arn := first(req.Input, "stateMachineVersionArn", "StateMachineVersionArn")
		versionKey := baseSMName(arn) + ":" + strconv.Itoa(versionNumber(arn))
		if versionNumber(arn) < 1 {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		if _, found := getRecord(ctx, p.col(req, "ver"), versionKey); !found {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		aliases, _, _ := p.col(req, "alias").List(ctx, "", "", 0)
		for _, alias := range aliases {
			var record map[string]any
			_ = json.Unmarshal(alias.Value, &record)
			for _, raw := range asSlice(record["routingConfiguration"]) {
				route, _ := raw.(map[string]any)
				if first(route, "stateMachineVersionArn", "StateMachineVersionArn") == arn {
					return nil, &spi.Fault{Code: "ConflictException", HTTPStatus: 409, Fault: "client"}
				}
			}
		}
		_ = p.col(req, "ver").Delete(ctx, versionKey)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateStateMachineAlias":
		name := first(req.Input, "name", "Name")
		routes := aliasRoutes(req.Input)
		baseARN, valid := p.validateAliasRoutes(ctx, req, routes)
		if !validAliasName(name) || !valid || len(first(req.Input, "description", "Description")) > 256 {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		aliasARN := baseARN + ":" + name
		if _, exists, _ := p.col(req, "alias").Get(ctx, aliasARN); exists {
			return nil, &spi.Fault{Code: "ConflictException", HTTPStatus: 409, Fault: "client"}
		}
		record := map[string]any{
			"stateMachineAliasArn": aliasARN, "name": name, "description": first(req.Input, "description", "Description"),
			"routingConfiguration": routes, "creationDate": float64(now), "updateDate": float64(now),
		}
		encoded, _ := json.Marshal(record)
		_ = p.col(req, "alias").Put(ctx, aliasARN, encoded)
		return &spi.Response{Output: map[string]any{"stateMachineAliasArn": aliasARN, "creationDate": float64(now)}}, nil
	case "DescribeStateMachineAlias":
		arn := first(req.Input, "stateMachineAliasArn", "StateMachineAliasArn")
		b, ok, _ := p.col(req, "alias").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var record map[string]any
		_ = json.Unmarshal(b, &record)
		return &spi.Response{Output: record}, nil
	case "ListStateMachineAliases":
		requestedARN := first(req.Input, "stateMachineArn", "StateMachineArn")
		baseARN := stateMachineBaseARN(requestedARN)
		if _, found := getRecord(ctx, p.col(req, "sm"), baseSMName(baseARN)); !found {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		aliases, _, _ := p.col(req, "alias").List(ctx, baseARN+":", "", 0)
		items := make([]any, 0, len(aliases))
		for _, alias := range aliases {
			var record map[string]any
			_ = json.Unmarshal(alias.Value, &record)
			if versionNumber(requestedARN) > 0 && !aliasReferences(record, requestedARN) {
				continue
			}
			items = append(items, map[string]any{"stateMachineAliasArn": record["stateMachineAliasArn"], "creationDate": record["creationDate"]})
		}
		sort.SliceStable(items, func(i, j int) bool {
			return toFloat(items[i].(map[string]any)["creationDate"]) > toFloat(items[j].(map[string]any)["creationDate"])
		})
		return pagedResponse(req.Input, items, "stateMachineAliases")
	case "UpdateStateMachineAlias":
		arn := first(req.Input, "stateMachineAliasArn", "StateMachineAliasArn")
		b, ok, _ := p.col(req, "alias").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var record map[string]any
		_ = json.Unmarshal(b, &record)
		routesValue, hasRoutes := inputValue(req.Input, "routingConfiguration", "RoutingConfiguration")
		description, hasDescription := inputValue(req.Input, "description", "Description")
		if !hasRoutes && !hasDescription || hasDescription && len(fmt.Sprint(description)) > 256 {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		if routes := asSlice(routesValue); hasRoutes {
			baseARN, valid := p.validateAliasRoutes(ctx, req, routes)
			if !valid || baseARN != stateMachineBaseARN(arn) {
				return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
			}
			record["routingConfiguration"] = routes
		}
		if description, exists := description, hasDescription; exists {
			record["description"] = description
		}
		record["updateDate"] = float64(now)
		encoded, _ := json.Marshal(record)
		_ = p.col(req, "alias").Put(ctx, arn, encoded)
		return &spi.Response{Output: map[string]any{"updateDate": float64(now)}}, nil
	case "DeleteStateMachineAlias":
		arn := first(req.Input, "stateMachineAliasArn", "StateMachineAliasArn")
		if _, found := getRecord(ctx, p.col(req, "alias"), arn); !found {
			return nil, &spi.Fault{Code: "ResourceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		_ = p.col(req, "alias").Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartExecution", "StartSyncExecution":
		arn := first(req.Input, "stateMachineArn", "StateMachineArn")
		if len(arn) < 1 || len(arn) > 256 || strings.Contains(arn, "/") {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		name := baseSMName(arn)
		sm, ok := p.resolveStateMachine(ctx, req, arn)
		if !ok {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		if req.Operation == "StartSyncExecution" && first(sm, "type", "Type") != "EXPRESS" {
			return nil, &spi.Fault{Code: "StateMachineTypeNotSupported", HTTPStatus: 400, Fault: "client"}
		}
		exName := first(req.Input, "name", "Name")
		if exName == "" {
			exName = p.deps.Rand.UUID()
		} else if !validResourceName(exName) {
			return nil, &spi.Fault{Code: "InvalidName", HTTPStatus: 400, Fault: "client"}
		}
		execARN := p.execARN(req, name, exName)
		inputJSON := first(req.Input, "input", "Input")
		if inputJSON == "" {
			inputJSON = "{}"
		}
		if len(inputJSON) > 262144 || !json.Valid([]byte(inputJSON)) {
			return nil, &spi.Fault{Code: "InvalidExecutionInput", HTTPStatus: 400, Fault: "client"}
		}
		traceHeader := first(req.Input, "traceHeader", "TraceHeader")
		if len(traceHeader) > 256 || !asciiString(traceHeader) {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		if req.Operation == "StartExecution" && first(sm, "type") == "STANDARD" {
			if existing, found := getRecord(ctx, p.col(req, "ex"), execARN); found {
				if first(existing, "status") == "RUNNING" && first(existing, "input") == inputJSON {
					return &spi.Response{Output: map[string]any{"executionArn": execARN, "startDate": existing["startDate"]}}, nil
				}
				return nil, &spi.Fault{Code: "ExecutionAlreadyExists", HTTPStatus: 400, Fault: "client"}
			}
		}
		in := parseJSON(inputJSON)
		def, _ := sm["definition"].(string)
		walkRequest := *req
		walkRequest.Input = maps.Clone(req.Input)
		walkRequest.Input["_stateMachineArn"] = p.smARN(req, name)
		walkRequest.Input["_executionArn"], walkRequest.Input["_executionName"] = execARN, exName
		walkRequest.Input["_executionInput"] = in
		walkRequest.Input["_executionStartTime"] = p.deps.Clock.Now().UTC().Format(time.RFC3339Nano)
		walkRequest.Input["_executionRoleArn"] = sm["roleArn"]
		walkRequest.Input["_executionRedriveCount"] = 0.0
		wr := p.walk(ctx, &walkRequest, def, "", in, nil)
		baseARN := p.smARN(req, name)
		for _, run := range wr.mapRuns {
			mapRunARN := run.arn
			if mapRunARN == "" {
				mapRunARN = p.mapRunARN(req, name, run.label)
			}
			run.record["mapRunArn"], run.record["executionArn"], run.record["stateMachineArn"] = mapRunARN, execARN, baseARN
			run.record["startDate"], run.record["stopDate"] = float64(now), float64(now)
			encoded, _ := json.Marshal(run.record)
			_ = p.col(req, "maprun").Put(ctx, mapRunARN, encoded)
		}
		ob, _ := json.Marshal(wr.out)
		rec := map[string]any{
			"executionArn": execARN, "stateMachineArn": baseARN, "name": exName, "status": wr.status,
			"startDate": float64(now), "input": inputJSON,
			"output": string(ob), "cause": wr.cause, "history": wr.hist, "definition": def,
			"roleArn": sm["roleArn"], "revisionId": sm["revisionId"], "stateMachineName": name, "type": sm["type"],
		}
		if traceHeader != "" {
			rec["traceHeader"] = traceHeader
		}
		if versionARN := first(sm, "_resolvedVersionArn"); versionARN != "" {
			rec["stateMachineVersionArn"] = versionARN
		}
		if aliasARN := first(sm, "_resolvedAliasArn"); aliasARN != "" {
			rec["stateMachineAliasArn"] = aliasARN
		}
		if wr.failedState != "" {
			rec["redriveState"], rec["redriveInput"] = wr.failedState, wr.failedInput
		}
		if wr.errorName != "" {
			rec["error"] = wr.errorName
		}
		if wr.status != "RUNNING" {
			rec["stopDate"] = float64(now)
		}
		if wr.pending != nil {
			wr.pending.ExecARN = execARN
			wr.pending.Definition = def
			pb, _ := json.Marshal(wr.pending)
			_ = p.col(req, "pending").Put(ctx, wr.pending.Token, pb)
			rec["pendingToken"] = wr.pending.Token
		}
		eb, _ := json.Marshal(rec)
		_ = p.col(req, "ex").Put(ctx, execARN, eb)
		output := map[string]any{"executionArn": execARN, "startDate": float64(now)}
		if req.Operation == "StartSyncExecution" {
			output["status"], output["input"], output["output"] = wr.status, rec["input"], rec["output"]
			if stop, exists := rec["stopDate"]; exists {
				output["stopDate"] = stop
			}
			if wr.cause != "" {
				output["cause"] = wr.cause
			}
			if wr.errorName != "" {
				output["error"] = wr.errorName
			}
		}
		return &spi.Response{Output: output}, nil
	case "StopExecution":
		ex := first(req.Input, "executionArn", "ExecutionArn")
		b, ok, _ := p.col(req, "ex").Get(ctx, ex)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if token := first(rec, "pendingToken"); token != "" {
			if pendingRecord, found, _ := p.col(req, "pending").Get(ctx, token); found {
				var pending pending
				_ = json.Unmarshal(pendingRecord, &pending)
				rec["redriveState"], rec["redriveInput"] = pending.StateName, pending.StateInput
			}
			_ = p.col(req, "pending").Delete(ctx, token)
			delete(rec, "pendingToken")
		}
		rec["status"] = "ABORTED"
		rec["stopDate"] = float64(now)
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ex").Put(ctx, ex, nb)
		return &spi.Response{Output: map[string]any{"stopDate": float64(now)}}, nil
	case "DescribeExecution":
		ex := first(req.Input, "executionArn", "ExecutionArn")
		b, ok, _ := p.col(req, "ex").Get(ctx, ex)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		delete(rec, "history")
		for _, internal := range []string{"definition", "roleArn", "revisionId", "stateMachineName", "type", "redriveState", "redriveInput", "redriveTokens", "pendingToken"} {
			delete(rec, internal)
		}
		return &spi.Response{Output: rec}, nil
	case "ListExecutions":
		stateMachineARN := first(req.Input, "stateMachineArn", "StateMachineArn")
		mapRunARN := first(req.Input, "mapRunArn", "MapRunArn")
		if (stateMachineARN == "") == (mapRunARN == "") {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		if mapRunARN != "" {
			if _, found := getRecord(ctx, p.col(req, "maprun"), mapRunARN); !found {
				return nil, &spi.Fault{Code: "ResourceNotFound", HTTPStatus: 400, Fault: "client"}
			}
		} else if machine, found := p.resolveStateMachine(ctx, req, stateMachineARN); !found {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		} else if first(machine, "type") == "EXPRESS" {
			return nil, &spi.Fault{Code: "StateMachineTypeNotSupported", HTTPStatus: 400, Fault: "client"}
		}
		status := first(req.Input, "statusFilter", "StatusFilter")
		redrive := first(req.Input, "redriveFilter", "RedriveFilter")
		if mapRunARN == "" && redrive != "" || status != "" && !slices.Contains([]string{"RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED", "PENDING_REDRIVE"}, status) {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		items := listRecords(ctx, p.col(req, "ex"), func(record map[string]any) (map[string]any, bool) {
			matches := mapRunARN != "" && first(record, "mapRunArn") == mapRunARN || stateMachineARN != "" && executionMatchesMachine(record, stateMachineARN)
			if !matches || status != "" && first(record, "status") != status || redrive == "REDRIVEN" && toFloat(record["redriveCount"]) == 0 || redrive == "NOT_REDRIVEN" && toFloat(record["redriveCount"]) > 0 {
				return nil, false
			}
			item := map[string]any{
				"executionArn": record["executionArn"], "stateMachineArn": record["stateMachineArn"], "name": record["name"],
				"status": record["status"], "startDate": record["startDate"],
			}
			for _, optional := range []string{"stopDate", "stateMachineAliasArn", "stateMachineVersionArn", "mapRunArn", "itemCount", "redriveCount", "redriveDate"} {
				if value, exists := record[optional]; exists {
					item[optional] = value
				}
			}
			return item, true
		})
		sort.SliceStable(items, func(i, j int) bool {
			return toFloat(items[i].(map[string]any)["startDate"]) > toFloat(items[j].(map[string]any)["startDate"])
		})
		return pagedResponse(req.Input, items, "executions")
	case "DescribeMapRun":
		arn := first(req.Input, "mapRunArn", "MapRunArn")
		record, ok := getRecord(ctx, p.col(req, "maprun"), arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: record}, nil
	case "ListMapRuns":
		executionARN := first(req.Input, "executionArn", "ExecutionArn")
		if _, ok := getRecord(ctx, p.col(req, "ex"), executionARN); !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		limit := int(inputNumber(req.Input, "maxResults", "MaxResults"))
		if limit == 0 {
			limit = 100
		}
		if limit < 0 || limit > 1000 {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		offset := 0
		if token := first(req.Input, "nextToken", "NextToken"); token != "" {
			var err error
			offset, err = strconv.Atoi(token)
			if err != nil || offset < 0 {
				return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
			}
		}
		records, _, _ := p.col(req, "maprun").List(ctx, "", "", 0)
		items := make([]any, 0)
		for _, item := range records {
			var record map[string]any
			_ = json.Unmarshal(item.Value, &record)
			if first(record, "executionArn") == executionARN {
				items = append(items, map[string]any{
					"executionArn": record["executionArn"], "mapRunArn": record["mapRunArn"], "startDate": record["startDate"],
					"stateMachineArn": record["stateMachineArn"], "stopDate": record["stopDate"],
				})
			}
		}
		sort.Slice(items, func(i, j int) bool {
			return toFloat(items[i].(map[string]any)["startDate"]) > toFloat(items[j].(map[string]any)["startDate"])
		})
		if offset > len(items) {
			return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
		}
		output := map[string]any{"mapRuns": items[offset:min(offset+limit, len(items))]}
		if offset+limit < len(items) {
			output["nextToken"] = strconv.Itoa(offset + limit)
		}
		return &spi.Response{Output: output}, nil
	case "UpdateMapRun":
		arn := first(req.Input, "mapRunArn", "MapRunArn")
		record, ok := getRecord(ctx, p.col(req, "maprun"), arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		if first(record, "status") != "RUNNING" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		updated := false
		for _, field := range []struct {
			lower, upper string
			max          float64
		}{
			{"maxConcurrency", "MaxConcurrency", math.MaxFloat64},
			{"toleratedFailureCount", "ToleratedFailureCount", math.MaxFloat64},
			{"toleratedFailurePercentage", "ToleratedFailurePercentage", 100},
		} {
			if value, exists := inputValue(req.Input, field.lower, field.upper); exists {
				number := toFloat(value)
				if number < 0 || number > field.max || field.lower != "toleratedFailurePercentage" && number != math.Trunc(number) {
					return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
				}
				record[field.lower], updated = number, true
			}
		}
		if !updated {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		encoded, _ := json.Marshal(record)
		_ = p.col(req, "maprun").Put(ctx, arn, encoded)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeStateMachineForExecution":
		executionARN := first(req.Input, "executionArn", "ExecutionArn")
		record, ok := getRecord(ctx, p.col(req, "ex"), executionARN)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		if first(record, "type") == "EXPRESS" {
			return nil, &spi.Fault{Code: "StateMachineTypeNotSupported", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{
			"definition": record["definition"], "name": record["stateMachineName"], "revisionId": record["revisionId"],
			"roleArn": record["roleArn"], "stateMachineArn": record["stateMachineArn"], "updateDate": record["startDate"],
		}}, nil
	case "RedriveExecution":
		return p.redriveExecution(ctx, req, now)
	case "GetExecutionHistory":
		ex := first(req.Input, "executionArn", "ExecutionArn")
		b, ok, _ := p.col(req, "ex").Get(ctx, ex)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if first(rec, "type") == "EXPRESS" {
			return nil, &spi.Fault{Code: "StateMachineTypeNotSupported", HTTPStatus: 400, Fault: "client"}
		}
		events := asSlice(rec["history"])
		if inputBool(req.Input, "reverseOrder", "ReverseOrder") {
			for left, right := 0, len(events)-1; left < right; left, right = left+1, right-1 {
				events[left], events[right] = events[right], events[left]
			}
		}
		return pagedResponse(req.Input, events, "events")
	case "CreateActivity":
		name := first(req.Input, "name", "Name")
		if !validResourceName(name) {
			return nil, &spi.Fault{Code: "InvalidName", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":activity:" + name
		encryption, _ := inputValue(req.Input, "encryptionConfiguration", "EncryptionConfiguration")
		if encryption != nil && !validEncryptionConfiguration(encryption) {
			return nil, &spi.Fault{Code: "InvalidEncryptionConfiguration", HTTPStatus: 400, Fault: "client"}
		}
		rawTags, validTags := optionalSlice(req.Input, "tags", "Tags")
		if !validTags {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		tags, tagFault := validatedTags(rawTags)
		if tagFault != "" {
			return nil, &spi.Fault{Code: tagFault, HTTPStatus: 400, Fault: "client"}
		}
		if existing, found := getRecord(ctx, p.col(req, "act"), name); found {
			if !reflect.DeepEqual(existing["_encryption"], encryption) {
				return nil, &spi.Fault{Code: "ActivityAlreadyExists", HTTPStatus: 400, Fault: "client"}
			}
			return &spi.Response{Output: map[string]any{"activityArn": arn, "creationDate": existing["creationDate"]}}, nil
		}
		rec := map[string]any{"activityArn": arn, "name": name, "creationDate": float64(now), "_encryption": encryption}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "act").Put(ctx, name, b)
		if tags != nil {
			tagsJSON, _ := json.Marshal(tags)
			_ = p.col(req, "tag").Put(ctx, arn, tagsJSON)
		}
		return &spi.Response{Output: map[string]any{"activityArn": arn, "creationDate": float64(now)}}, nil
	case "DeleteActivity":
		arn := first(req.Input, "activityArn", "ActivityArn")
		name, valid := activityName(req, arn)
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArn", HTTPStatus: 400, Fault: "client"}
		}
		_ = p.col(req, "act").Delete(ctx, name)
		_ = p.col(req, "tag").Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeActivity":
		name, valid := activityName(req, first(req.Input, "activityArn", "ActivityArn"))
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArn", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := p.col(req, "act").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ActivityDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if encryption := rec["_encryption"]; encryption != nil {
			rec["encryptionConfiguration"] = encryption
		}
		delete(rec, "_encryption")
		return &spi.Response{Output: rec}, nil
	case "ListActivities":
		items := listRecords(ctx, p.col(req, "act"), func(record map[string]any) (map[string]any, bool) {
			return map[string]any{"activityArn": record["activityArn"], "creationDate": record["creationDate"], "name": record["name"]}, true
		})
		return pagedResponse(req.Input, items, "activities")
	case "GetActivityTask":
		want := first(req.Input, "activityArn", "ActivityArn")
		name, valid := activityName(req, want)
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArn", HTTPStatus: 400, Fault: "client"}
		}
		if _, exists := getRecord(ctx, p.col(req, "act"), name); !exists {
			return nil, &spi.Fault{Code: "ActivityDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		if worker, exists := inputValue(req.Input, "workerName", "WorkerName"); exists {
			workerName, ok := worker.(string)
			if !ok || len(workerName) < 1 || len(workerName) > 80 {
				return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
			}
		}
		kvs, _, _ := p.col(req, "pending").List(ctx, "", "", 0)
		for _, kv := range kvs {
			var pend pending
			_ = json.Unmarshal(kv.Value, &pend)
			if want != "" && pend.ActivityARN != want {
				continue
			}
			inb, _ := json.Marshal(pend.Input)
			return &spi.Response{Output: map[string]any{"taskToken": pend.Token, "input": string(inb)}}, nil
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "SendTaskSuccess":
		return p.finishTask(ctx, req, now, true)
	case "SendTaskFailure":
		return p.finishTask(ctx, req, now, false)
	case "SendTaskHeartbeat":
		token := first(req.Input, "taskToken", "TaskToken")
		if len(token) < 1 || len(token) > 2048 {
			return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
		}
		if _, found, _ := p.col(req, "pending").Get(ctx, token); !found {
			return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "ValidateStateMachineDefinition":
		diagnostics := validateDefinition(first(req.Input, "definition", "Definition"), first(req.Input, "type", "Type"))
		maxResults := int(toFloat(req.Input["maxResults"]))
		if maxResults == 0 {
			maxResults = 100
		}
		if maxResults < 0 || maxResults > 100 {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		truncated := len(diagnostics) > maxResults
		if truncated {
			diagnostics = diagnostics[:maxResults]
		}
		result := "OK"
		if len(diagnostics) > 0 {
			result = "FAIL"
		}
		items := make([]any, len(diagnostics))
		for i := range diagnostics {
			items[i] = diagnostics[i]
		}
		return &spi.Response{Output: map[string]any{"result": result, "diagnostics": items, "truncated": truncated}}, nil
	case "TestState":
		definition := first(req.Input, "definition", "Definition")
		input := first(req.Input, "input", "Input")
		var data any = map[string]any{}
		if input != "" && json.Unmarshal([]byte(input), &data) != nil {
			return nil, &spi.Fault{Code: "InvalidExecutionInput", HTTPStatus: 400, Fault: "client"}
		}
		wrapped, next, err := testDefinition(definition, first(req.Input, "stateName", "StateName"), data)
		if err != nil {
			return nil, &spi.Fault{Code: "InvalidDefinition", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
		}
		wr := p.walk(ctx, req, wrapped, "", data, nil)
		output, _ := json.Marshal(wr.out)
		response := map[string]any{"status": wr.status, "output": string(output)}
		if next != "" && wr.status == "SUCCEEDED" {
			response["nextState"] = next
		}
		if wr.errorName != "" {
			response["error"] = wr.errorName
		}
		if wr.cause != "" {
			response["cause"] = wr.cause
		}
		return &spi.Response{Output: response}, nil
	case "TagResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		if !p.tagResourceExists(ctx, req, arn) {
			return nil, &spi.Fault{Code: resourceARNFault(arn), HTTPStatus: 400, Fault: "client"}
		}
		if _, exists := inputValue(req.Input, "tags", "Tags"); !exists {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rawTags, validTags := optionalSlice(req.Input, "tags", "Tags")
		if !validTags {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		incoming, tagFault := validatedTags(rawTags)
		if tagFault != "" {
			return nil, &spi.Fault{Code: tagFault, HTTPStatus: 400, Fault: "client"}
		}
		var tags []any
		if b, ok, _ := p.col(req, "tag").Get(ctx, arn); ok {
			_ = json.Unmarshal(b, &tags)
		}
		indexes := map[string]int{}
		for i, tag := range tags {
			m, _ := tag.(map[string]any)
			indexes[first(m, "key", "Key")] = i
		}
		for _, tag := range incoming {
			m, _ := tag.(map[string]any)
			key := first(m, "key", "Key")
			if i, ok := indexes[key]; ok {
				tags[i] = tag
			} else {
				indexes[key] = len(tags)
				tags = append(tags, tag)
			}
		}
		if len(tags) > 50 {
			return nil, &spi.Fault{Code: "TooManyTags", HTTPStatus: 400, Fault: "client"}
		}
		b, _ := json.Marshal(tags)
		_ = p.col(req, "tag").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		if !p.tagResourceExists(ctx, req, arn) {
			return nil, &spi.Fault{Code: resourceARNFault(arn), HTTPStatus: 400, Fault: "client"}
		}
		keys, validKeys := optionalSlice(req.Input, "tagKeys", "TagKeys")
		if !validKeys || len(keys) == 0 {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		var tags []any
		if b, ok, _ := p.col(req, "tag").Get(ctx, arn); ok {
			_ = json.Unmarshal(b, &tags)
		}
		drop := map[string]bool{}
		for _, key := range keys {
			value := fmt.Sprint(key)
			if !validTagText(value, 1, 128) || strings.HasPrefix(strings.ToLower(value), "aws:") {
				return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
			}
			drop[value] = true
		}
		kept := tags[:0]
		for _, tag := range tags {
			m, _ := tag.(map[string]any)
			if !drop[first(m, "key", "Key")] {
				kept = append(kept, tag)
			}
		}
		b, _ := json.Marshal(kept)
		_ = p.col(req, "tag").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		if !p.tagResourceExists(ctx, req, arn) {
			return nil, &spi.Fault{Code: resourceARNFault(arn), HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := p.col(req, "tag").Get(ctx, arn)
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"tags": tags}}, nil
	default:
		return nil, spi.NotImplemented("aws.states", req.Operation, "emulate")
	}
}

func (p *Pack) smARN(req *spi.Request, name string) string {
	return "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":stateMachine:" + name
}

func (p *Pack) execARN(req *spi.Request, sm, ex string) string {
	return "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":execution:" + sm + ":" + ex
}

func (p *Pack) mapRunARN(req *spi.Request, sm, label string) string {
	return "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":mapRun:" + sm + "/" + label + ":" + p.deps.Rand.UUID()
}

func (p *Pack) resolveStateMachine(ctx context.Context, req *spi.Request, arn string) (map[string]any, bool) {
	name, qualifier := baseSMName(arn), strings.TrimPrefix(smName(arn), baseSMName(arn))
	if qualifier == "" {
		return getRecord(ctx, p.col(req, "sm"), name)
	}
	qualifier = strings.TrimPrefix(qualifier, ":")
	if _, err := strconv.Atoi(qualifier); err == nil {
		machine, ok := getRecord(ctx, p.col(req, "ver"), name+":"+qualifier)
		if ok {
			machine["_resolvedVersionArn"] = arn
		}
		return machine, ok
	}
	alias, ok := getRecord(ctx, p.col(req, "alias"), arn)
	if !ok {
		return nil, false
	}
	routes := asSlice(alias["routingConfiguration"])
	pick, total := p.deps.Rand.Intn(100), 0
	for _, raw := range routes {
		route, _ := raw.(map[string]any)
		total += int(inputNumber(route, "weight", "Weight"))
		if pick < total {
			versionARN := first(route, "stateMachineVersionArn", "StateMachineVersionArn")
			machine, ok := getRecord(ctx, p.col(req, "ver"), baseSMName(versionARN)+":"+strconv.Itoa(versionNumber(versionARN)))
			if ok {
				machine["_resolvedVersionArn"], machine["_resolvedAliasArn"] = versionARN, arn
			}
			return machine, ok
		}
	}
	return nil, false
}

func (p *Pack) validateAliasRoutes(ctx context.Context, req *spi.Request, routes []any) (string, bool) {
	if len(routes) < 1 || len(routes) > 2 {
		return "", false
	}
	baseARN, total := "", 0
	for _, raw := range routes {
		route, ok := raw.(map[string]any)
		arn := first(route, "stateMachineVersionArn", "StateMachineVersionArn")
		weight := inputNumber(route, "weight", "Weight")
		if !ok || versionNumber(arn) < 1 || weight < 0 || weight > 100 || weight != math.Trunc(weight) {
			return "", false
		}
		currentBase := stateMachineBaseARN(arn)
		if baseARN == "" {
			baseARN = currentBase
		} else if currentBase != baseARN {
			return "", false
		}
		if _, exists := getRecord(ctx, p.col(req, "ver"), baseSMName(arn)+":"+strconv.Itoa(versionNumber(arn))); !exists {
			return "", false
		}
		total += int(weight)
	}
	return baseARN, total == 100
}

type pending struct {
	Token, ActivityARN, StateName, ExecARN, Definition string
	Input, StateInput                                  any
	Retries                                            map[int]int
	Variables                                          map[string]any
	Callback                                           bool
}

type walkResult struct {
	out           any
	status, cause string
	errorName     string
	hist          []any
	pending       *pending
	failedState   string
	failedInput   any
	mapRuns       []mapRunDraft
}

type mapRunDraft struct {
	arn    string
	label  string
	record map[string]any
}

type mapItemResult struct {
	metadata  map[string]any
	output    any
	succeeded bool
	itemCount int
	history   []any
}

func (p *Pack) redriveExecution(ctx context.Context, req *spi.Request, now int64) (*spi.Response, error) {
	arn := first(req.Input, "executionArn", "ExecutionArn")
	record, ok := getRecord(ctx, p.col(req, "ex"), arn)
	if !ok {
		return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
	}
	token := first(req.Input, "clientToken", "ClientToken")
	if len(token) > 64 {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
	}
	tokens, _ := record["redriveTokens"].(map[string]any)
	if redriveDate, exists := tokens[token]; token != "" && exists {
		return &spi.Response{Output: map[string]any{"redriveDate": redriveDate}}, nil
	}
	if first(record, "type") == "EXPRESS" || first(record, "status") == "RUNNING" || first(record, "status") == "SUCCEEDED" {
		return nil, &spi.Fault{Code: "ExecutionNotRedrivable", HTTPStatus: 400, Fault: "client"}
	}
	state, input := first(record, "redriveState"), record["redriveInput"]
	if state == "" {
		if pendingToken := first(record, "pendingToken"); pendingToken != "" {
			if pendingRecord, found, _ := p.col(req, "pending").Get(ctx, pendingToken); found {
				var pending pending
				_ = json.Unmarshal(pendingRecord, &pending)
				state, input = pending.StateName, pending.StateInput
				_ = p.col(req, "pending").Delete(ctx, pendingToken)
			}
		}
	}
	if state == "" {
		return nil, &spi.Fault{Code: "ExecutionNotRedrivable", HTTPStatus: 400, Fault: "client"}
	}
	var machine map[string]any
	if json.Unmarshal([]byte(first(record, "definition")), &machine) != nil {
		return nil, &spi.Fault{Code: "ExecutionNotRedrivable", HTTPStatus: 400, Fault: "client"}
	}
	machine["StartAt"] = state
	redriveDefinition, _ := json.Marshal(machine)
	walkRequest := *req
	walkRequest.Input = maps.Clone(req.Input)
	walkRequest.Input["_stateMachineArn"] = record["stateMachineArn"]
	walkRequest.Input["_executionArn"], walkRequest.Input["_executionName"] = arn, record["name"]
	walkRequest.Input["_executionInput"] = parseJSON(first(record, "input"))
	walkRequest.Input["_executionStartTime"] = time.Unix(int64(toFloat(record["startDate"])), 0).UTC().Format(time.RFC3339)
	walkRequest.Input["_executionRoleArn"] = record["roleArn"]
	walkRequest.Input["_executionRedriveCount"] = toFloat(record["redriveCount"]) + 1
	walkRequest.Input["_executionRedriveTime"] = p.deps.Clock.Now().UTC().Format(time.RFC3339Nano)
	result := p.walk(ctx, &walkRequest, string(redriveDefinition), "", input, nil)
	output, _ := json.Marshal(result.out)
	record["status"], record["output"], record["cause"] = result.status, string(output), result.cause
	delete(record, "error")
	delete(record, "pendingToken")
	delete(record, "redriveState")
	delete(record, "redriveInput")
	if result.errorName != "" {
		record["error"] = result.errorName
	}
	if result.failedState != "" {
		record["redriveState"], record["redriveInput"] = result.failedState, result.failedInput
	}
	record["redriveCount"] = toFloat(record["redriveCount"]) + 1
	record["redriveDate"] = float64(now)
	history := asSlice(record["history"])
	history = append(history, map[string]any{"type": "ExecutionRedriven", "id": len(history) + 1, "redriveCount": record["redriveCount"]})
	record["history"] = append(history, result.hist...)
	if result.status == "RUNNING" {
		delete(record, "stopDate")
	} else {
		record["stopDate"] = float64(now)
	}
	if result.pending != nil {
		result.pending.ExecARN, result.pending.Definition = arn, string(redriveDefinition)
		encoded, _ := json.Marshal(result.pending)
		_ = p.col(req, "pending").Put(ctx, result.pending.Token, encoded)
		record["pendingToken"] = result.pending.Token
	}
	if token != "" {
		if tokens == nil {
			tokens = map[string]any{}
		}
		tokens[token] = float64(now)
		record["redriveTokens"] = tokens
	}
	encoded, _ := json.Marshal(record)
	_ = p.col(req, "ex").Put(ctx, arn, encoded)
	return &spi.Response{Output: map[string]any{"redriveDate": float64(now)}}, nil
}

func (p *Pack) finishTask(ctx context.Context, req *spi.Request, now int64, ok bool) (*spi.Response, error) {
	tok := first(req.Input, "taskToken", "TaskToken")
	if len(tok) < 1 || len(tok) > 2048 {
		return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
	}
	output := ""
	if ok {
		value, exists := inputValue(req.Input, "output", "Output")
		var valid bool
		output, valid = value.(string)
		if !exists || !valid || len(output) > 262144 || !json.Valid([]byte(output)) {
			return nil, &spi.Fault{Code: "InvalidOutput", HTTPStatus: 400, Fault: "client"}
		}
	} else {
		for _, field := range []struct {
			lower, upper string
			maximum      int
		}{{"error", "Error", 256}, {"cause", "Cause", 32768}} {
			if value, exists := inputValue(req.Input, field.lower, field.upper); exists {
				text, valid := value.(string)
				if !valid || len(text) > field.maximum {
					return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
				}
			}
		}
	}
	b, found, _ := p.col(req, "pending").Get(ctx, tok)
	if !found {
		return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
	}
	var pend pending
	_ = json.Unmarshal(b, &pend)
	_ = p.col(req, "pending").Delete(ctx, tok)
	exb, eok, _ := p.col(req, "ex").Get(ctx, pend.ExecARN)
	if !eok {
		return &spi.Response{Output: map[string]any{}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(exb, &rec)
	data := parseJSON(output)
	from, definition := pend.StateName, pend.Definition
	var sm map[string]any
	_ = json.Unmarshal([]byte(pend.Definition), &sm)
	states, _ := sm["States"].(map[string]any)
	st, _ := states[pend.StateName].(map[string]any)
	queryLanguage := first(st, "QueryLanguage")
	if queryLanguage == "" {
		queryLanguage = first(sm, "QueryLanguage")
	}
	isJSONata := queryLanguage == "JSONata"
	walkRequest := *req
	walkRequest.Input = maps.Clone(req.Input)
	walkRequest.Input["_stateMachineArn"] = rec["stateMachineArn"]
	walkRequest.Input["_executionArn"], walkRequest.Input["_executionName"] = pend.ExecARN, first(rec, "name")
	walkRequest.Input["_executionInput"] = parseJSON(first(rec, "input"))
	walkRequest.Input["_executionStartTime"] = time.Unix(int64(toFloat(rec["startDate"])), 0).UTC().Format(time.RFC3339)
	walkRequest.Input["_executionRoleArn"] = rec["roleArn"]
	walkRequest.Input["_executionRedriveCount"] = rec["redriveCount"]
	retrying := false
	if !ok {
		failure := stateFailure{name: first(req.Input, "error", "Error"), cause: first(req.Input, "cause", "Cause")}
		if failure.name == "" {
			failure.name = "States.TaskFailed"
		}
		if failure.cause == "" {
			failure.cause = failure.name
		}
		if pend.Retries == nil {
			pend.Retries = map[int]int{}
		}
		if retryTask(st, failure.name, pend.Retries) {
			if !pend.Callback {
				pend.Token = p.deps.Rand.Hex(16)
				pb, _ := json.Marshal(pend)
				_ = p.col(req, "pending").Put(ctx, pend.Token, pb)
				rec["pendingToken"] = pend.Token
				nb, _ := json.Marshal(rec)
				_ = p.col(req, "ex").Put(ctx, pend.ExecARN, nb)
				return &spi.Response{Output: map[string]any{}}, nil
			}
			sm["StartAt"] = pend.StateName
			def, _ := json.Marshal(sm)
			definition, from, data, retrying = string(def), "", pend.StateInput, true
		}
		if !retrying {
			var next string
			var out any
			var caught, valid bool
			if isJSONata {
				next, out, caught, valid = catchJSONata(st, failure, pend.StateInput, jsonataContext(&walkRequest, pend.StateName, nil), pend.Variables, p.deps.Rand)
				if !valid {
					failure = stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
					caught = false
				}
			} else {
				next, out, caught = catchTask(st, failure, pend.StateInput)
				if caught && !applyCatchAssignments(st, failure, pend.StateInput, jsonataContext(&walkRequest, pend.StateName, nil), pend.Variables, p.deps.Rand) {
					failure = stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
					caught = false
				}
			}
			if caught && next == "" {
				failure = stateFailure{name: "States.Runtime", cause: "States.Runtime"}
			}
			if !caught || next == "" {
				rec["status"], rec["error"], rec["cause"], rec["stopDate"] = "FAILED", failure.name, failure.cause, float64(now)
				nb, _ := json.Marshal(rec)
				_ = p.col(req, "ex").Put(ctx, pend.ExecARN, nb)
				return &spi.Response{Output: map[string]any{}}, nil
			}
			sm["StartAt"] = next
			def, _ := json.Marshal(sm)
			definition, from, data = string(def), "", out
		}
	} else if isJSONata {
		var valid bool
		data, valid = applyJSONataState(st, pend.StateInput, data, jsonataContext(&walkRequest, pend.StateName, nil), pend.Variables, p.deps.Rand)
		if !valid {
			failure := stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
			if pend.Retries == nil {
				pend.Retries = map[int]int{}
			}
			if retryTask(st, failure.name, pend.Retries) {
				sm["StartAt"] = pend.StateName
				def, _ := json.Marshal(sm)
				definition, from, data, retrying = string(def), "", pend.StateInput, true
			} else if next, out, caught, catchValid := catchJSONata(st, failure, pend.StateInput, jsonataContext(&walkRequest, pend.StateName, nil), pend.Variables, p.deps.Rand); caught && catchValid && next != "" {
				sm["StartAt"] = next
				def, _ := json.Marshal(sm)
				definition, from, data = string(def), "", out
			} else {
				rec["status"], rec["error"], rec["cause"], rec["stopDate"] = "FAILED", failure.name, failure.cause, float64(now)
				nb, _ := json.Marshal(rec)
				_ = p.col(req, "ex").Put(ctx, pend.ExecARN, nb)
				return &spi.Response{Output: map[string]any{}}, nil
			}
		}
	} else {
		result := data
		out, valid := applyStateResult(st, pend.StateInput, result, p.deps.Rand)
		failure := "States.Runtime"
		if valid {
			failure = "States.QueryEvaluationError"
			valid = applyJSONataAssignments(jsonataScope{
				input: pend.StateInput, result: result, hasResult: true, context: jsonataContext(&walkRequest, pend.StateName, nil), variables: pend.Variables, random: p.deps.Rand,
			}, pend.Variables, st)
		}
		if !valid {
			rec["status"], rec["error"], rec["cause"], rec["stopDate"] = "FAILED", failure, failure, float64(now)
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "ex").Put(ctx, pend.ExecARN, nb)
			return &spi.Response{Output: map[string]any{}}, nil
		}
		data = out
	}
	if !retrying && !isJSONata {
		if output, valid := applyDataPath(st, "OutputPath", data); valid {
			data = output
		} else {
			rec["status"], rec["error"], rec["cause"], rec["stopDate"] = "FAILED", "States.Runtime", "States.Runtime", float64(now)
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "ex").Put(ctx, pend.ExecARN, nb)
			return &spi.Response{Output: map[string]any{}}, nil
		}
	}
	var retries map[string]map[int]int
	if retrying {
		retries = map[string]map[int]int{pend.StateName: pend.Retries}
	}
	wr := p.walk(ctx, &walkRequest, definition, from, data, retries, pend.Variables)
	ob, _ := json.Marshal(wr.out)
	rec["status"] = wr.status
	rec["output"] = string(ob)
	rec["cause"] = wr.cause
	rec["history"] = append(asSlice(rec["history"]), wr.hist...)
	if wr.errorName != "" {
		rec["error"] = wr.errorName
	} else {
		delete(rec, "error")
	}
	if wr.status != "RUNNING" {
		rec["stopDate"] = float64(now)
	}
	if wr.pending != nil {
		wr.pending.ExecARN = pend.ExecARN
		wr.pending.Definition = pend.Definition
		pb, _ := json.Marshal(wr.pending)
		_ = p.col(req, "pending").Put(ctx, wr.pending.Token, pb)
		rec["pendingToken"] = wr.pending.Token
	} else {
		delete(rec, "pendingToken")
	}
	nb, _ := json.Marshal(rec)
	_ = p.col(req, "ex").Put(ctx, pend.ExecARN, nb)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) walk(ctx context.Context, req *spi.Request, def, from string, input any, retries map[string]map[int]int, inheritedVariables ...map[string]any) (result walkResult) {
	currentState, currentInput := "", input
	variables := map[string]any{}
	if len(inheritedVariables) != 0 && inheritedVariables[0] != nil {
		variables = inheritedVariables[0]
	}
	var mapRuns []mapRunDraft
	defer func() {
		if result.status == "FAILED" && result.failedState == "" {
			result.failedState, result.failedInput = currentState, currentInput
		}
		result.mapRuns = append(result.mapRuns, mapRuns...)
	}()
	var sm map[string]any
	if err := json.Unmarshal([]byte(def), &sm); err != nil {
		return walkResult{out: input, status: "FAILED", cause: "InvalidDefinition"}
	}
	start, _ := sm["StartAt"].(string)
	machineQueryLanguage := first(sm, "QueryLanguage")
	if machineQueryLanguage == "" {
		machineQueryLanguage = "JSONPath"
	}
	states, _ := sm["States"].(map[string]any)
	data := input
	cur := start
	if from != "" {
		st, _ := states[from].(map[string]any)
		if end, _ := st["End"].(bool); end {
			return walkResult{out: data, status: "SUCCEEDED"}
		}
		next, _ := st["Next"].(string)
		if next == "" {
			return walkResult{out: data, status: "SUCCEEDED"}
		}
		cur = next
	}
	var hist []any
	if retries == nil {
		retries = map[string]map[int]int{}
	}
walkLoop:
	for hop := 0; hop < 64; hop++ {
		st, ok := states[cur].(map[string]any)
		if !ok {
			return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
		}
		rawInput := data
		currentState, currentInput = cur, rawInput
		queryLanguage := first(st, "QueryLanguage")
		if queryLanguage == "" {
			queryLanguage = machineQueryLanguage
		}
		isJSONata := queryLanguage == "JSONata"
		stateContext := jsonataContext(req, cur, nil)
		stateContext["State"].(map[string]any)["EnteredTime"] = p.deps.Clock.Now().UTC().Format(time.RFC3339Nano)
		if !isJSONata {
			data, ok = applyDataPath(st, "InputPath", data)
			if !ok {
				return walkResult{out: rawInput, status: "FAILED", cause: "States.Runtime", hist: hist}
			}
		}
		typ, _ := st["Type"].(string)
		hist = append(hist, map[string]any{"type": typ + "StateEntered", "id": hop + 1, "name": cur})
		jsonataOutputApplied := false
		switch typ {
		case "Pass":
			if isJSONata {
				data = rawInput
			} else {
				result := data
				if params, ok := st["Parameters"].(map[string]any); ok {
					result = applyParams(params, data, nil, p.deps.Rand)
				} else if value, ok := st["Result"]; ok {
					result = value
				}
				var valid bool
				data, valid = applyResultPath(st, rawInput, result)
				if !valid {
					return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
				}
			}
		case "Succeed":
			if isJSONata {
				data, ok = applyJSONataState(st, rawInput, rawInput, stateContext, variables, p.deps.Rand)
			} else {
				data, ok = applyDataPath(st, "OutputPath", data)
			}
			if !ok {
				return walkResult{out: rawInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
			}
			return walkResult{out: data, status: "SUCCEEDED", hist: hist}
		case "Fail":
			resolve := func(key string) (string, bool) {
				value := st[key]
				if isJSONata {
					var valid bool
					value, valid = evalJSONataValue(value, jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand})
					if !valid {
						return "", false
					}
				} else if path := first(st, key+"Path"); path != "" {
					if strings.HasPrefix(path, "States.") {
						value, _ = evalIntrinsic(path, rawInput, nil, p.deps.Rand)
					} else {
						value = jsonPath(rawInput, path)
					}
				}
				text, valid := value.(string)
				return text, valid || value == nil
			}
			errorName, errorValid := resolve("Error")
			cause, causeValid := resolve("Cause")
			if !errorValid || !causeValid {
				return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
			}
			if errorName == "" {
				errorName = "States.TaskFailed"
			}
			if cause == "" {
				cause = errorName
			}
			return walkResult{out: data, status: "FAILED", cause: cause, errorName: errorName, hist: hist}
		case "Wait":
			if isJSONata {
				if value, exists := st["Seconds"]; exists {
					resolved, valid := evalJSONataValue(value, jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand})
					seconds, numeric := exactNumber(resolved)
					if !valid || !numeric || seconds != math.Trunc(seconds) || seconds < 0 || seconds > 99999999 {
						return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				} else if value, exists := st["Timestamp"]; exists {
					resolved, valid := evalJSONataValue(value, jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand})
					timestamp, stringValue := resolved.(string)
					_, parseErr := time.Parse(time.RFC3339, timestamp)
					if !valid || !stringValue || parseErr != nil || !strings.Contains(timestamp, "T") || !strings.HasSuffix(timestamp, "Z") {
						return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				}
			}
		case "Task":
			res := first(st, "Resource")
			callback := strings.HasSuffix(res, ".waitForTaskToken")
			token := ""
			var taskContext map[string]any
			if callback {
				token = p.deps.Rand.Hex(16)
				taskContext = map[string]any{"Task": map[string]any{"Token": token}}
			}
			if retries[cur] == nil {
				retries[cur] = map[int]int{}
			}
			for {
				setJSONataRetryCount(stateContext, retries[cur])
				payload := taskPayload(st, data, taskContext, p.deps.Rand)
				failure := stateFailure{}
				taskReq := req
				if _, exists := st["Credentials"]; exists {
					var scope *jsonataScope
					if isJSONata {
						scope = &jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand}
					}
					identity, valid := taskIdentity(st, data, req.Identity, p.deps.Rand, scope)
					if valid {
						copy := *req
						copy.Identity = identity
						taskReq = &copy
					} else if isJSONata {
						failure = stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
					} else {
						failure = stateFailure{name: "States.Permissions", cause: "States.Permissions"}
					}
				}
				if isJSONata {
					var valid bool
					payload, valid = evalJSONataValue(st["Arguments"], jsonataScope{
						input: rawInput, context: mergeJSONataContext(stateContext, taskContext), variables: variables, random: p.deps.Rand,
					})
					if _, configured := st["Arguments"]; !configured {
						payload, valid = rawInput, true
					}
					if !valid {
						failure = stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
					}
				}
				if failure.name == "" && strings.Contains(res, ":activity:") {
					tok := p.deps.Rand.Hex(16)
					return walkResult{out: data, status: "RUNNING", hist: hist, pending: &pending{
						Token: tok, ActivityARN: res, StateName: cur, Input: payload, StateInput: rawInput, Retries: retries[cur], Variables: variables,
					}}
				}
				if failure.name == "" {
					out, err, errorPrefix, sdk, supported := p.invokeTask(ctx, taskReq, res, payload)
					if !supported {
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
					if err != nil {
						failure = taskFailure(errorPrefix, sdk, err)
					} else if callback {
						return walkResult{out: data, status: "RUNNING", hist: hist, pending: &pending{
							Token: token, StateName: cur, Input: payload, StateInput: rawInput, Retries: retries[cur], Variables: variables, Callback: true,
						}}
					} else if isJSONata {
						data, ok = applyJSONataState(st, rawInput, out, stateContext, variables, p.deps.Rand)
						if ok {
							jsonataOutputApplied = true
							break
						}
						failure = stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
					} else {
						data, ok = applyStateResult(st, rawInput, out, p.deps.Rand)
						if !ok {
							return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
						}
						if applyJSONataAssignments(jsonataScope{
							input: rawInput, result: out, hasResult: true, context: stateContext, variables: variables, random: p.deps.Rand,
						}, variables, st) {
							break
						}
						failure = stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
					}
				}
				// ponytail: retry delays are instant like Wait states; add virtual scheduling when timing fidelity is required.
				if retryTask(st, failure.name, retries[cur]) {
					continue
				}
				var next string
				var out any
				var caught, valid bool
				if isJSONata {
					next, out, caught, valid = catchJSONata(st, failure, rawInput, stateContext, variables, p.deps.Rand)
					if !valid {
						return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				} else {
					next, out, caught = catchTask(st, failure, rawInput)
					if caught && !applyCatchAssignments(st, failure, rawInput, stateContext, variables, p.deps.Rand) {
						return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				}
				if !caught {
					return walkResult{out: data, status: "FAILED", cause: failure.cause, errorName: failure.name, hist: hist}
				}
				if next == "" {
					return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
				}
				data = out
				if !isJSONata {
					data, ok = applyDataPath(st, "OutputPath", out)
					if !ok {
						return walkResult{out: rawInput, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
				}
				cur = next
				continue walkLoop
			}
		case "Choice":
			next, matchedChoice := selectedChoice(st, data)
			if isJSONata {
				var valid bool
				next, valid = jsonataChoiceNext(st, rawInput, stateContext, variables, p.deps.Rand)
				if !valid {
					return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
				}
			}
			if next == "" {
				return walkResult{out: data, status: "FAILED", cause: "States.NoChoiceMatched", hist: hist}
			}
			if !isJSONata {
				owners := []map[string]any{st}
				if matchedChoice != nil {
					owners = append(owners, matchedChoice)
				}
				if !applyJSONataAssignments(jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand}, variables, owners...) {
					return walkResult{out: rawInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
				}
				data, ok = applyDataPath(st, "OutputPath", data)
				if !ok {
					return walkResult{out: rawInput, status: "FAILED", cause: "States.Runtime", hist: hist}
				}
			}
			cur = next
			continue
		case "Parallel":
			stateInput := rawInput
			branchInput := data
			if params, ok := st["Parameters"].(map[string]any); ok && !isJSONata {
				branchInput = applyParams(params, data, nil, p.deps.Rand)
			}
			if retries[cur] == nil {
				retries[cur] = map[int]int{}
			}
			for {
				setJSONataRetryCount(stateContext, retries[cur])
				branches, _ := st["Branches"].([]any)
				var results []any
				var failed *walkResult
				for _, br := range branches {
					bm, _ := br.(map[string]any)
					if bm["QueryLanguage"] == nil {
						bm = maps.Clone(bm)
						bm["QueryLanguage"] = machineQueryLanguage
					}
					bdef, _ := json.Marshal(bm)
					wr := p.walk(ctx, req, string(bdef), "", branchInput, nil, maps.Clone(variables))
					mapRuns = append(mapRuns, wr.mapRuns...)
					if wr.status != "SUCCEEDED" {
						failed = &wr
						break
					}
					results = append(results, wr.out)
				}
				if failed == nil {
					if isJSONata {
						data, ok = applyJSONataState(st, stateInput, results, stateContext, variables, p.deps.Rand)
						if ok {
							jsonataOutputApplied = true
							break
						}
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError"}
					} else if data, ok = applyStateResult(st, stateInput, results, p.deps.Rand); !ok {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", hist: hist}
					} else if applyJSONataAssignments(jsonataScope{
						input: stateInput, result: results, hasResult: true, context: stateContext, variables: variables, random: p.deps.Rand,
					}, variables, st) {
						break
					} else {
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError"}
					}
				}
				var next string
				var out any
				var retry, caught, valid bool
				if isJSONata {
					next, out, retry, caught, valid = recoverJSONata(st, *failed, stateInput, stateContext, variables, p.deps.Rand, retries[cur])
					if !valid {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				} else {
					next, out, retry, caught = recoverState(st, *failed, stateInput, retries[cur])
				}
				if retry {
					continue
				}
				if caught {
					if next == "" {
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
					failureName := failed.errorName
					if failureName == "" {
						failureName = "States.TaskFailed"
					}
					if !isJSONata && !applyCatchAssignments(st, stateFailure{name: failureName, cause: failed.cause}, stateInput, stateContext, variables, p.deps.Rand) {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
					data = out
					if !isJSONata {
						data, ok = applyDataPath(st, "OutputPath", out)
						if !ok {
							return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", hist: hist}
						}
					}
					cur = next
					continue walkLoop
				}
				failed.hist, failed.failedState, failed.failedInput = hist, cur, stateInput
				return *failed
			}
		case "Map":
			stateInput := rawInput
			var mapScope *jsonataScope
			if isJSONata {
				mapScope = &jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand}
			}
			arr, source, ok := p.mapItems(ctx, req, st, data, mapScope)
			if isJSONata {
				items := any(data)
				if _, hasReader := st["ItemReader"]; hasReader {
					items = arr
				}
				if configured, exists := st["Items"]; exists {
					items, ok = evalJSONataValue(configured, jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand})
				}
				arr, ok = items.([]any)
				source = "STATE_DATA"
			}
			if !ok {
				failure := "States.Runtime"
				if _, hasReader := st["ItemReader"]; hasReader {
					failure = "States.ItemReaderFailed"
				}
				return walkResult{out: data, status: "FAILED", cause: failure, errorName: failure, hist: hist}
			}
			arr, ok = batchMapItems(st, data, arr, p.deps.Rand, mapScope)
			if !ok {
				return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
			}
			iter, _ := st["Iterator"].(map[string]any)
			if iter == nil {
				iter, _ = st["ItemProcessor"].(map[string]any)
			}
			if iter["QueryLanguage"] == nil {
				iter = maps.Clone(iter)
				iter["QueryLanguage"] = machineQueryLanguage
			}
			idef, _ := json.Marshal(iter)
			processorConfig, _ := iter["ProcessorConfig"].(map[string]any)
			distributed := first(processorConfig, "Mode") == "DISTRIBUTED"
			_, hasResultWriter := st["ResultWriter"]
			if hasResultWriter && !distributed {
				return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
			}
			selector := st["ItemSelector"]
			if selector == nil && !isJSONata {
				selector = st["Parameters"]
			}
			if retries[cur] == nil {
				retries[cur] = map[int]int{}
			}
			for {
				setJSONataRetryCount(stateContext, retries[cur])
				var results []any
				var itemResults []mapItemResult
				var failed *walkResult
				failedCount := 0
				label, mapRunARN := first(st, "Label"), ""
				if label == "" {
					label = cur
				}
				if distributed {
					mapRunARN = p.mapRunARN(req, mapStateMachineName(req), label)
				}
				for index, item := range arr {
					iterationInput := item
					itemContext := map[string]any{"Map": map[string]any{"Item": map[string]any{
						"Index": float64(index), "Value": item, "Source": source,
					}}}
					if selected, valid := selector.(map[string]any); valid && !isJSONata {
						iterationInput = applyParams(selected, data, itemContext, p.deps.Rand)
					} else if selector != nil {
						iterationInput, ok = evalJSONataValue(selector, jsonataScope{
							input: rawInput, context: mergeJSONataContext(stateContext, itemContext), variables: variables, random: p.deps.Rand,
						})
						if !ok {
							return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
						}
					}
					wr := p.walk(ctx, req, string(idef), "", iterationInput, nil, maps.Clone(variables))
					mapRuns = append(mapRuns, wr.mapRuns...)
					if distributed {
						itemResult := p.mapItemResult(req, cur, iterationInput, wr)
						p.storeMapItemExecution(ctx, req, mapRunARN, string(idef), itemResult)
						if hasResultWriter {
							itemResults = append(itemResults, itemResult)
						}
					}
					if wr.status != "SUCCEEDED" {
						failedCount++
						if failed == nil {
							failed = &wr
						}
						if !distributed {
							break
						}
						continue
					}
					results = append(results, wr.out)
				}
				mapOutput := any(results)
				if distributed {
					allowed := failedCount == 0
					failureCount, hasFailureCount := st["ToleratedFailureCount"]
					failurePercentage, hasFailurePercentage := st["ToleratedFailurePercentage"]
					if failedCount > 0 && (hasFailureCount || hasFailurePercentage) {
						allowed = (!hasFailureCount || float64(failedCount) <= toFloat(failureCount)) &&
							(!hasFailurePercentage || float64(failedCount)*100/float64(len(arr)) <= toFloat(failurePercentage))
					}
					status := "FAILED"
					if allowed {
						status, failed = "SUCCEEDED", nil
					}
					counts := map[string]any{
						"total": float64(len(arr)), "succeeded": float64(len(arr) - failedCount), "failed": float64(failedCount),
						"aborted": 0.0, "timedOut": 0.0, "pending": 0.0, "pendingRedrive": 0.0, "failuresNotRedrivable": 0.0, "resultsWritten": 0.0, "running": 0.0,
					}
					if hasResultWriter {
						var written int
						var writerOK bool
						mapOutput, written, writerOK = p.writeMapResults(ctx, req, st, data, itemResults, mapRunARN, mapScope)
						counts["resultsWritten"] = float64(written)
						if !writerOK {
							status = "FAILED"
							failed = &walkResult{out: data, status: "FAILED", cause: "States.ResultWriterFailed", errorName: "States.ResultWriterFailed"}
						}
					}
					mapRuns = append(mapRuns, mapRunDraft{arn: mapRunARN, label: label, record: map[string]any{
						"status": status, "executionCounts": counts, "itemCounts": counts, "redriveCount": 0.0,
						"maxConcurrency": toFloat(st["MaxConcurrency"]), "toleratedFailureCount": toFloat(failureCount), "toleratedFailurePercentage": toFloat(failurePercentage),
					}})
				}
				if failed == nil {
					if mapOutput == nil {
						mapOutput = []any{}
					}
					if isJSONata {
						data, ok = applyJSONataState(st, stateInput, mapOutput, stateContext, variables, p.deps.Rand)
						if ok {
							jsonataOutputApplied = true
							break
						}
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError"}
					} else if data, ok = applyStateResult(st, stateInput, mapOutput, p.deps.Rand); !ok {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", hist: hist}
					} else if applyJSONataAssignments(jsonataScope{
						input: stateInput, result: mapOutput, hasResult: true, context: stateContext, variables: variables, random: p.deps.Rand,
					}, variables, st) {
						break
					} else {
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError"}
					}
				}
				var next string
				var out any
				var retry, caught, valid bool
				if isJSONata {
					next, out, retry, caught, valid = recoverJSONata(st, *failed, stateInput, stateContext, variables, p.deps.Rand, retries[cur])
					if !valid {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				} else {
					next, out, retry, caught = recoverState(st, *failed, stateInput, retries[cur])
				}
				if retry {
					continue
				}
				if caught {
					if next == "" {
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
					failureName := failed.errorName
					if failureName == "" {
						failureName = "States.TaskFailed"
					}
					if !isJSONata && !applyCatchAssignments(st, stateFailure{name: failureName, cause: failed.cause}, stateInput, stateContext, variables, p.deps.Rand) {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
					data = out
					if !isJSONata {
						data, ok = applyDataPath(st, "OutputPath", out)
						if !ok {
							return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", hist: hist}
						}
					}
					cur = next
					continue walkLoop
				}
				failed.hist, failed.failedState, failed.failedInput = hist, cur, stateInput
				return *failed
			}
		default:
			return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
		}
		if isJSONata {
			if !jsonataOutputApplied {
				data, ok = applyJSONataState(st, rawInput, data, stateContext, variables, p.deps.Rand)
			}
		} else {
			data, ok = applyDataPath(st, "OutputPath", data)
			if ok && (typ == "Pass" || typ == "Wait") {
				ok = applyJSONataAssignments(jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand}, variables, st)
			}
		}
		if !ok {
			failure := "States.Runtime"
			if isJSONata {
				failure = "States.QueryEvaluationError"
			}
			return walkResult{out: rawInput, status: "FAILED", cause: failure, errorName: failure, hist: hist}
		}
		if end, _ := st["End"].(bool); end {
			return walkResult{out: data, status: "SUCCEEDED", hist: hist}
		}
		next, _ := st["Next"].(string)
		if next == "" {
			return walkResult{out: data, status: "SUCCEEDED", hist: hist}
		}
		cur = next
	}
	return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
}

type stateFailure struct{ name, cause string }

func taskFailure(prefix string, sdk bool, err error) stateFailure {
	var fault *spi.Fault
	if errors.As(err, &fault) {
		name := fault.Code
		if prefix == "SQS" {
			name = "AmazonSQSException"
		} else if sdk {
			if dot := strings.LastIndex(name, "."); dot >= 0 {
				name = name[dot+1:]
			}
			if prefix == "Sqs" && name == "NonExistentQueue" {
				name = "QueueDoesNotExist"
			}
			if !strings.HasSuffix(name, "Exception") {
				name += "Exception"
			}
		}
		if prefix != "" && !strings.HasPrefix(name, prefix+".") {
			name = prefix + "." + name
		}
		return stateFailure{name: name, cause: fault.Error()}
	}
	return stateFailure{name: "States.TaskFailed", cause: err.Error()}
}

func retryTask(st map[string]any, name string, attempts map[int]int) bool {
	for i, raw := range asSlice(st["Retry"]) {
		retrier, _ := raw.(map[string]any)
		if !matchesError(retrier["ErrorEquals"], name) {
			continue
		}
		maxAttempts := 3
		if raw, ok := retrier["MaxAttempts"]; ok {
			maxAttempts = int(toFloat(raw))
		}
		if attempts[i] >= maxAttempts {
			return false
		}
		attempts[i]++
		return true
	}
	return false
}

func catchTask(st map[string]any, failure stateFailure, input any) (string, any, bool) {
	for _, raw := range asSlice(st["Catch"]) {
		catcher, _ := raw.(map[string]any)
		if !matchesError(catcher["ErrorEquals"], failure.name) {
			continue
		}
		out, valid := applyResultPath(catcher, input, map[string]any{"Error": failure.name, "Cause": failure.cause})
		if !valid {
			return "", input, true
		}
		return first(catcher, "Next"), out, true
	}
	return "", input, false
}

func catchJSONata(state map[string]any, failure stateFailure, input any, context map[string]any, variables map[string]any, random spi.Rand) (string, any, bool, bool) {
	errorOutput := map[string]any{"Error": failure.name, "Cause": failure.cause}
	for _, raw := range asSlice(state["Catch"]) {
		catcher, _ := raw.(map[string]any)
		if !matchesError(catcher["ErrorEquals"], failure.name) {
			continue
		}
		scope := jsonataScope{
			input: input, errorOutput: errorOutput, hasErrorOutput: true,
			context: context, variables: variables, random: random,
		}
		output := input
		var ok bool
		if configured, exists := catcher["Output"]; exists {
			output, ok = evalJSONataValue(configured, scope)
			if !ok {
				return "", input, true, false
			}
		}
		if !applyJSONataAssignments(scope, variables, catcher) {
			return "", input, true, false
		}
		return first(catcher, "Next"), output, true, true
	}
	return "", input, false, true
}

func applyCatchAssignments(state map[string]any, failure stateFailure, input any, context map[string]any, variables map[string]any, random spi.Rand) bool {
	for _, raw := range asSlice(state["Catch"]) {
		catcher, _ := raw.(map[string]any)
		if matchesError(catcher["ErrorEquals"], failure.name) {
			return applyJSONataAssignments(jsonataScope{
				input: input, errorOutput: map[string]any{"Error": failure.name, "Cause": failure.cause}, hasErrorOutput: true,
				context: context, variables: variables, random: random,
			}, variables, catcher)
		}
	}
	return true
}

func recoverState(st map[string]any, failed walkResult, input any, attempts map[int]int) (string, any, bool, bool) {
	name := failed.errorName
	if name == "" {
		name = "States.TaskFailed"
	}
	failure := stateFailure{name: name, cause: failed.cause}
	if retryTask(st, name, attempts) {
		return "", input, true, false
	}
	next, out, caught := catchTask(st, failure, input)
	return next, out, false, caught
}

func recoverJSONata(state map[string]any, failed walkResult, input any, context map[string]any, variables map[string]any, random spi.Rand, attempts map[int]int) (string, any, bool, bool, bool) {
	name := failed.errorName
	if name == "" {
		name = "States.TaskFailed"
	}
	failure := stateFailure{name: name, cause: failed.cause}
	if retryTask(state, name, attempts) {
		return "", input, true, false, true
	}
	next, out, caught, valid := catchJSONata(state, failure, input, context, variables, random)
	return next, out, false, caught, valid
}

func matchesError(raw any, name string) bool {
	for _, candidate := range asSlice(raw) {
		switch fmt.Sprint(candidate) {
		case name:
			return true
		case "States.TaskFailed":
			if name != "States.Timeout" {
				return true
			}
		case "States.ALL":
			if name != "States.Runtime" && name != "States.DataLimitExceeded" {
				return true
			}
		}
	}
	return false
}

func applyResultPath(state map[string]any, input, result any) (any, bool) {
	raw, exists := state["ResultPath"]
	if !exists || raw == "$" {
		return result, true
	}
	if raw == nil {
		return input, true
	}
	path, ok := raw.(string)
	root, object := input.(map[string]any)
	if !ok || !object || !strings.HasPrefix(path, "$.") {
		return input, false
	}
	parts := strings.Split(strings.TrimPrefix(path, "$."), ".")
	cur := root
	for _, part := range parts[:len(parts)-1] {
		next, ok := cur[part].(map[string]any)
		if !ok {
			return input, false
		}
		cur = next
	}
	if parts[len(parts)-1] == "" {
		return input, false
	}
	cur[parts[len(parts)-1]] = result
	return input, true
}

func applyStateResult(state map[string]any, input, result any, random spi.Rand) (any, bool) {
	if selector, ok := state["ResultSelector"].(map[string]any); ok {
		result = applyParams(selector, result, nil, random)
	}
	return applyResultPath(state, input, result)
}

func applyDataPath(state map[string]any, key string, input any) (any, bool) {
	raw, exists := state[key]
	if !exists || raw == "$" {
		return input, true
	}
	if raw == nil {
		return map[string]any{}, true
	}
	path, ok := raw.(string)
	if !ok || !strings.HasPrefix(path, "$") {
		return input, false
	}
	selected := jsonPath(input, path)
	return selected, selected != nil
}

type jsonataScope struct {
	input, result, errorOutput any
	hasResult, hasErrorOutput  bool
	context                    map[string]any
	variables                  map[string]any
	random                     spi.Rand
}

func jsonataContext(req *spi.Request, state string, extra map[string]any) map[string]any {
	machineARN := first(req.Input, "_stateMachineArn", "stateMachineArn", "StateMachineArn")
	context := map[string]any{
		"State": map[string]any{"Name": state, "RetryCount": 0.0},
		"StateMachine": map[string]any{
			"Id": machineARN, "Name": baseSMName(machineARN),
		},
	}
	execution := map[string]any{}
	if arn := first(req.Input, "_executionArn"); arn != "" {
		execution["Id"] = arn
	}
	if name := first(req.Input, "_executionName", "name", "Name"); name != "" {
		execution["Name"] = name
	}
	if input, exists := req.Input["_executionInput"]; exists {
		execution["Input"] = input
	}
	if start := first(req.Input, "_executionStartTime"); start != "" {
		execution["StartTime"] = start
	}
	if role := first(req.Input, "_executionRoleArn"); role != "" {
		execution["RoleArn"] = role
	}
	execution["RedriveCount"] = toFloat(req.Input["_executionRedriveCount"])
	if redrive := first(req.Input, "_executionRedriveTime"); redrive != "" {
		execution["RedriveTime"] = redrive
	}
	if len(execution) != 0 {
		context["Execution"] = execution
	}
	for key, value := range extra {
		context[key] = value
	}
	return context
}

func setJSONataRetryCount(context map[string]any, attempts map[int]int) {
	count := 0
	for _, attempts := range attempts {
		count += attempts
	}
	context["State"].(map[string]any)["RetryCount"] = float64(count)
}

func mergeJSONataContext(context, extra map[string]any) map[string]any {
	merged := maps.Clone(context)
	for key, value := range extra {
		merged[key] = value
	}
	return merged
}

func evalJSONataValue(value any, scope jsonataScope) (any, bool) {
	switch value := value.(type) {
	case string:
		if !strings.HasPrefix(value, "{%") || !strings.HasSuffix(value, "%}") {
			return value, true
		}
		expression := strings.TrimSpace(value[2 : len(value)-2])
		compiled, err := jsonata.Compile("($mirrorValue := ("+expression+"); {'exists': $exists($mirrorValue), 'value': $mirrorValue})", false)
		if err != nil {
			return nil, false
		}
		compiled.SetMaxTime(1000)
		compiled.SetMaxDepth(100)
		compiled.SetMaxRange(10000)
		states := map[string]any{"input": scope.input, "context": scope.context}
		if scope.hasResult {
			states["result"] = scope.result
		}
		if scope.hasErrorOutput {
			states["errorOutput"] = scope.errorOutput
		}
		bindings := maps.Clone(scope.variables)
		bindings["states"] = states
		bindings["uuid"] = jsonata.JSONataFunc(func(args []any) (any, error) {
			if len(args) != 0 || scope.random == nil {
				return nil, errors.New("uuid takes no arguments")
			}
			return scope.random.UUID(), nil
		})
		bindings["random"] = jsonata.JSONataFunc(func(args []any) (any, error) {
			if len(args) > 1 || scope.random == nil {
				return nil, errors.New("random takes at most one argument")
			}
			random := scope.random
			if len(args) == 1 {
				random = internalrand.New(fmt.Sprint(args[0]))
			}
			return float64(random.Intn(1_000_000_000)) / 1_000_000_000, nil
		})
		bindings["parse"] = jsonata.JSONataFunc(func(args []any) (any, error) {
			if len(args) != 1 {
				return nil, errors.New("parse takes one argument")
			}
			var parsed any
			if err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &parsed); err != nil {
				return nil, err
			}
			return parsed, nil
		})
		bindings["hash"] = jsonata.JSONataFunc(func(args []any) (any, error) {
			if len(args) != 2 {
				return nil, errors.New("hash takes two arguments")
			}
			hash, ok := intrinsicHash(fmt.Sprint(args[0]), fmt.Sprint(args[1]))
			if !ok {
				return nil, errors.New("unsupported hash algorithm")
			}
			return hash, nil
		})
		bindings["partition"] = jsonata.JSONataFunc(func(args []any) (any, error) {
			if len(args) != 2 {
				return nil, errors.New("partition takes two arguments")
			}
			items, ok := args[0].([]any)
			if array, wrapped := args[0].(*jsonata.JSONataArray); wrapped {
				items, ok = make([]any, array.Length()), true
				for index := range items {
					items[index] = array.Get(index)
				}
			}
			size, numeric := intrinsicNumber(args[1])
			width := int(math.Floor(size))
			if !ok || !numeric || width < 1 {
				return nil, errors.New("invalid partition arguments")
			}
			parts := []any{}
			for len(items) != 0 {
				n := min(width, len(items))
				parts, items = append(parts, append([]any(nil), items[:n]...)), items[n:]
			}
			return parts, nil
		})
		bindings["range"] = jsonata.JSONataFunc(func(args []any) (any, error) {
			if len(args) != 3 {
				return nil, errors.New("range takes three arguments")
			}
			startValue, startOK := intrinsicNumber(args[0])
			endValue, endOK := intrinsicNumber(args[1])
			stepValue, stepOK := intrinsicNumber(args[2])
			start, end, step := int(math.Floor(startValue)), int(math.Floor(endValue)), int(math.Floor(stepValue))
			if !startOK || !endOK || !stepOK || step == 0 {
				return nil, errors.New("invalid range arguments")
			}
			values := []any{}
			for n := start; len(values) < 10000 && (step > 0 && n <= end || step < 0 && n >= end); n += step {
				values = append(values, float64(n))
			}
			if len(values) == 10000 {
				return nil, errors.New("range exceeds limit")
			}
			return values, nil
		})
		result, err := compiled.Evaluate([]byte("{}"), bindings)
		if err != nil {
			return nil, false
		}
		var wrapped map[string]any
		if json.Unmarshal(result, &wrapped) != nil || !toBool(wrapped["exists"]) {
			return nil, false
		}
		resultValue, exists := wrapped["value"]
		return resultValue, exists
	case map[string]any:
		out := make(map[string]any, len(value))
		for key, item := range value {
			resolved, ok := evalJSONataValue(item, scope)
			if !ok {
				return nil, false
			}
			out[key] = resolved
		}
		return out, true
	case []any:
		out := make([]any, len(value))
		for index, item := range value {
			resolved, ok := evalJSONataValue(item, scope)
			if !ok {
				return nil, false
			}
			out[index] = resolved
		}
		return out, true
	default:
		return value, true
	}
}

func applyJSONataState(state map[string]any, input, result any, context map[string]any, variables map[string]any, random spi.Rand) (any, bool) {
	scope := jsonataScope{input: input, result: result, hasResult: true, context: context, variables: variables, random: random}
	output := result
	var ok bool
	if configured, exists := state["Output"]; exists {
		output, ok = evalJSONataValue(configured, scope)
		if !ok {
			return input, false
		}
	}
	return output, applyJSONataAssignments(scope, variables, state)
}

func applyJSONataAssignments(scope jsonataScope, variables map[string]any, owners ...map[string]any) bool {
	assignments := map[string]any{}
	for _, owner := range owners {
		assign, _ := owner["Assign"].(map[string]any)
		for name, expression := range assign {
			if strings.ContainsAny(name, ".[]") || name == "states" {
				return false
			}
			value, ok := evalJSONataValue(expression, scope)
			if !ok {
				return false
			}
			assignments[name] = value
		}
	}
	for name, value := range assignments {
		variables[name] = value
	}
	return true
}

func taskPayload(st map[string]any, data any, context map[string]any, random spi.Rand) any {
	params, ok := st["Parameters"].(map[string]any)
	if !ok {
		return data
	}
	p := applyParams(params, data, context, random)
	return p
}

func (p *Pack) mapItems(ctx context.Context, req *spi.Request, state map[string]any, data any, scope *jsonataScope) ([]any, string, bool) {
	reader, hasReader := state["ItemReader"].(map[string]any)
	if !hasReader {
		items := data
		if path := first(state, "ItemsPath"); path != "" {
			items = jsonPath(data, path)
		}
		array, ok := items.([]any)
		return array, "STATE_DATA", ok
	}
	if first(reader, "Resource") != "arn:aws:states:::s3:getObject" {
		return nil, "", false
	}
	parameters, _ := reader["Parameters"].(map[string]any)
	resolved := any(applyParams(parameters, data, nil, p.deps.Rand))
	if scope != nil {
		var valid bool
		resolved, valid = evalJSONataValue(reader["Arguments"], *scope)
		if _, configured := reader["Arguments"]; !configured {
			return nil, "", false
		}
		if !valid {
			return nil, "", false
		}
	}
	input, valid := resolved.(map[string]any)
	if !valid {
		return nil, "", false
	}
	response, err := s3.New(p.deps).Invoke(ctx, &spi.Request{
		Identity: req.Identity, Operation: "GetObject", Input: input,
	})
	if err != nil || response == nil || response.Stream == nil {
		return nil, "", false
	}
	body, readErr := io.ReadAll(response.Stream)
	closeErr := response.Stream.Close()
	if readErr != nil || closeErr != nil {
		return nil, "", false
	}
	config, _ := reader["ReaderConfig"].(map[string]any)
	inputType := first(config, "InputType")
	if inputType == "" {
		inputType = "JSON"
	}
	switch inputType {
	case "JSON":
		var items []any
		if json.Unmarshal(body, &items) != nil {
			return nil, "", false
		}
		return limitReaderItems(items, inputType, config, data, scope)
	case "JSONL":
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		items := []any{}
		for {
			var item any
			if err := decoder.Decode(&item); errors.Is(err, io.EOF) {
				return limitReaderItems(items, inputType, config, data, scope)
			} else if err != nil {
				return nil, "", false
			}
			items = append(items, item)
		}
	case "CSV":
		parser := csv.NewReader(strings.NewReader(string(body)))
		delimiters := map[string]rune{"COMMA": ',', "PIPE": '|', "SEMICOLON": ';', "SPACE": ' ', "TAB": '\t'}
		if delimiter := first(config, "CSVDelimiter"); delimiter != "" {
			parser.Comma = delimiters[delimiter]
			if parser.Comma == 0 {
				return nil, "", false
			}
		}
		records, err := parser.ReadAll()
		if err != nil || len(records) == 0 {
			return nil, "", false
		}
		headers := records[0]
		if first(config, "CSVHeaderLocation") == "GIVEN" {
			headers = nil
			for _, header := range asSlice(config["CSVHeaders"]) {
				headers = append(headers, fmt.Sprint(header))
			}
		} else {
			records = records[1:]
		}
		items := make([]any, 0, len(records))
		for _, record := range records {
			if len(record) != len(headers) {
				return nil, "", false
			}
			item := map[string]any{}
			for index, header := range headers {
				item[header] = record[index]
			}
			items = append(items, item)
		}
		return limitReaderItems(items, inputType, config, data, scope)
	case "PARQUET":
		file, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return nil, "", false
		}
		items := []any{}
		for _, group := range file.RowGroups() {
			reader := parquet.NewGenericRowGroupReader[any](group)
			rows := make([]any, int(reader.NumRows()))
			n, readErr := reader.Read(rows)
			closeErr := reader.Close()
			if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
				return nil, "", false
			}
			for _, row := range rows[:n] {
				encoded, err := json.Marshal(row)
				var item any
				if err != nil || json.Unmarshal(encoded, &item) != nil {
					return nil, "", false
				}
				items = append(items, item)
			}
		}
		return limitReaderItems(items, inputType, config, data, scope)
	default:
		return nil, "", false
	}
}

func limitReaderItems(items []any, source string, config map[string]any, data any, scope *jsonataScope) ([]any, string, bool) {
	value, hasValue := config["MaxItems"]
	path, hasPath := config["MaxItemsPath"].(string)
	if !hasValue && !hasPath {
		return items, source, true
	}
	if scope != nil {
		var valid bool
		value, valid = evalJSONataValue(value, *scope)
		if !valid || hasPath {
			return nil, "", false
		}
	} else if hasValue == hasPath {
		return nil, "", false
	} else if hasPath {
		value = jsonPath(data, path)
	}
	maximum, numeric := exactNumber(value)
	limit := int(maximum)
	if !numeric || maximum != math.Trunc(maximum) || limit < 1 {
		return nil, "", false
	}
	if limit < len(items) {
		items = items[:limit]
	}
	return items, source, true
}

func batchMapItems(state map[string]any, data any, items []any, random spi.Rand, scope *jsonataScope) ([]any, bool) {
	batcher, exists := state["ItemBatcher"].(map[string]any)
	if !exists {
		return items, true
	}
	resolveLimit := func(valueKey, pathKey string) (int, bool) {
		value, hasValue := batcher[valueKey]
		path, hasPath := batcher[pathKey].(string)
		if scope != nil && hasValue {
			var valid bool
			value, valid = evalJSONataValue(value, *scope)
			if !valid {
				return 0, false
			}
			hasValue = true
		}
		if hasValue == hasPath {
			return 0, false
		}
		if hasPath {
			value = jsonPath(data, path)
		}
		number, ok := exactNumber(value)
		return int(number), ok && number == math.Trunc(number) && number > 0
	}
	maxItems, hasMaxItems := 0, false
	if _, direct := batcher["MaxItemsPerBatch"]; direct || batcher["MaxItemsPerBatchPath"] != nil {
		maxItems, hasMaxItems = resolveLimit("MaxItemsPerBatch", "MaxItemsPerBatchPath")
		if !hasMaxItems {
			return nil, false
		}
	}
	maxBytes, hasMaxBytes := 262144, false
	if _, direct := batcher["MaxInputBytesPerBatch"]; direct || batcher["MaxInputBytesPerBatchPath"] != nil {
		maxBytes, hasMaxBytes = resolveLimit("MaxInputBytesPerBatch", "MaxInputBytesPerBatchPath")
		if !hasMaxBytes || maxBytes > 262144 {
			return nil, false
		}
	}
	if !hasMaxItems && !hasMaxBytes {
		return nil, false
	}
	var batchInput map[string]any
	if raw, configured := batcher["BatchInput"]; configured {
		if scope != nil {
			resolved, valid := evalJSONataValue(raw, *scope)
			var ok bool
			batchInput, ok = resolved.(map[string]any)
			if !valid || !ok {
				return nil, false
			}
		} else {
			input, ok := raw.(map[string]any)
			if !ok {
				return nil, false
			}
			batchInput = applyParams(input, data, nil, random)
		}
	}
	makeBatch := func(values []any) map[string]any {
		batch := map[string]any{"Items": append([]any(nil), values...)}
		if batchInput != nil {
			batch["BatchInput"] = batchInput
		}
		return batch
	}
	batched, current := []any{}, []any{}
	for _, item := range items {
		candidate := append(append([]any(nil), current...), item)
		encoded, _ := json.Marshal(makeBatch(candidate))
		if len(current) != 0 && (hasMaxItems && len(candidate) > maxItems || len(encoded) > maxBytes) {
			batched = append(batched, makeBatch(current))
			candidate = []any{item}
			encoded, _ = json.Marshal(makeBatch(candidate))
		}
		if len(encoded) > maxBytes {
			return nil, false
		}
		current = candidate
		if hasMaxItems && len(current) == maxItems {
			batched, current = append(batched, makeBatch(current)), nil
		}
	}
	if len(current) != 0 {
		batched = append(batched, makeBatch(current))
	}
	return batched, true
}

func mapStateMachineName(req *spi.Request) string {
	if arn := first(req.Input, "stateMachineArn", "StateMachineArn"); arn != "" {
		return baseSMName(arn)
	}
	parts := strings.Split(first(req.Input, "executionArn", "ExecutionArn"), ":")
	if len(parts) > 6 {
		return strings.Split(parts[6], "/")[0]
	}
	return "stateMachine"
}

func (p *Pack) mapItemResult(req *spi.Request, state string, input any, result walkResult) mapItemResult {
	name := p.deps.Rand.UUID()
	machine := mapStateMachineName(req) + "/" + state
	inputJSON, _ := json.Marshal(input)
	metadata := map[string]any{
		"ExecutionArn": "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":execution:" + machine + ":" + name,
		"Input":        string(inputJSON), "InputDetails": map[string]any{"Included": true}, "Name": name,
		"StartDate": p.deps.Clock.Now().UTC().Format(time.RFC3339Nano), "StateMachineArn": p.smARN(req, machine), "Status": result.status,
		"StopDate": p.deps.Clock.Now().UTC().Format(time.RFC3339Nano),
	}
	if result.status == "SUCCEEDED" {
		outputJSON, _ := json.Marshal(result.out)
		metadata["Output"], metadata["OutputDetails"] = string(outputJSON), map[string]any{"Included": true}
	} else {
		metadata["Error"], metadata["Cause"] = result.errorName, result.cause
	}
	itemCount := 1
	if batch, ok := input.(map[string]any); ok {
		if items, ok := batch["Items"].([]any); ok {
			itemCount = len(items)
		}
	}
	return mapItemResult{metadata: metadata, output: result.out, succeeded: result.status == "SUCCEEDED", itemCount: itemCount, history: result.hist}
}

func (p *Pack) storeMapItemExecution(ctx context.Context, req *spi.Request, mapRunARN, definition string, item mapItemResult) {
	metadata := item.metadata
	now := float64(p.deps.Clock.Now().Unix())
	record := map[string]any{
		"executionArn": metadata["ExecutionArn"], "stateMachineArn": metadata["StateMachineArn"], "name": metadata["Name"],
		"status": metadata["Status"], "startDate": now, "stopDate": now, "input": metadata["Input"], "mapRunArn": mapRunARN,
		"itemCount": float64(item.itemCount), "type": "STANDARD", "definition": definition, "history": item.history,
		"stateMachineName": lastSeg(first(metadata, "StateMachineArn"), ":"),
	}
	if machine, found := p.resolveStateMachine(ctx, req, first(req.Input, "stateMachineArn", "StateMachineArn")); found {
		record["roleArn"], record["revisionId"] = machine["roleArn"], machine["revisionId"]
	}
	for _, field := range []string{"Output", "Error", "Cause"} {
		if value, exists := metadata[field]; exists {
			record[strings.ToLower(field)] = value
		}
	}
	encoded, _ := json.Marshal(record)
	_ = p.col(req, "ex").Put(ctx, first(metadata, "ExecutionArn"), encoded)
}

func (p *Pack) writeMapResults(ctx context.Context, req *spi.Request, state map[string]any, data any, items []mapItemResult, mapRunARN string, scope *jsonataScope) (any, int, bool) {
	writer, ok := state["ResultWriter"].(map[string]any)
	if !ok || len(writer) == 0 {
		return nil, 0, false
	}
	config, hasConfig := writer["WriterConfig"].(map[string]any)
	resource := first(writer, "Resource")
	parameters, hasParameters := writer["Parameters"].(map[string]any)
	if !hasParameters {
		parameters, hasParameters = writer["Arguments"].(map[string]any)
	}
	if resource == "" && !hasConfig || resource != "" && (resource != "arn:aws:states:::s3:putObject" || !hasParameters) {
		return nil, 0, false
	}
	transformation := first(config, "Transformation")
	if transformation == "" {
		transformation = "COMPACT"
		if resource != "" {
			transformation = "NONE"
		}
	}
	outputType := first(config, "OutputType")
	if outputType == "" {
		outputType = "JSON"
	}
	if transformation != "NONE" && transformation != "COMPACT" && transformation != "FLATTEN" || outputType != "JSON" && outputType != "JSONL" {
		return nil, 0, false
	}
	transform := func(selected []mapItemResult) []any {
		out := []any{}
		for _, item := range selected {
			if !item.succeeded || transformation == "NONE" {
				out = append(out, item.metadata)
			} else if values, flatten := item.output.([]any); transformation == "FLATTEN" && flatten {
				out = append(out, values...)
			} else {
				out = append(out, item.output)
			}
		}
		return out
	}
	encode := func(values []any) ([]byte, bool) {
		if outputType == "JSON" {
			encoded, err := json.Marshal(values)
			return encoded, err == nil
		}
		var encoded bytes.Buffer
		encoder := json.NewEncoder(&encoded)
		for _, value := range values {
			if encoder.Encode(value) != nil {
				return nil, false
			}
		}
		return encoded.Bytes(), true
	}
	formatted := transform(items)
	if resource == "" {
		if outputType == "JSON" {
			return formatted, 0, true
		}
		encoded, valid := encode(formatted)
		return string(encoded), 0, valid
	}
	resolved := applyParams(parameters, data, nil, p.deps.Rand)
	if scope != nil {
		value, valid := evalJSONataValue(writer["Arguments"], *scope)
		if !valid {
			return nil, 0, false
		}
		resolved, valid = value.(map[string]any)
		if !valid {
			return nil, 0, false
		}
	}
	bucket, prefix := first(resolved, "Bucket"), strings.Trim(first(resolved, "Prefix"), "/")
	if bucket == "" {
		return nil, 0, false
	}
	if prefix != "" {
		prefix += "/"
	}
	prefix += lastSeg(mapRunARN, ":")
	resultFiles := map[string]any{"SUCCEEDED": []any{}, "FAILED": []any{}, "PENDING": []any{}}
	storage := s3.New(p.deps)
	put := func(key string, body []byte) bool {
		_, err := storage.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "PutObject", Input: map[string]any{"Bucket": bucket, "Key": key}, Body: io.NopCloser(bytes.NewReader(body))})
		return err == nil
	}
	for _, status := range []string{"SUCCEEDED", "FAILED"} {
		selected := []mapItemResult{}
		for _, item := range items {
			if item.metadata["Status"] == status {
				selected = append(selected, item)
			}
		}
		if len(selected) == 0 {
			continue
		}
		body, valid := encode(transform(selected))
		key := prefix + "/" + status + "_0.json"
		if !valid || !put(key, body) {
			return nil, 0, false
		}
		resultFiles[status] = []any{map[string]any{"Key": key, "Size": len(body)}}
	}
	manifestKey := prefix + "/manifest.json"
	manifest, _ := json.Marshal(map[string]any{"DestinationBucket": bucket, "MapRunArn": mapRunARN, "ResultFiles": resultFiles})
	if !put(manifestKey, manifest) {
		return nil, 0, false
	}
	return map[string]any{"MapRunArn": mapRunARN, "ResultWriterDetails": map[string]any{"Bucket": bucket, "Key": manifestKey}}, len(items), true
}

func taskIdentity(st map[string]any, data any, identity spi.Identity, random spi.Rand, scope *jsonataScope) (spi.Identity, bool) {
	credentials, ok := st["Credentials"].(map[string]any)
	if !ok {
		return identity, false
	}
	resolved := applyParams(credentials, data, nil, random)
	if scope != nil {
		value, valid := evalJSONataValue(credentials, *scope)
		if !valid {
			return identity, false
		}
		resolved, ok = value.(map[string]any)
		if !ok {
			return identity, false
		}
	}
	roleARN := first(resolved, "RoleArn")
	parts := strings.Split(roleARN, ":")
	if !validRoleARN(roleARN) || len(parts) < 6 || parts[4] == "" {
		return identity, false
	}
	identity.Account, identity.ARN, identity.AccessKeyID = parts[4], roleARN, ""
	return identity, true
}

func applyParams(params map[string]any, data any, context map[string]any, random spi.Rand) map[string]any {
	out := map[string]any{}
	for k, v := range params {
		if strings.HasSuffix(k, ".$") {
			path := fmt.Sprint(v)
			if strings.HasPrefix(path, "States.") {
				out[strings.TrimSuffix(k, ".$")], _ = evalIntrinsic(path, data, context, random)
				continue
			}
			source := data
			if strings.HasPrefix(path, "$$.") {
				source, path = context, strings.TrimPrefix(path, "$")
			}
			out[strings.TrimSuffix(k, ".$")] = jsonPath(source, path)
			continue
		}
		if m, ok := v.(map[string]any); ok {
			out[k] = applyParams(m, data, context, random)
			continue
		}
		out[k] = v
	}
	return out
}

func evalIntrinsic(expression string, data any, context map[string]any, random spi.Rand) (any, bool) {
	open := strings.IndexByte(expression, '(')
	if open < len("States.") || !strings.HasSuffix(expression, ")") {
		return nil, false
	}
	name := expression[len("States."):open]
	rawArgs := splitIntrinsicArgs(expression[open+1 : len(expression)-1])
	args := make([]any, len(rawArgs))
	for i, raw := range rawArgs {
		var ok bool
		args[i], ok = evalIntrinsicArg(raw, data, context, random)
		if !ok {
			return nil, false
		}
	}
	switch name {
	case "Array":
		return args, true
	case "ArrayPartition":
		if len(args) != 2 {
			break
		}
		array, ok := args[0].([]any)
		number, numeric := intrinsicNumber(args[1])
		size := int(math.Round(number))
		if !ok || !numeric || size <= 0 {
			break
		}
		parts := []any{}
		for len(array) > 0 {
			n := min(size, len(array))
			parts = append(parts, append([]any(nil), array[:n]...))
			array = array[n:]
		}
		return parts, true
	case "ArrayContains":
		if len(args) != 2 {
			break
		}
		array, ok := args[0].([]any)
		if !ok {
			break
		}
		for _, item := range array {
			if reflect.DeepEqual(item, args[1]) {
				return true, true
			}
		}
		return false, true
	case "ArrayRange":
		if len(args) != 3 {
			break
		}
		startValue, startOK := intrinsicNumber(args[0])
		endValue, endOK := intrinsicNumber(args[1])
		stepValue, stepOK := intrinsicNumber(args[2])
		start, end, step := int(math.Round(startValue)), int(math.Round(endValue)), int(math.Round(stepValue))
		if !startOK || !endOK || !stepOK || step == 0 {
			break
		}
		values := []any{}
		for n := start; len(values) < 1000 && (step > 0 && n <= end || step < 0 && n >= end); n += step {
			values = append(values, float64(n))
		}
		if len(values) == 1000 {
			last := int(values[len(values)-1].(float64))
			if step > 0 && last+step <= end || step < 0 && last+step >= end {
				break
			}
		}
		return values, true
	case "ArrayGetItem":
		if len(args) != 2 {
			break
		}
		array, ok := args[0].([]any)
		number, numeric := intrinsicNumber(args[1])
		index := int(math.Round(number))
		if !ok || !numeric || index < 0 || index >= len(array) {
			break
		}
		return array[index], true
	case "ArrayLength":
		if len(args) == 1 {
			if array, ok := args[0].([]any); ok {
				return float64(len(array)), true
			}
		}
	case "ArrayUnique":
		if len(args) != 1 {
			break
		}
		array, ok := args[0].([]any)
		if !ok {
			break
		}
		unique := []any{}
		for _, item := range array {
			found := false
			for _, existing := range unique {
				found = found || reflect.DeepEqual(existing, item)
			}
			if !found {
				unique = append(unique, item)
			}
		}
		return unique, true
	case "Base64Encode":
		if len(args) == 1 {
			return base64.StdEncoding.EncodeToString([]byte(fmt.Sprint(args[0]))), true
		}
	case "Base64Decode":
		if len(args) == 1 {
			decoded, err := base64.StdEncoding.DecodeString(fmt.Sprint(args[0]))
			return string(decoded), err == nil
		}
	case "Hash":
		if len(args) == 2 {
			return intrinsicHash(fmt.Sprint(args[0]), fmt.Sprint(args[1]))
		}
	case "JsonMerge":
		if len(args) != 3 || toBool(args[2]) {
			break
		}
		left, lok := args[0].(map[string]any)
		right, rok := args[1].(map[string]any)
		if !lok || !rok {
			break
		}
		merged := map[string]any{}
		for k, v := range left {
			merged[k] = v
		}
		for k, v := range right {
			merged[k] = v
		}
		return merged, true
	case "StringToJson":
		if len(args) == 1 {
			var value any
			err := json.Unmarshal([]byte(fmt.Sprint(args[0])), &value)
			return value, err == nil
		}
	case "JsonToString":
		if len(args) == 1 {
			encoded, err := json.Marshal(args[0])
			return string(encoded), err == nil
		}
	case "MathAdd":
		if len(args) == 2 {
			left, lok := intrinsicNumber(args[0])
			right, rok := intrinsicNumber(args[1])
			if lok && rok {
				return math.Round(left) + math.Round(right), true
			}
		}
	case "MathRandom":
		if len(args) == 2 || len(args) == 3 {
			startValue, startOK := intrinsicNumber(args[0])
			endValue, endOK := intrinsicNumber(args[1])
			start, end := int(math.Round(startValue)), int(math.Round(endValue))
			if !startOK || !endOK || start >= end {
				break
			}
			if len(args) == 3 {
				seed, ok := intrinsicNumber(args[2])
				if !ok {
					break
				}
				return float64(start + internalrand.New(strconv.FormatInt(int64(math.Round(seed)), 10)).Intn(end-start)), true
			}
			if random != nil {
				return float64(start + random.Intn(end-start)), true
			}
		}
	case "StringSplit":
		if len(args) == 2 {
			separators := fmt.Sprint(args[1])
			parts := strings.FieldsFunc(fmt.Sprint(args[0]), func(r rune) bool { return strings.ContainsRune(separators, r) })
			out := make([]any, len(parts))
			for i, part := range parts {
				out[i] = part
			}
			return out, true
		}
	case "Format":
		if len(args) > 0 {
			formatted := fmt.Sprint(args[0])
			if strings.Count(formatted, "{}") != len(args)-1 {
				break
			}
			for _, arg := range args[1:] {
				formatted = strings.Replace(formatted, "{}", fmt.Sprint(arg), 1)
			}
			return formatted, true
		}
	case "UUID":
		if len(args) == 0 && random != nil {
			return random.UUID(), true
		}
	}
	return nil, false
}

func evalIntrinsicArg(raw string, data any, context map[string]any, random spi.Rand) (any, bool) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "States.") {
		return evalIntrinsic(raw, data, context, random)
	}
	if strings.HasPrefix(raw, "$$.") {
		return jsonPath(context, strings.TrimPrefix(raw, "$")), true
	}
	if strings.HasPrefix(raw, "$") {
		return jsonPath(data, raw), true
	}
	if len(raw) >= 2 && raw[0] == '\'' && raw[len(raw)-1] == '\'' {
		return unescapeIntrinsic(raw[1 : len(raw)-1])
	}
	var value any
	if json.Unmarshal([]byte(raw), &value) == nil {
		return value, true
	}
	return nil, false
}

func splitIntrinsicArgs(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var args []string
	start, depth := 0, 0
	quoted, escaped := false, false
	for i, r := range raw {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && quoted {
			escaped = true
			continue
		}
		if r == '\'' {
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		switch r {
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				args = append(args, raw[start:i])
				start = i + 1
			}
		}
	}
	return append(args, raw[start:])
}

func unescapeIntrinsic(raw string) (string, bool) {
	var out strings.Builder
	escaped := false
	for _, r := range raw {
		if escaped {
			if !strings.ContainsRune("'{}\\", r) {
				return "", false
			}
			out.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		out.WriteRune(r)
	}
	return out.String(), !escaped
}

func intrinsicHash(data, algorithm string) (any, bool) {
	var sum []byte
	switch algorithm {
	case "MD5":
		value := md5.Sum([]byte(data))
		sum = value[:]
	case "SHA-1":
		value := sha1.Sum([]byte(data))
		sum = value[:]
	case "SHA-256":
		value := sha256.Sum256([]byte(data))
		sum = value[:]
	case "SHA-384":
		value := sha512.Sum384([]byte(data))
		sum = value[:]
	case "SHA-512":
		value := sha512.Sum512([]byte(data))
		sum = value[:]
	default:
		return nil, false
	}
	return hex.EncodeToString(sum), true
}

func intrinsicNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case float32:
		return float64(number), true
	case int:
		return float64(number), true
	case int64:
		return float64(number), true
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func (p *Pack) invokeTask(ctx context.Context, req *spi.Request, resource string, payload any) (any, error, string, bool, bool) {
	callback := strings.HasSuffix(resource, ".waitForTaskToken")
	resource, syncJSON, syncJob := syncTaskResource(resource)
	resource = strings.TrimSuffix(resource, ".waitForTaskToken")
	if callback && !callbackIntegration(resource) {
		return nil, nil, "", false, false
	}
	if syncJob && !syncIntegration(resource) {
		return nil, nil, "", false, false
	}
	if strings.Contains(resource, ":function:") || strings.Contains(resource, "lambda:invoke") {
		output, err := p.invokeLambda(ctx, req, resource, payload)
		return output, err, "Lambda", false, true
	}
	service, operation, prefix, sdk, supported := taskIntegration(resource)
	if !supported {
		return nil, nil, "", false, false
	}
	input, valid := payload.(map[string]any)
	if !valid {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}, prefix, sdk, true
	}
	if service == "states" {
		input = maps.Clone(input)
		if nestedInput, exists := input["Input"]; exists {
			if _, encoded := nestedInput.(string); !encoded {
				body, err := json.Marshal(nestedInput)
				if err != nil {
					return nil, err, prefix, sdk, true
				}
				input["Input"] = string(body)
			}
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: operation, Input: input})
		if response == nil {
			return nil, err, prefix, sdk, true
		}
		if err == nil && syncJob {
			described, describeErr := p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "DescribeExecution", Input: map[string]any{"executionArn": response.Output["executionArn"]}})
			if describeErr != nil {
				return nil, describeErr, prefix, sdk, true
			}
			if described == nil {
				return nil, errors.New("nested execution returned no result"), prefix, sdk, true
			}
			if first(described.Output, "status") != "SUCCEEDED" {
				return nil, errors.New(first(described.Output, "cause", "error", "status")), prefix, sdk, true
			}
			return nestedExecutionOutput(described.Output, syncJSON), nil, prefix, sdk, true
		}
		return response.Output, err, prefix, sdk, true
	}
	if service == "ecs" {
		input = maps.Clone(input)
		input["cluster"], input["taskDefinition"] = first(input, "cluster", "Cluster"), first(input, "taskDefinition", "TaskDefinition")
	}
	for _, factory := range registry.Factories() {
		if !matchesTaskService(factory.ServiceID, service) {
			continue
		}
		handler, err := factory.New(p.deps)
		if err != nil {
			return nil, err, prefix, sdk, true
		}
		// ponytail: construct and close integrations per task; cache only if profiling shows initialization cost matters.
		if !slices.Contains(handler.Operations(), operation) {
			if closer, ok := handler.(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			return nil, spi.NotImplemented(factory.ServiceID, operation, "emulate"), prefix, sdk, true
		}
		response, invokeErr := handler.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: operation, Input: input})
		if invokeErr == nil && response != nil && service == "ecs" && syncJob {
			if len(asSlice(response.Output["failures"])) != 0 {
				invokeErr = &spi.Fault{Code: "Unknown", HTTPStatus: 400, Fault: "server"}
			} else {
				stopped := []any{}
				for _, task := range asSlice(response.Output["tasks"]) {
					taskRecord, _ := task.(map[string]any)
					stop, err := handler.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "StopTask", Input: map[string]any{"cluster": input["cluster"], "task": first(taskRecord, "taskArn")}})
					if err != nil {
						invokeErr = errors.Join(invokeErr, err)
						break
					}
					if stop == nil {
						invokeErr = errors.New("ECS StopTask returned no result")
						break
					}
					stopped = append(stopped, stop.Output["task"])
				}
				response.Output["tasks"] = stopped
			}
		}
		if closer, ok := handler.(interface{ Close() error }); ok {
			invokeErr = errors.Join(invokeErr, closer.Close())
		}
		if response == nil {
			return nil, invokeErr, prefix, sdk, true
		}
		return response.Output, invokeErr, prefix, sdk, true
	}
	return nil, spi.NotImplemented("aws."+service, operation, "emulate"), prefix, sdk, true
}

func callbackIntegration(resource string) bool {
	switch resource {
	case "arn:aws:states:::sqs:sendMessage", "arn:aws:states:::sns:publish",
		"arn:aws:states:::states:startExecution", "arn:aws:states:::lambda:invoke":
		return true
	default:
		return false
	}
}

func syncIntegration(resource string) bool {
	switch resource {
	case "arn:aws:states:::batch:submitJob", "arn:aws:states:::codebuild:startBuild",
		"arn:aws:states:::glue:startJobRun", "arn:aws:states:::elasticmapreduce:addStep",
		"arn:aws:states:::elasticmapreduce:createCluster", "arn:aws:states:::states:startExecution",
		"arn:aws:states:::ecs:runTask":
		return true
	default:
		return false
	}
}

func syncTaskResource(resource string) (string, bool, bool) {
	if strings.HasSuffix(resource, ".sync:2") {
		return strings.TrimSuffix(resource, ".sync:2"), true, true
	}
	if strings.HasSuffix(resource, ".sync") {
		return strings.TrimSuffix(resource, ".sync"), false, true
	}
	return resource, false, false
}

func nestedExecutionOutput(execution map[string]any, jsonOutput bool) map[string]any {
	output := map[string]any{
		"ExecutionArn": execution["executionArn"], "Input": execution["input"], "InputDetails": map[string]any{"Included": true},
		"Name": execution["name"], "Output": execution["output"], "OutputDetails": map[string]any{"Included": true},
		"StartDate": execution["startDate"], "StateMachineArn": execution["stateMachineArn"], "Status": execution["status"], "StopDate": execution["stopDate"],
	}
	if jsonOutput {
		output["Output"] = parseJSON(first(execution, "output"))
	}
	return output
}

func taskIntegration(resource string) (service, operation, prefix string, sdk, ok bool) {
	optimized := map[string][3]string{
		"arn:aws:states:::batch:submitJob":                {"batch", "SubmitJob", "Batch"},
		"arn:aws:states:::codebuild:startBuild":           {"codebuild", "StartBuild", "CodeBuild"},
		"arn:aws:states:::sqs:sendMessage":                {"sqs", "SendMessage", "SQS"},
		"arn:aws:states:::sns:publish":                    {"sns", "Publish", "SNS"},
		"arn:aws:states:::dynamodb:getItem":               {"dynamodb", "GetItem", "DynamoDB"},
		"arn:aws:states:::dynamodb:putItem":               {"dynamodb", "PutItem", "DynamoDB"},
		"arn:aws:states:::dynamodb:updateItem":            {"dynamodb", "UpdateItem", "DynamoDB"},
		"arn:aws:states:::dynamodb:deleteItem":            {"dynamodb", "DeleteItem", "DynamoDB"},
		"arn:aws:states:::ecs:runTask":                    {"ecs", "RunTask", "AmazonECS"},
		"arn:aws:states:::elasticmapreduce:addStep":       {"elasticmapreduce", "AddJobFlowSteps", "ElasticMapReduce"},
		"arn:aws:states:::elasticmapreduce:createCluster": {"elasticmapreduce", "RunJobFlow", "ElasticMapReduce"},
		"arn:aws:states:::glue:startJobRun":               {"glue", "StartJobRun", "Glue"},
		"arn:aws:states:::states:startExecution":          {"states", "StartExecution", "StepFunctions"},
	}
	if integration, found := optimized[resource]; found {
		return integration[0], integration[1], integration[2], false, true
	}
	const marker = "arn:aws:states:::aws-sdk:"
	if !strings.HasPrefix(resource, marker) {
		return "", "", "", false, false
	}
	service, action, found := strings.Cut(strings.TrimPrefix(resource, marker), ":")
	if !found || service == "" || action == "" || strings.Contains(action, ".") {
		return "", "", "", false, false
	}
	operation = strings.ToUpper(action[:1]) + action[1:]
	aliases := map[string]string{"sfn": "states", "cloudwatch": "monitoring", "resourcegroupstaggingapi": "tagging"}
	if alias := aliases[service]; alias != "" {
		service = alias
	}
	prefixes := map[string]string{"dynamodb": "DynamoDb", "sqs": "Sqs", "sns": "Sns", "states": "Sfn"}
	prefix = prefixes[service]
	if prefix == "" {
		prefix = strings.ToUpper(service[:1]) + service[1:]
	}
	return service, operation, prefix, true, true
}

func matchesTaskService(serviceID, requested string) bool {
	name := strings.TrimPrefix(serviceID, "aws.")
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	return name == requested || strings.ReplaceAll(name, "-", "") == requested
}

func (p *Pack) invokeLambda(ctx context.Context, req *spi.Request, resource string, payload any) (any, error) {
	name := ""
	if params, ok := payload.(map[string]any); ok && strings.Contains(resource, "lambda:invoke") {
		name, _ = params["FunctionName"].(string)
		if pl, ok := params["Payload"]; ok {
			payload = pl
		}
	}
	if name == "" {
		if i := strings.LastIndex(resource, "function:"); i >= 0 {
			name = resource[i+len("function:"):]
		}
	}
	in := map[string]any{"FunctionName": name}
	switch m := payload.(type) {
	case map[string]any:
		for k, v := range m {
			in[k] = v
		}
	default:
		in["input"] = m
	}
	lp := lambda.New(p.deps)
	resp, err := lp.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Invoke", Input: in})
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Output == nil {
		return map[string]any{}, nil
	}
	if raw, ok := resp.Output["Payload"]; ok {
		switch t := raw.(type) {
		case json.RawMessage:
			return parseJSON(string(t)), nil
		case []byte:
			return parseJSON(string(t)), nil
		case string:
			return parseJSON(t), nil
		default:
			return t, nil
		}
	}
	return resp.Output, nil
}

func choiceNext(st map[string]any, data any) string {
	next, _ := selectedChoice(st, data)
	return next
}

func selectedChoice(st map[string]any, data any) (string, map[string]any) {
	choices, _ := st["Choices"].([]any)
	for _, c := range choices {
		cm, _ := c.(map[string]any)
		if matchChoice(cm, data) {
			n, _ := cm["Next"].(string)
			return n, cm
		}
	}
	d, _ := st["Default"].(string)
	return d, nil
}

func jsonataChoiceNext(state map[string]any, input any, context map[string]any, variables map[string]any, random spi.Rand) (string, bool) {
	scope := jsonataScope{input: input, result: input, context: context, variables: variables, random: random}
	var matched map[string]any
	for _, raw := range asSlice(state["Choices"]) {
		choice, _ := raw.(map[string]any)
		condition, ok := evalJSONataValue(choice["Condition"], scope)
		if !ok {
			return "", false
		}
		value, boolean := condition.(bool)
		if !boolean {
			return "", false
		}
		if value {
			matched = choice
			break
		}
	}
	assignments := []map[string]any{state}
	if matched != nil {
		assignments = append(assignments, matched)
	}
	if !applyJSONataAssignments(scope, variables, assignments...) {
		return "", false
	}
	if matched != nil {
		return first(matched, "Next"), true
	}
	return first(state, "Default"), true
}

func matchChoice(cm map[string]any, data any) bool {
	if rules := asSlice(cm["And"]); rules != nil {
		for _, raw := range rules {
			rule, _ := raw.(map[string]any)
			if !matchChoice(rule, data) {
				return false
			}
		}
		return len(rules) > 0
	}
	if rules := asSlice(cm["Or"]); rules != nil {
		for _, raw := range rules {
			rule, _ := raw.(map[string]any)
			if matchChoice(rule, data) {
				return true
			}
		}
		return false
	}
	if raw, ok := cm["Not"].(map[string]any); ok {
		return !matchChoice(raw, data)
	}
	got, present := jsonPathLookup(data, first(cm, "Variable"))
	if expected, ok := cm["IsPresent"]; ok {
		return present == toBool(expected)
	}
	if !present {
		return false
	}
	for key, actual := range map[string]bool{
		"IsNull": got == nil, "IsString": isString(got), "IsNumeric": isNumber(got), "IsBoolean": isBool(got), "IsTimestamp": isTimestamp(got),
	} {
		if expected, ok := cm[key]; ok {
			return actual == toBool(expected)
		}
	}
	if pattern, ok := cm["StringMatches"].(string); ok {
		value, valid := got.(string)
		return valid && wildcardMatch(pattern, value)
	}
	if op, want, ok := choiceComparison(cm, "String", data); ok {
		left, leftOK := got.(string)
		right, rightOK := want.(string)
		return leftOK && rightOK && compareResult(strings.Compare(left, right), op)
	}
	if op, want, ok := choiceComparison(cm, "Numeric", data); ok {
		left, leftOK := choiceNumber(got)
		right, rightOK := choiceNumber(want)
		return leftOK && rightOK && numericComparison(left, right, op)
	}
	if op, want, ok := choiceComparison(cm, "Boolean", data); ok {
		left, leftOK := got.(bool)
		right, rightOK := want.(bool)
		return op == "Equals" && leftOK && rightOK && left == right
	}
	if op, want, ok := choiceComparison(cm, "Timestamp", data); ok {
		left, leftOK := parseTimestamp(got)
		right, rightOK := parseTimestamp(want)
		if !leftOK || !rightOK {
			return false
		}
		cmp := 0
		if left.Before(right) {
			cmp = -1
		} else if left.After(right) {
			cmp = 1
		}
		return compareResult(cmp, op)
	}
	return false
}

func choiceComparison(rule map[string]any, prefix string, data any) (string, any, bool) {
	for _, op := range []string{"Equals", "LessThan", "LessThanEquals", "GreaterThan", "GreaterThanEquals"} {
		if value, exists := rule[prefix+op]; exists {
			return op, value, true
		}
		if path, exists := rule[prefix+op+"Path"].(string); exists {
			value, found := jsonPathLookup(data, path)
			return op, value, found
		}
	}
	return "", nil, false
}

func compareResult(comparison int, op string) bool {
	switch op {
	case "Equals":
		return comparison == 0
	case "LessThan":
		return comparison < 0
	case "LessThanEquals":
		return comparison <= 0
	case "GreaterThan":
		return comparison > 0
	case "GreaterThanEquals":
		return comparison >= 0
	}
	return false
}

func numericComparison(left, right float64, op string) bool {
	switch op {
	case "Equals":
		return left == right
	case "LessThan":
		return left < right
	case "LessThanEquals":
		return left <= right
	case "GreaterThan":
		return left > right
	case "GreaterThanEquals":
		return left >= right
	}
	return false
}

func choiceNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case float32:
		return float64(number), true
	case int:
		return float64(number), true
	case int64:
		return float64(number), true
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func isString(value any) bool { _, ok := value.(string); return ok }
func isNumber(value any) bool { _, ok := choiceNumber(value); return ok }
func isBool(value any) bool   { _, ok := value.(bool); return ok }
func isTimestamp(value any) bool {
	_, ok := parseTimestamp(value)
	return ok
}

func parseTimestamp(value any) (time.Time, bool) {
	raw, ok := value.(string)
	if !ok {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	return parsed, err == nil
}

func wildcardMatch(pattern, value string) bool {
	p, v := []rune(pattern), []rune(value)
	memo := map[[2]int]bool{}
	seen := map[[2]int]bool{}
	var match func(int, int) bool
	match = func(pi, vi int) bool {
		key := [2]int{pi, vi}
		if seen[key] {
			return memo[key]
		}
		seen[key] = true
		var result bool
		switch {
		case pi == len(p):
			result = vi == len(v)
		case p[pi] == '\\' && pi+1 < len(p):
			result = vi < len(v) && p[pi+1] == v[vi] && match(pi+2, vi+1)
		case p[pi] == '*':
			result = match(pi+1, vi) || vi < len(v) && match(pi, vi+1)
		default:
			result = vi < len(v) && p[pi] == v[vi] && match(pi+1, vi+1)
		}
		memo[key] = result
		return result
	}
	return match(0, 0)
}

func jsonPath(data any, path string) any {
	value, _ := jsonPathLookup(data, path)
	return value
}

func jsonPathLookup(data any, path string) (any, bool) {
	if path == "$" || path == "" {
		return data, true
	}
	tokens, ok := jsonPathTokens(strings.TrimPrefix(path, "$"))
	if !ok {
		return nil, false
	}
	nodes := []any{data}
	multiple := false
	for _, token := range tokens {
		var next []any
		for _, node := range nodes {
			switch token.kind {
			case 'f':
				if object, ok := node.(map[string]any); ok {
					if value, exists := object[token.key]; exists {
						next = append(next, value)
					}
				}
			case 'i':
				if array, ok := node.([]any); ok && token.start >= 0 && token.start < len(array) {
					next = append(next, array[token.start])
				}
			case '*':
				if array, ok := node.([]any); ok {
					next = append(next, array...)
				}
			case 's':
				if array, ok := node.([]any); ok {
					start, end := max(0, token.start), min(len(array), token.end)
					if start <= end {
						next = append(next, array[start:end]...)
					}
				}
			}
		}
		if token.kind == '*' || token.kind == 's' {
			multiple = true
		}
		nodes = next
	}
	if multiple {
		return nodes, len(nodes) > 0
	}
	if len(nodes) == 0 {
		return nil, false
	}
	return nodes[0], true
}

type pathToken struct {
	kind       byte
	key        string
	start, end int
}

func jsonPathTokens(path string) ([]pathToken, bool) {
	var tokens []pathToken
	for len(path) > 0 {
		if path[0] == '.' {
			path = path[1:]
			continue
		}
		if path[0] != '[' {
			end := strings.IndexAny(path, ".[")
			if end < 0 {
				end = len(path)
			}
			if end == 0 {
				return nil, false
			}
			tokens = append(tokens, pathToken{kind: 'f', key: path[:end]})
			path = path[end:]
			continue
		}
		close := strings.IndexByte(path, ']')
		if close < 0 {
			return nil, false
		}
		member := path[1:close]
		path = path[close+1:]
		switch {
		case member == "*":
			tokens = append(tokens, pathToken{kind: '*'})
		case strings.Contains(member, ":"):
			bounds := strings.SplitN(member, ":", 2)
			start, startErr := strconv.Atoi(bounds[0])
			end, endErr := strconv.Atoi(bounds[1])
			if startErr != nil || endErr != nil {
				return nil, false
			}
			tokens = append(tokens, pathToken{kind: 's', start: start, end: end})
		case len(member) >= 2 && (member[0] == '\'' && member[len(member)-1] == '\'' || member[0] == '"' && member[len(member)-1] == '"'):
			tokens = append(tokens, pathToken{kind: 'f', key: member[1 : len(member)-1]})
		default:
			index, err := strconv.Atoi(member)
			if err != nil {
				return nil, false
			}
			tokens = append(tokens, pathToken{kind: 'i', start: index})
		}
	}
	return tokens, true
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func aliasRoutes(input map[string]any) []any {
	return inputSlice(input, "routingConfiguration", "RoutingConfiguration")
}

func inputSlice(input map[string]any, keys ...string) []any {
	value, _ := inputValue(input, keys...)
	return asSlice(value)
}

func optionalSlice(input map[string]any, keys ...string) ([]any, bool) {
	value, exists := inputValue(input, keys...)
	if !exists {
		return nil, true
	}
	items, valid := value.([]any)
	return items, valid
}

func inputBool(input map[string]any, keys ...string) bool {
	value, _ := inputValue(input, keys...)
	return toBool(value)
}

func inputValue(input map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		if value, exists := input[key]; exists {
			return value, true
		}
	}
	return nil, false
}

func inputNumber(input map[string]any, keys ...string) float64 {
	value, _ := inputValue(input, keys...)
	return toFloat(value)
}

func getRecord(ctx context.Context, collection spi.Collection, key string) (map[string]any, bool) {
	encoded, ok, _ := collection.Get(ctx, key)
	if !ok {
		return nil, false
	}
	var record map[string]any
	if json.Unmarshal(encoded, &record) != nil {
		return nil, false
	}
	return record, true
}

func listRecords(ctx context.Context, collection spi.Collection, selectRecord func(map[string]any) (map[string]any, bool)) []any {
	kvs, _, _ := collection.List(ctx, "", "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var record map[string]any
		_ = json.Unmarshal(kv.Value, &record)
		if selected, keep := selectRecord(record); keep {
			items = append(items, selected)
		}
	}
	return items
}

func pagedResponse(input map[string]any, items []any, key string) (*spi.Response, error) {
	rawLimit := inputNumber(input, "maxResults", "MaxResults")
	limit := int(rawLimit)
	if limit == 0 {
		limit = 100
	}
	if limit < 0 || limit > 1000 || rawLimit != math.Trunc(rawLimit) {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
	}
	offset := 0
	if token := first(input, "nextToken", "NextToken"); token != "" {
		var err error
		offset, err = strconv.Atoi(token)
		if err != nil || offset < 0 || offset >= len(items) {
			return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
		}
	}
	end := min(offset+limit, len(items))
	output := map[string]any{key: items[offset:end]}
	if end < len(items) {
		output["nextToken"] = strconv.Itoa(end)
	}
	return &spi.Response{Output: output}, nil
}

func executionMatchesMachine(record map[string]any, arn string) bool {
	if versionNumber(arn) > 0 {
		return first(record, "stateMachineVersionArn") == arn
	}
	if smName(arn) != baseSMName(arn) {
		return first(record, "stateMachineAliasArn") == arn
	}
	return first(record, "stateMachineArn") == arn
}

func aliasReferences(record map[string]any, versionARN string) bool {
	for _, raw := range asSlice(record["routingConfiguration"]) {
		route, _ := raw.(map[string]any)
		if first(route, "stateMachineVersionArn", "StateMachineVersionArn") == versionARN {
			return true
		}
	}
	return false
}

func parseJSON(s string) any {
	if s == "" {
		return map[string]any{}
	}
	var v any
	if json.Unmarshal([]byte(s), &v) == nil {
		return v
	}
	return s
}

func validateDefinition(definition string, machineType ...string) []map[string]any {
	var machine map[string]any
	if json.Unmarshal([]byte(definition), &machine) != nil {
		return []map[string]any{{"severity": "ERROR", "code": "INVALID_JSON_DESCRIPTION", "message": "The definition is not valid JSON.", "location": "/"}}
	}
	var diagnostics []map[string]any
	typeName := ""
	if len(machineType) != 0 {
		typeName = machineType[0]
	}
	validateMachine(machine, "", typeName, &diagnostics)
	return diagnostics
}

func validateMachine(machine map[string]any, location, machineType string, diagnostics *[]map[string]any, inheritedVariables ...map[string]struct{}) {
	start, _ := machine["StartAt"].(string)
	states, ok := machine["States"].(map[string]any)
	add := func(code, message, path string) {
		*diagnostics = append(*diagnostics, map[string]any{"severity": "ERROR", "code": code, "message": message, "location": location + path})
	}
	queryLanguage := first(machine, "QueryLanguage")
	if queryLanguage == "" {
		queryLanguage = "JSONPath"
	} else if queryLanguage != "JSONPath" && queryLanguage != "JSONata" {
		add("SCHEMA_VALIDATION_FAILED", "QueryLanguage must be JSONPath or JSONata.", "/QueryLanguage")
	}
	if !ok || len(states) == 0 {
		add("MISSING_REQUIRED_FIELD", "States must contain at least one state.", "/States")
		return
	}
	if _, exists := states[start]; start == "" || !exists {
		add("MISSING_TRANSITION_TARGET", "StartAt must name a state.", "/StartAt")
	}
	outerVariables := map[string]struct{}{}
	if len(inheritedVariables) != 0 {
		outerVariables = inheritedVariables[0]
	}
	scopeVariables := map[string]struct{}{}
	for stateName, raw := range states {
		state, _ := raw.(map[string]any)
		owners := []any{state}
		owners = append(owners, asSlice(state["Choices"])...)
		owners = append(owners, asSlice(state["Catch"])...)
		for _, rawOwner := range owners {
			owner, _ := rawOwner.(map[string]any)
			assign, _ := owner["Assign"].(map[string]any)
			for variable := range assign {
				if !validVariableName(variable) || variable == "states" {
					add("SCHEMA_VALIDATION_FAILED", "Assign contains an invalid variable name.", "/States/"+stateName+"/Assign/"+variable)
				}
				if _, shadows := outerVariables[variable]; shadows {
					add("SCHEMA_VALIDATION_FAILED", "An inner scope cannot assign an outer variable.", "/States/"+stateName+"/Assign/"+variable)
				}
				scopeVariables[variable] = struct{}{}
			}
		}
	}
	visibleVariables := maps.Clone(outerVariables)
	for variable := range scopeVariables {
		visibleVariables[variable] = struct{}{}
	}
	for name, raw := range states {
		state, ok := raw.(map[string]any)
		if !ok {
			add("SCHEMA_VALIDATION_FAILED", "State must be an object.", "/States/"+name)
			continue
		}
		typ, _ := state["Type"].(string)
		stateQueryLanguage := first(state, "QueryLanguage")
		if stateQueryLanguage == "" {
			stateQueryLanguage = queryLanguage
		} else if stateQueryLanguage != "JSONPath" && stateQueryLanguage != "JSONata" || queryLanguage == "JSONata" && stateQueryLanguage != "JSONata" {
			add("SCHEMA_VALIDATION_FAILED", "State QueryLanguage is not compatible with the state machine.", "/States/"+name+"/QueryLanguage")
		}
		isJSONata := stateQueryLanguage == "JSONata"
		jsonataOnly := []string{"Arguments", "Output", "Items"}
		jsonPathOnly := []string{
			"InputPath", "Parameters", "ResultSelector", "ResultPath", "OutputPath", "ItemsPath", "SecondsPath", "TimestampPath", "ErrorPath", "CausePath",
			"MaxConcurrencyPath", "ToleratedFailureCountPath", "ToleratedFailurePercentagePath", "TimeoutSecondsPath", "HeartbeatSecondsPath",
		}
		for _, field := range jsonataOnly {
			if _, exists := state[field]; exists && !isJSONata {
				add("SCHEMA_VALIDATION_FAILED", field+" requires JSONata.", "/States/"+name+"/"+field)
			}
		}
		for _, field := range jsonPathOnly {
			if _, exists := state[field]; exists && isJSONata {
				add("SCHEMA_VALIDATION_FAILED", field+" is not supported with JSONata.", "/States/"+name+"/"+field)
			}
		}
		for field, value := range state {
			if isJSONata || field == "Assign" {
				if !validJSONataExpressions(value) {
					add("SCHEMA_VALIDATION_FAILED", "JSONata expression is invalid or has malformed delimiters.", "/States/"+name+"/"+field)
				}
				if jsonataReferences(value, "$states.result") && !((typ == "Task" || typ == "Map" || typ == "Parallel") && (field == "Assign" || field == "Output")) {
					add("SCHEMA_VALIDATION_FAILED", "$states.result is not available in this field.", "/States/"+name+"/"+field)
				}
				if jsonataReferences(value, "$states.errorOutput") && field != "Catch" {
					add("SCHEMA_VALIDATION_FAILED", "$states.errorOutput is only available in Catch Assign or Output.", "/States/"+name+"/"+field)
				}
			}
		}
		if _, assigned := state["Assign"]; assigned && (typ == "Succeed" || typ == "Fail") {
			add("SCHEMA_VALIDATION_FAILED", "Assign is not supported by terminal states.", "/States/"+name+"/Assign")
		}
		if _, output := state["Output"]; output && typ == "Fail" {
			add("SCHEMA_VALIDATION_FAILED", "Fail does not support Output.", "/States/"+name+"/Output")
		}
		if typ == "Wait" {
			configured := 0
			for _, field := range []string{"Seconds", "Timestamp", "SecondsPath", "TimestampPath"} {
				if _, exists := state[field]; exists {
					configured++
				}
			}
			if configured != 1 {
				add("SCHEMA_VALIDATION_FAILED", "Wait must contain exactly one wait-time field.", "/States/"+name)
			}
		}
		if typ == "Fail" {
			_, errorDirect := state["Error"]
			_, errorPath := state["ErrorPath"]
			if errorDirect && errorPath {
				add("SCHEMA_VALIDATION_FAILED", "Error and ErrorPath are mutually exclusive.", "/States/"+name)
			}
			_, causeDirect := state["Cause"]
			_, causePath := state["CausePath"]
			if causeDirect && causePath {
				add("SCHEMA_VALIDATION_FAILED", "Cause and CausePath are mutually exclusive.", "/States/"+name)
			}
		}
		if !supportedStateType(typ) {
			add("SCHEMA_VALIDATION_FAILED", "Unsupported state Type.", "/States/"+name+"/Type")
		}
		end, _ := state["End"].(bool)
		_, hasNext := state["Next"]
		if typ != "Succeed" && typ != "Fail" && typ != "Choice" && !end && !hasNext {
			add("MISSING_REQUIRED_FIELD", "State must have Next or End.", "/States/"+name)
		}
		if typ == "Task" && first(state, "Resource") == "" {
			add("MISSING_REQUIRED_FIELD", "Task must have Resource.", "/States/"+name+"/Resource")
		}
		if raw, exists := state["Credentials"]; exists {
			credentials, valid := raw.(map[string]any)
			roleARN, static := credentials["RoleArn"].(string)
			_, dynamic := credentials["RoleArn.$"].(string)
			jsonataRole := isJSONata && static && validJSONataExpressions(roleARN)
			if !valid || isJSONata && (!jsonataRole || dynamic) || !isJSONata && (static == dynamic || static && !validRoleARN(roleARN)) {
				add("SCHEMA_VALIDATION_FAILED", "Credentials must contain one valid RoleArn or RoleArn.$.", "/States/"+name+"/Credentials")
			}
		}
		if resource := first(state, "Resource"); typ == "Task" && strings.HasSuffix(resource, ".waitForTaskToken") &&
			!callbackIntegration(strings.TrimSuffix(resource, ".waitForTaskToken")) {
			add("SCHEMA_VALIDATION_FAILED", "Resource does not support the callback integration pattern.", "/States/"+name+"/Resource")
		}
		if resource := first(state, "Resource"); machineType == "EXPRESS" && strings.HasSuffix(resource, ".waitForTaskToken") {
			add("SCHEMA_VALIDATION_FAILED", "Express workflows do not support the callback integration pattern.", "/States/"+name+"/Resource")
		}
		if resource, _, syncJob := syncTaskResource(first(state, "Resource")); typ == "Task" && syncJob && !syncIntegration(resource) {
			add("SCHEMA_VALIDATION_FAILED", "Resource does not support the Run a Job integration pattern.", "/States/"+name+"/Resource")
		}
		if _, _, syncJob := syncTaskResource(first(state, "Resource")); machineType == "EXPRESS" && syncJob {
			add("SCHEMA_VALIDATION_FAILED", "Express workflows do not support the Run a Job integration pattern.", "/States/"+name+"/Resource")
		}
		checkTarget := func(raw any, path string) {
			target, _ := raw.(string)
			if _, exists := states[target]; target == "" || !exists {
				add("MISSING_TRANSITION_TARGET", "Transition target does not exist.", "/States/"+name+path)
			}
		}
		if next, exists := state["Next"]; exists {
			checkTarget(next, "/Next")
		}
		for i, raw := range asSlice(state["Catch"]) {
			catcher, _ := raw.(map[string]any)
			for field, value := range catcher {
				if jsonataReferences(value, "$states.errorOutput") && field != "Assign" && field != "Output" {
					add("SCHEMA_VALIDATION_FAILED", "$states.errorOutput is only available in Catch Assign or Output.", fmt.Sprintf("/States/%s/Catch/%d/%s", name, i, field))
				}
			}
			if _, resultPath := catcher["ResultPath"]; resultPath && isJSONata {
				add("SCHEMA_VALIDATION_FAILED", "Catch ResultPath is not supported with JSONata.", fmt.Sprintf("/States/%s/Catch/%d/ResultPath", name, i))
			}
			if _, output := catcher["Output"]; output && !isJSONata {
				add("SCHEMA_VALIDATION_FAILED", "Catch Output requires JSONata.", fmt.Sprintf("/States/%s/Catch/%d/Output", name, i))
			}
			checkTarget(catcher["Next"], fmt.Sprintf("/Catch/%d/Next", i))
		}
		if typ == "Choice" {
			choices := asSlice(state["Choices"])
			if len(choices) == 0 {
				add("MISSING_REQUIRED_FIELD", "Choice must have Choices.", "/States/"+name+"/Choices")
			}
			for i, raw := range choices {
				choice, _ := raw.(map[string]any)
				_, hasCondition := choice["Condition"]
				if isJSONata != hasCondition {
					add("SCHEMA_VALIDATION_FAILED", "Choice rules must use Condition exactly when JSONata is selected.", fmt.Sprintf("/States/%s/Choices/%d", name, i))
				}
				checkTarget(choice["Next"], fmt.Sprintf("/Choices/%d/Next", i))
			}
			if fallback, exists := state["Default"]; exists {
				checkTarget(fallback, "/Default")
			}
		}
		if typ == "Parallel" {
			branches := asSlice(state["Branches"])
			if len(branches) == 0 {
				add("MISSING_REQUIRED_FIELD", "Parallel must have Branches.", "/States/"+name+"/Branches")
			}
			for i, raw := range branches {
				branch, _ := raw.(map[string]any)
				if branch["QueryLanguage"] == nil {
					branch = maps.Clone(branch)
					branch["QueryLanguage"] = queryLanguage
				}
				validateMachine(branch, fmt.Sprintf("%s/States/%s/Branches/%d", location, name, i), machineType, diagnostics, visibleVariables)
			}
		}
		if typ == "Map" {
			for _, field := range []string{"ItemReader", "ResultWriter"} {
				configuration, _ := state[field].(map[string]any)
				if configuration == nil {
					continue
				}
				if _, parameters := configuration["Parameters"]; parameters && isJSONata {
					add("SCHEMA_VALIDATION_FAILED", field+" Parameters is not supported with JSONata.", "/States/"+name+"/"+field+"/Parameters")
				}
				if _, arguments := configuration["Arguments"]; arguments && !isJSONata {
					add("SCHEMA_VALIDATION_FAILED", field+" Arguments requires JSONata.", "/States/"+name+"/"+field+"/Arguments")
				}
			}
			if reader, _ := state["ItemReader"].(map[string]any); reader != nil {
				config, _ := reader["ReaderConfig"].(map[string]any)
				if _, path := config["MaxItemsPath"]; path && isJSONata {
					add("SCHEMA_VALIDATION_FAILED", "MaxItemsPath is not supported with JSONata.", "/States/"+name+"/ItemReader/ReaderConfig/MaxItemsPath")
				}
			}
			if batcher, _ := state["ItemBatcher"].(map[string]any); batcher != nil && isJSONata {
				for _, field := range []string{"MaxItemsPerBatchPath", "MaxInputBytesPerBatchPath"} {
					if _, exists := batcher[field]; exists {
						add("SCHEMA_VALIDATION_FAILED", field+" is not supported with JSONata.", "/States/"+name+"/ItemBatcher/"+field)
					}
				}
			}
			processor, _ := state["ItemProcessor"].(map[string]any)
			if processor == nil {
				processor, _ = state["Iterator"].(map[string]any)
			}
			if processor != nil {
				if processor["QueryLanguage"] == nil {
					processor = maps.Clone(processor)
					processor["QueryLanguage"] = queryLanguage
				}
				validateMachine(processor, location+"/States/"+name+"/ItemProcessor", machineType, diagnostics, visibleVariables)
			} else {
				add("MISSING_REQUIRED_FIELD", "Map must have ItemProcessor.", "/States/"+name+"/ItemProcessor")
			}
		}
	}
}

func validVariableName(name string) bool {
	if name == "" || utf8.RuneCountInString(name) > 80 {
		return false
	}
	for index, character := range []rune(name) {
		if index == 0 && character != '_' && !unicode.IsLetter(character) || index > 0 && character != '_' && !unicode.IsLetter(character) && !unicode.IsDigit(character) {
			return false
		}
	}
	return true
}

func validJSONataExpressions(value any) bool {
	switch value := value.(type) {
	case string:
		hasOpen, hasClose := strings.Contains(value, "{%"), strings.Contains(value, "%}")
		if !hasOpen && !hasClose {
			return true
		}
		if !strings.HasPrefix(value, "{%") || !strings.HasSuffix(value, "%}") || len(value) < 4 {
			return false
		}
		expression := strings.TrimSpace(value[2 : len(value)-2])
		if strings.Contains(expression, "$eval(") {
			return false
		}
		_, err := jsonata.Compile(expression, false)
		return err == nil
	case map[string]any:
		for _, item := range value {
			if !validJSONataExpressions(item) {
				return false
			}
		}
	case []any:
		for _, item := range value {
			if !validJSONataExpressions(item) {
				return false
			}
		}
	}
	return true
}

func jsonataReferences(value any, variable string) bool {
	switch value := value.(type) {
	case string:
		return strings.HasPrefix(value, "{%") && strings.HasSuffix(value, "%}") && strings.Contains(value, variable)
	case map[string]any:
		for _, item := range value {
			if jsonataReferences(item, variable) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if jsonataReferences(item, variable) {
				return true
			}
		}
	}
	return false
}

func supportedStateType(typ string) bool {
	switch typ {
	case "Pass", "Succeed", "Fail", "Wait", "Task", "Choice", "Parallel", "Map":
		return true
	default:
		return false
	}
}

func testDefinition(definition, stateName string, input any) (string, string, error) {
	var parsed map[string]any
	if json.Unmarshal([]byte(definition), &parsed) != nil {
		return "", "", fmt.Errorf("definition is not valid JSON")
	}
	state := parsed
	if states, ok := parsed["States"].(map[string]any); ok {
		if stateName == "" {
			stateName, _ = parsed["StartAt"].(string)
		}
		state, _ = states[stateName].(map[string]any)
	}
	if !supportedStateType(first(state, "Type")) {
		return "", "", fmt.Errorf("state does not have a supported Type")
	}
	if typ := first(state, "Type"); typ == "Parallel" || typ == "Map" || typ == "Task" && strings.Contains(first(state, "Resource"), ":activity:") {
		return "", "", fmt.Errorf("state requires a mock")
	}
	next := first(state, "Next")
	copy := map[string]any{}
	for key, value := range state {
		copy[key] = value
	}
	if first(copy, "Type") == "Choice" {
		next = choiceNext(copy, input)
		copy["Type"] = "Pass"
		delete(copy, "Choices")
		delete(copy, "Default")
	}
	delete(copy, "Next")
	if typ := first(copy, "Type"); typ != "Succeed" && typ != "Fail" {
		copy["End"] = true
	}
	machine := map[string]any{"StartAt": "TestState", "States": map[string]any{"TestState": copy}}
	encoded, _ := json.Marshal(machine)
	return string(encoded), next, nil
}

func smName(arn string) string {
	return lastSeg(arn, "stateMachine:")
}

func baseSMName(arn string) string {
	name := smName(arn)
	if base, _, found := strings.Cut(name, ":"); found {
		return base
	}
	return name
}

func stateMachineBaseARN(arn string) string {
	return strings.TrimSuffix(arn, smName(arn)) + baseSMName(arn)
}

func versionNumber(arn string) int {
	number, _ := strconv.Atoi(lastSeg(arn, ":"))
	return number
}

func validAliasName(name string) bool {
	if len(name) < 1 || len(name) > 80 {
		return false
	}
	hasNonDigit := false
	for _, char := range name {
		if char < '0' || char > '9' {
			hasNonDigit = true
		}
		if char < '0' || char > '9' {
			if char < 'A' || char > 'Z' {
				if char < 'a' || char > 'z' {
					if char != '-' && char != '_' {
						return false
					}
				}
			}
		}
	}
	return hasNonDigit
}

func validResourceName(name string) bool {
	length := utf8.RuneCountInString(name)
	if length < 1 || length > 80 || !utf8.ValidString(name) {
		return false
	}
	for _, char := range name {
		if char < ' ' || char >= 0x7f && char <= 0x9f || char == 0xfffe || char == 0xffff || strings.ContainsRune(" <>{}[]?*\"#%\\^|~`$&,;:/", char) {
			return false
		}
	}
	return true
}

func validRoleARN(arn string) bool {
	return len(arn) >= 1 && len(arn) <= 256 && strings.HasPrefix(arn, "arn:") && strings.Contains(arn, ":iam::") && strings.Contains(arn, ":role/")
}

func validEncryptionConfiguration(value any) bool {
	configuration, ok := value.(map[string]any)
	if !ok {
		return false
	}
	typeName := first(configuration, "type", "Type")
	keyValue, keySet := inputValue(configuration, "kmsKeyId", "KmsKeyId")
	keyID, keyValid := keyValue.(string)
	period, periodSet := inputValue(configuration, "kmsDataKeyReusePeriodSeconds", "KmsDataKeyReusePeriodSeconds")
	if typeName == "AWS_OWNED_KEY" {
		return !keySet && !periodSet
	}
	if typeName != "CUSTOMER_MANAGED_KMS_KEY" || !keySet || !keyValid || len(keyID) < 1 || len(keyID) > 2048 {
		return false
	}
	if !periodSet {
		return true
	}
	number, ok := exactNumber(period)
	return ok && number == math.Trunc(number) && number >= 60 && number <= 900
}

func validLoggingConfiguration(value any) bool {
	configuration, ok := value.(map[string]any)
	if !ok {
		return false
	}
	level := first(configuration, "level", "Level")
	if level == "" {
		level = "OFF"
	}
	if level != "ALL" && level != "ERROR" && level != "FATAL" && level != "OFF" {
		return false
	}
	if included, exists := inputValue(configuration, "includeExecutionData", "IncludeExecutionData"); exists {
		if _, ok := included.(bool); !ok {
			return false
		}
	}
	destinations, validDestinations := optionalSlice(configuration, "destinations", "Destinations")
	if !validDestinations || len(destinations) > 1 || level != "OFF" && len(destinations) != 1 {
		return false
	}
	for _, raw := range destinations {
		destination, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		groupValue, exists := inputValue(destination, "cloudWatchLogsLogGroup", "CloudWatchLogsLogGroup")
		group, ok := groupValue.(map[string]any)
		arn := first(group, "logGroupArn", "LogGroupArn")
		if !exists || !ok || len(arn) < 1 || len(arn) > 256 {
			return false
		}
	}
	return true
}

func validTracingConfiguration(value any) bool {
	configuration, ok := value.(map[string]any)
	if !ok {
		return false
	}
	enabled, exists := inputValue(configuration, "enabled", "Enabled")
	_, boolean := enabled.(bool)
	return exists && boolean
}

func exactNumber(value any) (float64, bool) {
	switch number := value.(type) {
	case float64:
		return number, true
	case int:
		return float64(number), true
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func asciiString(value string) bool {
	for _, char := range value {
		if char > 0x7f {
			return false
		}
	}
	return true
}

func validatedTags(raw []any) ([]any, string) {
	if len(raw) > 50 {
		return nil, "TooManyTags"
	}
	tags, seen := make([]any, 0, len(raw)), map[string]bool{}
	for _, value := range raw {
		tag, ok := value.(map[string]any)
		if !ok {
			return nil, "ValidationException"
		}
		key, tagValue := first(tag, "key", "Key"), first(tag, "value", "Value")
		if !validTagText(key, 1, 128) || !validTagText(tagValue, 0, 256) || seen[key] || strings.HasPrefix(strings.ToLower(key), "aws:") || strings.HasPrefix(strings.ToLower(tagValue), "aws:") {
			return nil, "ValidationException"
		}
		seen[key] = true
		tags = append(tags, map[string]any{"key": key, "value": tagValue})
	}
	return tags, ""
}

func validTagText(value string, minimum, maximum int) bool {
	length := utf8.RuneCountInString(value)
	if !utf8.ValidString(value) || length < minimum || length > maximum {
		return false
	}
	for _, char := range value {
		if !unicode.IsLetter(char) && !unicode.IsNumber(char) && !unicode.IsSpace(char) && !strings.ContainsRune("_.:/=+-@", char) {
			return false
		}
	}
	return true
}

func (p *Pack) tagResourceExists(ctx context.Context, req *spi.Request, arn string) bool {
	prefix := "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":"
	if name, found := strings.CutPrefix(arn, prefix+"stateMachine:"); found && validResourceName(name) {
		_, exists := getRecord(ctx, p.col(req, "sm"), name)
		return exists
	}
	if name, found := strings.CutPrefix(arn, prefix+"activity:"); found && validResourceName(name) {
		_, exists := getRecord(ctx, p.col(req, "act"), name)
		return exists
	}
	return false
}

func resourceARNFault(arn string) string {
	if len(arn) >= 1 && len(arn) <= 256 && strings.HasPrefix(arn, "arn:") && (strings.Contains(arn, ":stateMachine:") || strings.Contains(arn, ":activity:")) {
		return "ResourceNotFound"
	}
	return "InvalidArn"
}

func activityName(req *spi.Request, arn string) (string, bool) {
	prefix := "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":activity:"
	name, found := strings.CutPrefix(arn, prefix)
	return name, found && len(arn) <= 256 && validResourceName(name)
}

func lastSeg(s, sep string) string {
	if i := strings.LastIndex(s, sep); i >= 0 {
		return s[i+len(sep):]
	}
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		switch v := in[k].(type) {
		case string:
			if v != "" {
				return v
			}
		}
	}
	return ""
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case json.Number:
		f, _ := n.Float64()
		return f
	case int:
		return float64(n)
	default:
		var f float64
		fmt.Sscanf(fmt.Sprint(v), "%f", &f)
		return f
	}
}

func toBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	default:
		return fmt.Sprint(v) == "true"
	}
}
