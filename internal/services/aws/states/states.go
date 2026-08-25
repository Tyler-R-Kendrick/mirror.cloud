// Package states is Step Functions-lite: SM records plus a Pass/Succeed/Fail/Wait/Task/Choice/Parallel walker.
package states

import (
	"bytes"
	"compress/gzip"
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
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	jsonata "github.com/jsonata-go/jsonata/v206"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	internalrand "github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// ponytail: jsonata-go mutates package-global evaluator state; remove this lock when the dependency is concurrency-safe.
var jsonataMu sync.Mutex

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.states", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Step Functions-lite.
type Pack struct {
	deps      spi.Deps
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// New constructs the pack.
func New(d spi.Deps) *Pack {
	p := &Pack{deps: d, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if d.Store == nil || d.Clock == nil {
		close(p.done)
		return p
	}
	go p.waitLoop()
	return p
}

func (p *Pack) derived(key string) *Pack {
	child := &Pack{deps: p.deps, wake: p.wake, stop: p.stop, done: p.done}
	child.deps.Rand = p.deps.Rand.Derive(key)
	return child
}

// Close stops the durable Wait worker.
func (p *Pack) Close() error {
	p.closeOnce.Do(func() { close(p.stop) })
	<-p.done
	return nil
}

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

func (p *Pack) signalWaits() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Pack) waitLoop() {
	defer close(p.done)
	for {
		next := p.runDueWaits(context.Background())
		if next.IsZero() {
			select {
			case <-p.wake:
			case <-p.stop:
				return
			}
			continue
		}
		delay := max(time.Duration(0), next.Sub(p.deps.Clock.Now()))
		select {
		case <-p.deps.Clock.After(delay):
		case <-p.wake:
		case <-p.stop:
			return
		}
	}
}

func (p *Pack) runDueWaits(ctx context.Context) time.Time {
	now := p.deps.Clock.Now()
	var earliest time.Time
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return earliest
	}
	for _, identity := range scopes {
		request := &spi.Request{Identity: spi.Identity{Account: identity.Account, Region: identity.Region}, Input: map[string]any{}}
		pendingCollection := p.col(request, "pending")
		kvs, _, err := pendingCollection.List(ctx, "", "", 0)
		if err != nil {
			continue
		}
		for _, kv := range kvs {
			var wait pending
			if json.Unmarshal(kv.Value, &wait) != nil || wait.WaitUntil == "" && wait.Deadline == "" && wait.TaskDeadline == "" && wait.HeartbeatDeadline == "" {
				continue
			}
			var next time.Time
			kind := ""
			for _, candidate := range []struct{ value, kind string }{
				{wait.Deadline, "execution"}, {wait.TaskDeadline, "task"}, {wait.HeartbeatDeadline, "task"}, {wait.WaitUntil, "wait"},
			} {
				parsed, err := time.Parse(time.RFC3339Nano, candidate.value)
				if err == nil && (next.IsZero() || parsed.Before(next)) {
					next, kind = parsed, candidate.kind
				}
			}
			if next.IsZero() {
				continue
			}
			if next.After(now) {
				if earliest.IsZero() || next.Before(earliest) {
					earliest = next
				}
				continue
			}
			switch kind {
			case "execution":
				p.timeoutExecution(ctx, request, kv.Key, wait)
			case "task":
				p.timeoutTask(ctx, request, kv.Key)
			default:
				p.resumeWait(ctx, request, kv.Key, wait)
			}
		}
	}
	return earliest
}

func (p *Pack) timeoutTask(ctx context.Context, req *spi.Request, token string) {
	timeoutRequest := &spi.Request{Identity: req.Identity, Input: map[string]any{"taskToken": token, "error": "States.Timeout", "cause": "States.Timeout"}}
	_, _ = p.finishTask(ctx, timeoutRequest, p.deps.Clock.Now().Unix(), false)
}

func startTaskTimers(task *pending, now time.Time) {
	task.Started = true
	if task.TimeoutSeconds > 0 {
		task.TaskDeadline = now.Add(time.Duration(task.TimeoutSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
	}
	if task.HeartbeatSeconds > 0 {
		task.HeartbeatDeadline = now.Add(time.Duration(task.HeartbeatSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
	}
}

func (p *Pack) timeoutExecution(ctx context.Context, req *spi.Request, token string, wait pending) {
	encoded, found, _ := p.col(req, "ex").Get(ctx, wait.ExecARN)
	if !found {
		_ = p.col(req, "pending").Delete(ctx, token)
		return
	}
	var record map[string]any
	if json.Unmarshal(encoded, &record) != nil || first(record, "status") != "RUNNING" || first(record, "pendingToken") != token {
		_ = p.col(req, "pending").Delete(ctx, token)
		return
	}
	record["status"], record["error"], record["cause"] = "TIMED_OUT", "States.Timeout", "States.Timeout"
	record["stopDate"] = float64(p.deps.Clock.Now().Unix())
	history := asSlice(record["history"])
	record["history"] = append(history, map[string]any{"type": "ExecutionTimedOut", "id": len(history) + 1, "error": "States.Timeout", "cause": "States.Timeout"})
	delete(record, "pendingToken")
	body, _ := json.Marshal(record)
	_ = p.col(req, "ex").Put(ctx, wait.ExecARN, body)
	_ = p.col(req, "pending").Delete(ctx, token)
}

func (p *Pack) resumeWait(ctx context.Context, req *spi.Request, token string, wait pending) {
	encoded, found, _ := p.col(req, "ex").Get(ctx, wait.ExecARN)
	if !found {
		_ = p.col(req, "pending").Delete(ctx, token)
		return
	}
	var record map[string]any
	if json.Unmarshal(encoded, &record) != nil || first(record, "status") != "RUNNING" || first(record, "pendingToken") != token {
		_ = p.col(req, "pending").Delete(ctx, token)
		return
	}
	walkRequest := *req
	walkRequest.Input = map[string]any{
		"_stateMachineArn": record["stateMachineArn"], "_executionArn": wait.ExecARN, "_executionName": record["name"],
		"_executionInput": parseJSON(first(record, "input")), "_executionStartTime": time.Unix(int64(toFloat(record["startDate"])), 0).UTC().Format(time.RFC3339),
		"_executionRoleArn": record["roleArn"], "_executionType": record["type"], "_executionRedriveCount": record["redriveCount"], "_executionDeadline": wait.Deadline,
	}
	definition, from := wait.Definition, wait.StateName
	var retries map[string]map[int]int
	if wait.Retry {
		var machine map[string]any
		if json.Unmarshal([]byte(definition), &machine) != nil {
			_ = p.col(req, "pending").Delete(ctx, token)
			return
		}
		machine["StartAt"] = wait.StateName
		encoded, _ := json.Marshal(machine)
		definition, from = string(encoded), ""
		retries = map[string]map[int]int{wait.StateName: wait.Retries}
	}
	result := p.walk(ctx, &walkRequest, definition, from, wait.StateInput, retries, wait.Variables)
	now := p.deps.Clock.Now().Unix()
	output, _ := json.Marshal(result.out)
	record["status"], record["output"], record["cause"] = result.status, string(output), result.cause
	record["history"] = append(asSlice(record["history"]), result.hist...)
	delete(record, "error")
	delete(record, "pendingToken")
	if result.errorName != "" {
		record["error"] = result.errorName
	}
	if result.failedState != "" {
		record["redriveState"], record["redriveInput"] = result.failedState, result.failedInput
	}
	if result.status != "RUNNING" {
		record["stopDate"] = float64(now)
	}
	if result.pending != nil {
		result.pending.ExecARN, result.pending.Definition = wait.ExecARN, wait.Definition
		pendingBytes, _ := json.Marshal(result.pending)
		_ = p.col(req, "pending").Put(ctx, result.pending.Token, pendingBytes)
		record["pendingToken"] = result.pending.Token
	}
	for _, run := range result.mapRuns {
		arn := run.arn
		if arn == "" {
			arn = p.mapRunARN(req, first(record, "stateMachineName"), run.label)
		}
		run.record["mapRunArn"], run.record["executionArn"], run.record["stateMachineArn"] = arn, wait.ExecARN, record["stateMachineArn"]
		run.record["startDate"], run.record["stopDate"] = float64(now), float64(now)
		body, _ := json.Marshal(run.record)
		_ = p.col(req, "maprun").Put(ctx, arn, body)
	}
	body, _ := json.Marshal(record)
	_ = p.col(req, "ex").Put(ctx, wait.ExecARN, body)
	_ = p.col(req, "pending").Delete(ctx, token)
	if result.pending != nil {
		p.signalWaits()
	}
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
		walkRequest.Input["_executionType"] = sm["type"]
		walkRequest.Input["_executionRedriveCount"] = 0.0
		walkRequest.Input["_executionDeadline"] = executionDeadline(def, first(sm, "type"), p.deps.Clock.Now())
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
		if wr.pending != nil {
			p.signalWaits()
		}
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
		if !validErrorCause(req.Input) {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := p.col(req, "ex").Get(ctx, ex)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if first(rec, "type") == "EXPRESS" {
			return nil, &spi.Fault{Code: "StateMachineTypeNotSupported", HTTPStatus: 400, Fault: "client"}
		}
		if value, exists := inputValue(req.Input, "error", "Error"); exists {
			rec["error"] = value
		}
		if value, exists := inputValue(req.Input, "cause", "Cause"); exists {
			rec["cause"] = value
		}
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
		if first(rec, "type") == "EXPRESS" && first(rec, "mapRunArn") == "" {
			return nil, &spi.Fault{Code: "StateMachineTypeNotSupported", HTTPStatus: 400, Fault: "client"}
		}
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
			if pend.Started || want != "" && pend.ActivityARN != want {
				continue
			}
			startTaskTimers(&pend, p.deps.Clock.Now())
			encoded, _ := json.Marshal(pend)
			_ = p.col(req, "pending").Put(ctx, pend.Token, encoded)
			p.signalWaits()
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
		body, found, _ := p.col(req, "pending").Get(ctx, token)
		if !found {
			return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
		}
		var pend pending
		_ = json.Unmarshal(body, &pend)
		if !pend.Started {
			return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
		}
		if pend.HeartbeatSeconds > 0 {
			pend.HeartbeatDeadline = p.deps.Clock.Now().Add(time.Duration(pend.HeartbeatSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
			body, _ = json.Marshal(pend)
			_ = p.col(req, "pending").Put(ctx, token, body)
			p.signalWaits()
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
	Token, ActivityARN, StateName, ExecARN, Definition   string
	WaitUntil, Deadline, TaskDeadline, HeartbeatDeadline string
	Input, StateInput                                    any
	Retries                                              map[int]int
	Variables                                            map[string]any
	TimeoutSeconds, HeartbeatSeconds                     int
	Callback, Retry, Started                             bool
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
	walkRequest.Input["_executionType"] = record["type"]
	walkRequest.Input["_executionRedriveCount"] = toFloat(record["redriveCount"]) + 1
	walkRequest.Input["_executionRedriveTime"] = p.deps.Clock.Now().UTC().Format(time.RFC3339Nano)
	walkRequest.Input["_executionDeadline"] = executionDeadline(string(redriveDefinition), first(record, "type"), p.deps.Clock.Now())
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
	if result.pending != nil {
		p.signalWaits()
	}
	return &spi.Response{Output: map[string]any{"redriveDate": float64(now)}}, nil
}

func validErrorCause(input map[string]any) bool {
	for _, field := range []struct {
		lower, upper string
		maximum      int
	}{{"error", "Error", 256}, {"cause", "Cause", 32768}} {
		if value, exists := inputValue(input, field.lower, field.upper); exists {
			text, valid := value.(string)
			if !valid || len(text) > field.maximum {
				return false
			}
		}
	}
	return true
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
	} else if !validErrorCause(req.Input) {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
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
	walkRequest.Input["_executionType"] = rec["type"]
	walkRequest.Input["_executionRedriveCount"] = rec["redriveCount"]
	walkRequest.Input["_executionDeadline"] = pend.Deadline
	persistRetry := func(delay time.Duration) bool {
		scheduled := p.scheduleRetry(ctx, &walkRequest, pend.StateName, pend.StateInput, pend.Retries, pend.Variables, delay, nil)
		if scheduled == nil {
			return false
		}
		scheduled.pending.ExecARN, scheduled.pending.Definition = pend.ExecARN, pend.Definition
		pb, _ := json.Marshal(scheduled.pending)
		_ = p.col(req, "pending").Put(ctx, scheduled.pending.Token, pb)
		rec["pendingToken"] = scheduled.pending.Token
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ex").Put(ctx, pend.ExecARN, nb)
		p.signalWaits()
		return true
	}
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
		if delay, retry := retryTask(st, failure.name, pend.Retries, p.deps.Rand); retry {
			if persistRetry(delay) {
				return &spi.Response{Output: map[string]any{}}, nil
			}
			if !pend.Callback {
				pend.Token = p.deps.Rand.Hex(16)
				pend.Started, pend.TaskDeadline, pend.HeartbeatDeadline = false, "", ""
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
					failure = stateFailure{name: "States.Runtime", cause: "States.Runtime"}
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
			if delay, retry := retryTask(st, failure.name, pend.Retries, p.deps.Rand); retry {
				if persistRetry(delay) {
					return &spi.Response{Output: map[string]any{}}, nil
				}
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
		stateContext := jsonataContext(&walkRequest, pend.StateName, nil)
		out, valid := applyStateResult(st, pend.StateInput, result, stateContext, p.deps.Rand, pend.Variables)
		failure := "States.Runtime"
		if valid {
			valid = applyJSONPathAssignments(result, stateContext, pend.Variables, pend.Variables, p.deps.Rand, st)
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
		if output, valid := applyDataPath(st, "OutputPath", data, pend.Variables); valid {
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
	if wr.pending != nil {
		p.signalWaits()
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) walk(ctx context.Context, req *spi.Request, def, from string, input any, retries map[string]map[int]int, inheritedVariables ...map[string]any) (result walkResult) {
	if !validStatePayload(input) {
		return walkResult{out: input, status: "FAILED", cause: "States.DataLimitExceeded", errorName: "States.DataLimitExceeded"}
	}
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
	maxTransitions := 25000
	if first(req.Input, "_executionType") == "EXPRESS" {
		maxTransitions = math.MaxInt
	}
walkLoop:
	for hop := 0; hop < maxTransitions; hop++ {
		if deadline, err := time.Parse(time.RFC3339Nano, first(req.Input, "_executionDeadline")); err == nil && !p.deps.Clock.Now().Before(deadline) {
			return walkResult{out: data, status: "TIMED_OUT", cause: "States.Timeout", errorName: "States.Timeout", hist: hist}
		}
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
			data, ok = applyDataPath(st, "InputPath", data, variables)
			if !ok {
				return walkResult{out: rawInput, status: "FAILED", cause: "States.Runtime", hist: hist}
			}
		}
		typ, _ := st["Type"].(string)
		hist = append(hist, map[string]any{"type": typ + "StateEntered", "id": hop + 1, "name": cur})
		jsonataOutputApplied := false
		jsonPathAssignmentInput := data
		var waitUntil time.Time
		switch typ {
		case "Pass":
			if isJSONata {
				data = rawInput
			} else {
				result := data
				if params, ok := st["Parameters"].(map[string]any); ok {
					var valid bool
					result, valid = applyParamsValidated(params, data, stateContext, p.deps.Rand, variables)
					if !valid {
						return walkResult{out: rawInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
					}
				} else if value, ok := st["Result"]; ok {
					result = value
				}
				var valid bool
				data, valid = applyResultPath(st, rawInput, result)
				jsonPathAssignmentInput = result
				if !valid {
					return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
				}
			}
		case "Succeed":
			if isJSONata {
				data, ok = applyJSONataState(st, rawInput, rawInput, stateContext, variables, p.deps.Rand)
			} else {
				data, ok = applyDataPath(st, "OutputPath", data, variables)
			}
			if !ok {
				return walkResult{out: rawInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
			}
			if !validStatePayload(data) {
				return walkResult{out: rawInput, status: "FAILED", cause: "States.DataLimitExceeded", errorName: "States.DataLimitExceeded", hist: hist}
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
						value, _ = evalIntrinsic(path, rawInput, stateContext, p.deps.Rand, variables)
					} else {
						value = jsonPath(rawInput, path, variables)
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
			now := p.deps.Clock.Now()
			if isJSONata {
				if value, exists := st["Seconds"]; exists {
					resolved, valid := evalJSONataValue(value, jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand})
					seconds, numeric := exactNumber(resolved)
					if !valid || !numeric || seconds != math.Trunc(seconds) || seconds < 0 || seconds > 99999999 {
						return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
					waitUntil = now.Add(time.Duration(seconds) * time.Second)
				} else if value, exists := st["Timestamp"]; exists {
					resolved, valid := evalJSONataValue(value, jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand})
					timestamp, stringValue := resolved.(string)
					parsed, parseErr := time.Parse(time.RFC3339, timestamp)
					if !valid || !stringValue || parseErr != nil || !strings.Contains(timestamp, "T") || !strings.HasSuffix(timestamp, "Z") {
						return walkResult{out: data, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
					waitUntil = parsed
				}
			} else if value, exists := st["Seconds"]; exists {
				seconds, numeric := exactNumber(value)
				if !numeric || seconds != math.Trunc(seconds) || seconds < 0 || seconds > 99999999 {
					return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
				}
				waitUntil = now.Add(time.Duration(seconds) * time.Second)
			} else if value, exists := st["Timestamp"]; exists {
				timestamp, stringValue := value.(string)
				parsed, parseErr := time.Parse(time.RFC3339, timestamp)
				if !stringValue || parseErr != nil || !strings.Contains(timestamp, "T") || !strings.HasSuffix(timestamp, "Z") {
					return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
				}
				waitUntil = parsed
			} else if path := first(st, "SecondsPath"); path != "" {
				resolved, found := jsonPathLookup(data, path, variables)
				seconds, numeric := exactNumber(resolved)
				if !found || !numeric || seconds != math.Trunc(seconds) || seconds < 0 || seconds > 99999999 {
					return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
				}
				waitUntil = now.Add(time.Duration(seconds) * time.Second)
			} else if path := first(st, "TimestampPath"); path != "" {
				resolved, found := jsonPathLookup(data, path, variables)
				timestamp, stringValue := resolved.(string)
				parsed, parseErr := time.Parse(time.RFC3339, timestamp)
				if !found || !stringValue || parseErr != nil || !strings.Contains(timestamp, "T") || !strings.HasSuffix(timestamp, "Z") {
					return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
				}
				waitUntil = parsed
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
				payload, payloadOK := taskPayload(st, data, mergeJSONataContext(stateContext, taskContext), p.deps.Rand, variables)
				failure := stateFailure{}
				if !payloadOK {
					failure = stateFailure{name: "States.Runtime", cause: "States.Runtime"}
				}
				taskReq := req
				if _, exists := st["Credentials"]; exists {
					var scope *jsonataScope
					if isJSONata {
						scope = &jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand}
					}
					identity, valid := taskIdentity(st, data, stateContext, variables, req.Identity, p.deps.Rand, scope)
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
				var numericScope *jsonataScope
				if isJSONata {
					numericScope = &jsonataScope{input: rawInput, context: stateContext, variables: variables, random: p.deps.Rand}
				}
				timeout, hasTimeout, timeoutValid := stateInteger(st, "TimeoutSeconds", rawInput, numericScope, 1, 99999999, variables)
				heartbeat, hasHeartbeat, heartbeatValid := stateInteger(st, "HeartbeatSeconds", rawInput, numericScope, 1, 99999999, variables)
				maximumHeartbeat := 99999999.0
				if hasTimeout {
					maximumHeartbeat = timeout
				}
				if !timeoutValid || !heartbeatValid || hasHeartbeat && heartbeat >= maximumHeartbeat {
					name := "States.Runtime"
					if isJSONata {
						name = "States.QueryEvaluationError"
					}
					failure = stateFailure{name: name, cause: name}
				}
				if failure.name == "" && !validStatePayload(payload) {
					return walkResult{out: rawInput, status: "FAILED", cause: "States.DataLimitExceeded", errorName: "States.DataLimitExceeded", hist: hist}
				}
				if failure.name == "" && strings.Contains(res, ":activity:") {
					tok := p.deps.Rand.Hex(16)
					return walkResult{out: data, status: "RUNNING", hist: hist, pending: &pending{
						Token: tok, ActivityARN: res, StateName: cur, Input: payload, StateInput: rawInput, Retries: retries[cur], Variables: variables, Deadline: first(req.Input, "_executionDeadline"),
						TimeoutSeconds: int(timeout), HeartbeatSeconds: int(heartbeat),
					}}
				}
				if failure.name == "" {
					out, err, errorPrefix, sdk, supported := p.invokeTask(ctx, taskReq, res, payload)
					if !supported {
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
					if err != nil {
						failure = taskFailure(errorPrefix, sdk, err)
					} else if !validStatePayload(out) {
						return walkResult{out: rawInput, status: "FAILED", cause: "States.DataLimitExceeded", errorName: "States.DataLimitExceeded", hist: hist}
					} else if callback {
						pendingTask := &pending{
							Token: token, StateName: cur, Input: payload, StateInput: rawInput, Retries: retries[cur], Variables: variables, Callback: true, Deadline: first(req.Input, "_executionDeadline"),
							TimeoutSeconds: int(timeout), HeartbeatSeconds: int(heartbeat),
						}
						startTaskTimers(pendingTask, p.deps.Clock.Now())
						return walkResult{out: data, status: "RUNNING", hist: hist, pending: pendingTask}
					} else if isJSONata {
						data, ok = applyJSONataState(st, rawInput, out, stateContext, variables, p.deps.Rand)
						if ok {
							jsonataOutputApplied = true
							break
						}
						failure = stateFailure{name: "States.QueryEvaluationError", cause: "States.QueryEvaluationError"}
					} else {
						data, ok = applyStateResult(st, rawInput, out, stateContext, p.deps.Rand, variables)
						if !ok {
							return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
						}
						if applyJSONPathAssignments(out, stateContext, variables, variables, p.deps.Rand, st) {
							break
						}
						failure = stateFailure{name: "States.Runtime", cause: "States.Runtime"}
					}
				}
				if delay, retry := retryTask(st, failure.name, retries[cur], p.deps.Rand); retry {
					if scheduled := p.scheduleRetry(ctx, req, cur, rawInput, retries[cur], variables, delay, hist); scheduled != nil {
						return *scheduled
					}
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
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
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
					data, ok = applyDataPath(st, "OutputPath", out, variables)
					if !ok {
						return walkResult{out: rawInput, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
				}
				cur = next
				continue walkLoop
			}
		case "Choice":
			next, matchedChoice := selectedChoice(st, data, variables)
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
				if !applyJSONPathAssignments(data, stateContext, variables, variables, p.deps.Rand, owners...) {
					return walkResult{out: rawInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
				}
				data, ok = applyDataPath(st, "OutputPath", data, variables)
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
				var valid bool
				branchInput, valid = applyParamsValidated(params, data, stateContext, p.deps.Rand, variables)
				if !valid {
					return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
				}
			}
			if retries[cur] == nil {
				retries[cur] = map[int]int{}
			}
			for {
				setJSONataRetryCount(stateContext, retries[cur])
				branches, _ := st["Branches"].([]any)
				branchDefs := make([]string, len(branches))
				for index, br := range branches {
					bm, _ := br.(map[string]any)
					if bm["QueryLanguage"] == nil {
						bm = maps.Clone(bm)
						bm["QueryLanguage"] = machineQueryLanguage
					}
					bdef, _ := json.Marshal(bm)
					branchDefs[index] = string(bdef)
				}
				branchWalks := make([]walkResult, len(branches))
				branchCtx, cancelBranches := context.WithCancel(ctx)
				var branchWG sync.WaitGroup
				for index := range branchDefs {
					branchWG.Add(1)
					go func() {
						defer branchWG.Done()
						child := p.derived(fmt.Sprintf("%s/parallel/%s/%d", first(req.Input, "_executionArn"), cur, index))
						branchWalks[index] = child.walk(branchCtx, req, branchDefs[index], "", cloneJSON(branchInput), nil, maps.Clone(variables))
						if branchWalks[index].status != "SUCCEEDED" {
							cancelBranches()
						}
					}()
				}
				branchWG.Wait()
				cancelBranches()
				results := make([]any, 0, len(branches))
				var failed *walkResult
				for index := range branchWalks {
					wr := branchWalks[index]
					mapRuns = append(mapRuns, wr.mapRuns...)
					if wr.status != "SUCCEEDED" {
						if failed == nil {
							failed = &wr
						}
						continue
					}
					results = append(results, wr.out)
				}
				if failed == nil {
					if !validStatePayload(results) {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.DataLimitExceeded", errorName: "States.DataLimitExceeded", hist: hist}
					}
					if isJSONata {
						data, ok = applyJSONataState(st, stateInput, results, stateContext, variables, p.deps.Rand)
						if ok {
							jsonataOutputApplied = true
							break
						}
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError"}
					} else if data, ok = applyStateResult(st, stateInput, results, stateContext, p.deps.Rand, variables); !ok {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", hist: hist}
					} else if applyJSONPathAssignments(results, stateContext, variables, variables, p.deps.Rand, st) {
						break
					} else {
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime"}
					}
				}
				var next string
				var out any
				var delay time.Duration
				var retry, caught, valid bool
				if isJSONata {
					next, out, delay, retry, caught, valid = recoverJSONata(st, *failed, stateInput, stateContext, variables, p.deps.Rand, retries[cur])
					if !valid {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				} else {
					next, out, delay, retry, caught = recoverState(st, *failed, stateInput, retries[cur], p.deps.Rand)
				}
				if retry {
					if scheduled := p.scheduleRetry(ctx, req, cur, stateInput, retries[cur], variables, delay, hist); scheduled != nil {
						return *scheduled
					}
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
						return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
					}
					data = out
					if !isJSONata {
						data, ok = applyDataPath(st, "OutputPath", out, variables)
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
			iter, _ := st["Iterator"].(map[string]any)
			if iter == nil {
				iter, _ = st["ItemProcessor"].(map[string]any)
			}
			if iter == nil {
				return walkResult{out: data, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
			}
			if iter["QueryLanguage"] == nil {
				iter = maps.Clone(iter)
				iter["QueryLanguage"] = machineQueryLanguage
			}
			idef, _ := json.Marshal(iter)
			processorConfig, _ := iter["ProcessorConfig"].(map[string]any)
			distributed := first(processorConfig, "Mode") == "DISTRIBUTED"
			mapVariables := variables
			if distributed {
				mapVariables = map[string]any{}
			}
			if distributed && mapScope != nil {
				mapScope.variables = mapVariables
			}
			executionType := first(processorConfig, "ExecutionType")
			if executionType == "" {
				executionType = "STANDARD"
			}
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
				dataset, source, itemsOK := p.mapItems(ctx, req, st, data, mapScope, mapVariables)
				if _, hasReader := st["ItemReader"]; !isJSONata && hasReader {
					if path := first(st, "ItemsPath"); path != "" {
						dataset = jsonPath(dataset, path, mapVariables)
					}
				}
				var failed *walkResult
				if isJSONata {
					items := any(data)
					_, hasReader := st["ItemReader"]
					if hasReader {
						items = dataset
					} else {
						source = "STATE_DATA"
					}
					if configured, exists := st["Items"]; exists {
						items, itemsOK = evalJSONataValue(configured, *mapScope)
					}
					dataset = items
				}
				arr := []any{}
				var itemKeys []any
				var itemSources []string
				switch items := dataset.(type) {
				case []any:
					arr = items
				case map[string]any:
					itemsOK = distributed
					if distributed {
						keys := make([]string, 0, len(items))
						for key := range items {
							keys = append(keys, key)
						}
						sort.Strings(keys)
						for _, key := range keys {
							arr = append(arr, items[key])
							itemKeys = append(itemKeys, key)
						}
					}
				case mapDataset:
					arr, itemKeys, itemSources = items.values, items.keys, items.sources
				default:
					itemsOK = false
				}
				if !itemsOK {
					name := "States.Runtime"
					if strings.HasPrefix(source, "States.") {
						name = source
					} else if _, hasReader := st["ItemReader"]; hasReader {
						name = "States.ItemReaderFailed"
					} else if isJSONata {
						name = "States.QueryEvaluationError"
					}
					failed = &walkResult{out: stateInput, status: "FAILED", cause: name, errorName: name}
				}
				if failed == nil {
					reader, hasReader := st["ItemReader"].(map[string]any)
					readerConfig, _ := reader["ReaderConfig"].(map[string]any)
					inputType := first(readerConfig, "InputType")
					textReader := hasReader && (first(reader, "Resource") == "arn:aws:states:::s3:getObject" || first(readerConfig, "Transformation") == "LOAD_AND_FLATTEN") && (inputType == "" || slices.Contains([]string{"CSV", "JSON", "JSONL"}, inputType))
					for index, item := range arr {
						if textReader {
							encoded, err := json.Marshal(item)
							if err != nil || len(encoded) > 8*1024*1024 {
								failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.ItemReaderFailed", errorName: "States.ItemReaderFailed"}
								break
							}
						}
						itemSource := source
						if index < len(itemSources) {
							itemSource = itemSources[index]
						}
						itemDetails := map[string]any{"Index": float64(index), "Value": item, "Source": itemSource}
						if index < len(itemKeys) {
							if key, keyed := itemKeys[index].(string); keyed {
								itemDetails["Key"] = key
							}
						}
						itemContext := map[string]any{"Map": map[string]any{"Item": itemDetails}}
						if selected, valid := selector.(map[string]any); valid && !isJSONata {
							arr[index], itemsOK = applyParamsValidated(selected, data, mergeJSONataContext(stateContext, itemContext), p.deps.Rand, mapVariables)
							if !itemsOK {
								failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime"}
								break
							}
						} else if selector != nil {
							arr[index], itemsOK = evalJSONataValue(selector, jsonataScope{
								input: rawInput, context: mergeJSONataContext(stateContext, itemContext), variables: mapVariables, random: p.deps.Rand,
							})
							if !itemsOK {
								failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError"}
								break
							}
						}
					}
				}
				if failed == nil {
					var batchOK bool
					arr, batchOK = batchMapItems(st, data, arr, p.deps.Rand, mapScope, mapVariables)
					if !batchOK {
						name := "States.Runtime"
						if isJSONata {
							name = "States.QueryEvaluationError"
						}
						failed = &walkResult{out: stateInput, status: "FAILED", cause: name, errorName: name}
					}
				}
				maxConcurrency, _, concurrencyOK := stateInteger(st, "MaxConcurrency", rawInput, mapScope, 0, math.MaxInt32, mapVariables)
				failureCount, hasFailureCount, failureCountOK := stateInteger(st, "ToleratedFailureCount", rawInput, mapScope, 0, math.MaxInt32, mapVariables)
				failurePercentage, hasFailurePercentage, failurePercentageOK := stateInteger(st, "ToleratedFailurePercentage", rawInput, mapScope, 0, 100, mapVariables)
				if failed == nil && (!concurrencyOK || !failureCountOK || !failurePercentageOK) {
					name := "States.Runtime"
					if isJSONata {
						name = "States.QueryEvaluationError"
					}
					failed = &walkResult{out: stateInput, status: "FAILED", cause: name, errorName: name}
				}
				var results []any
				var itemResults []mapItemResult
				failedCount := 0
				label, mapRunARN := first(st, "Label"), ""
				if label == "" {
					label = cur
				}
				if distributed {
					mapRunARN = p.mapRunARN(req, mapStateMachineName(req), label)
				}
				if failed == nil {
					type iterationRun struct {
						processed bool
						walk      walkResult
						item      mapItemResult
					}
					runs := make([]iterationRun, len(arr))
					limit, ceiling := int(maxConcurrency), 40
					if distributed {
						ceiling = 10000
					}
					if limit == 0 || limit > ceiling {
						limit = ceiling
					}
					limit = min(limit, len(arr))
					if limit > 0 {
						iterationCtx, cancelIterations := context.WithCancel(ctx)
						jobs := make(chan int)
						var iterationWG sync.WaitGroup
						for range limit {
							iterationWG.Add(1)
							go func() {
								defer iterationWG.Done()
								for index := range jobs {
									if !distributed && iterationCtx.Err() != nil {
										return
									}
									iterationInput := cloneJSON(arr[index])
									child := p.derived(fmt.Sprintf("%s/map/%s/%d", first(req.Input, "_executionArn"), cur, index))
									walkRequest := req
									childName, childStartTime := "", ""
									if distributed {
										childName = child.deps.Rand.UUID()
										childStartTime = p.deps.Clock.Now().UTC().Format(time.RFC3339Nano)
										machine := mapStateMachineName(req) + "/" + cur
										copy := *req
										copy.Input = maps.Clone(req.Input)
										copy.Input["_stateMachineArn"] = p.smARN(req, machine)
										copy.Input["_executionArn"] = "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":execution:" + machine + ":" + childName
										copy.Input["_executionName"], copy.Input["_executionInput"] = childName, iterationInput
										copy.Input["_executionStartTime"] = childStartTime
										copy.Input["_executionRoleArn"] = req.Input["_executionRoleArn"]
										copy.Input["_executionType"] = executionType
										copy.Input["_executionRedriveCount"] = 0.0
										copy.Input["_executionDeadline"] = executionDeadline(string(idef), executionType, p.deps.Clock.Now())
										walkRequest = &copy
									}
									childVariables := maps.Clone(variables)
									if distributed {
										childVariables = map[string]any{}
									}
									wr := child.walk(iterationCtx, walkRequest, string(idef), "", iterationInput, nil, childVariables)
									run := iterationRun{processed: true, walk: wr}
									if distributed {
										run.item = p.mapItemResult(req, cur, childName, childStartTime, iterationInput, wr)
										p.storeMapItemExecution(iterationCtx, req, mapRunARN, string(idef), executionType, run.item)
									}
									runs[index] = run
									if wr.status != "SUCCEEDED" && !distributed {
										cancelIterations()
									}
								}
							}()
						}
					sendItems:
						for index := range arr {
							if !distributed && iterationCtx.Err() != nil {
								break
							}
							select {
							case jobs <- index:
							case <-iterationCtx.Done():
								break sendItems
							}
						}
						close(jobs)
						iterationWG.Wait()
						cancelIterations()
					}
					for index := range runs {
						run := runs[index]
						if !run.processed {
							continue
						}
						mapRuns = append(mapRuns, run.walk.mapRuns...)
						if distributed && hasResultWriter {
							itemResults = append(itemResults, run.item)
						}
						if run.walk.status != "SUCCEEDED" {
							failedCount++
							if failed == nil {
								failure := run.walk
								failed = &failure
							}
							continue
						}
						results = append(results, run.walk.out)
					}
				}
				mapOutput := any(results)
				if distributed && (failed == nil || failedCount > 0) {
					allowed := failedCount == 0
					if failedCount > 0 && (hasFailureCount || hasFailurePercentage) {
						allowed = (!hasFailureCount || float64(failedCount) <= failureCount) &&
							(!hasFailurePercentage || float64(failedCount)*100/float64(len(arr)) <= failurePercentage)
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
						var writerOK, evaluationError bool
						mapOutput, written, writerOK, evaluationError = p.writeMapResults(ctx, req, st, data, itemResults, mapRunARN, mapScope, mapVariables)
						counts["resultsWritten"] = float64(written)
						if !writerOK {
							status = "FAILED"
							name := "States.ResultWriterFailed"
							if evaluationError {
								name = "States.QueryEvaluationError"
							}
							failed = &walkResult{out: data, status: "FAILED", cause: name, errorName: name}
						}
					}
					mapRuns = append(mapRuns, mapRunDraft{arn: mapRunARN, label: label, record: map[string]any{
						"status": status, "executionCounts": counts, "itemCounts": counts, "redriveCount": 0.0,
						"maxConcurrency": maxConcurrency, "toleratedFailureCount": failureCount, "toleratedFailurePercentage": failurePercentage,
					}})
				}
				if failed == nil {
					if mapOutput == nil {
						mapOutput = []any{}
					}
					if !validStatePayload(mapOutput) {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.DataLimitExceeded", errorName: "States.DataLimitExceeded", hist: hist}
					}
					if isJSONata {
						data, ok = applyJSONataState(st, stateInput, mapOutput, stateContext, variables, p.deps.Rand, mapScope.variables)
						if ok {
							jsonataOutputApplied = true
							break
						}
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError"}
					} else if data, ok = applyStateResult(st, stateInput, mapOutput, stateContext, p.deps.Rand, mapVariables); !ok {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", hist: hist}
					} else if applyJSONPathAssignments(mapOutput, stateContext, mapVariables, variables, p.deps.Rand, st) {
						break
					} else {
						failed = &walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime"}
					}
				}
				var next string
				var out any
				var delay time.Duration
				var retry, caught, valid bool
				if isJSONata {
					next, out, delay, retry, caught, valid = recoverJSONata(st, *failed, stateInput, stateContext, variables, p.deps.Rand, retries[cur])
					if !valid {
						return walkResult{out: stateInput, status: "FAILED", cause: "States.QueryEvaluationError", errorName: "States.QueryEvaluationError", hist: hist}
					}
				} else {
					next, out, delay, retry, caught = recoverState(st, *failed, stateInput, retries[cur], p.deps.Rand)
				}
				if retry {
					if scheduled := p.scheduleRetry(ctx, req, cur, stateInput, retries[cur], variables, delay, hist); scheduled != nil {
						return *scheduled
					}
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
						return walkResult{out: stateInput, status: "FAILED", cause: "States.Runtime", errorName: "States.Runtime", hist: hist}
					}
					data = out
					if !isJSONata {
						data, ok = applyDataPath(st, "OutputPath", out, variables)
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
			data, ok = applyDataPath(st, "OutputPath", data, variables)
			if ok && (typ == "Pass" || typ == "Wait") {
				ok = applyJSONPathAssignments(jsonPathAssignmentInput, stateContext, variables, variables, p.deps.Rand, st)
			}
		}
		if !ok {
			failure := "States.Runtime"
			if isJSONata {
				failure = "States.QueryEvaluationError"
			}
			return walkResult{out: rawInput, status: "FAILED", cause: failure, errorName: failure, hist: hist}
		}
		if !validStatePayload(data) {
			return walkResult{out: rawInput, status: "FAILED", cause: "States.DataLimitExceeded", errorName: "States.DataLimitExceeded", hist: hist}
		}
		if waitUntil.After(p.deps.Clock.Now()) {
			if first(req.Input, "_executionType") == "EXPRESS" {
				delay, timeout := p.executionDelay(req, waitUntil.Sub(p.deps.Clock.Now()))
				select {
				case <-p.deps.Clock.After(delay):
					if timeout {
						return walkResult{out: data, status: "TIMED_OUT", cause: "States.Timeout", errorName: "States.Timeout", hist: hist}
					}
				case <-ctx.Done():
					return walkResult{out: data, status: "FAILED", cause: "States.Timeout", errorName: "States.Timeout", hist: hist}
				}
			} else {
				return walkResult{out: data, status: "RUNNING", hist: hist, pending: &pending{
					Token: p.deps.Rand.Hex(16), StateName: cur, StateInput: data, Variables: variables, WaitUntil: waitUntil.UTC().Format(time.RFC3339Nano), Deadline: first(req.Input, "_executionDeadline"),
				}}
			}
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

func retryTask(st map[string]any, name string, attempts map[int]int, random spi.Rand) (time.Duration, bool) {
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
			return 0, false
		}
		interval, backoff := 1.0, 2.0
		if raw, ok := retrier["IntervalSeconds"]; ok {
			interval = toFloat(raw)
		}
		if raw, ok := retrier["BackoffRate"]; ok {
			backoff = toFloat(raw)
		}
		delay := interval * math.Pow(backoff, float64(attempts[i]))
		if raw, ok := retrier["MaxDelaySeconds"]; ok {
			delay = min(delay, toFloat(raw))
		}
		delay = min(delay, float64(math.MaxInt64)/float64(time.Second))
		attempts[i]++
		duration := time.Duration(delay * float64(time.Second))
		if first(retrier, "JitterStrategy") == "FULL" && duration > 0 {
			// ponytail: millisecond jitter is deterministic and sufficient for workflow scheduling; use finer entropy if sub-ms ASL retry fields are introduced.
			duration = time.Duration(random.Intn(int(duration/time.Millisecond)+1)) * time.Millisecond
		}
		return duration, true
	}
	return 0, false
}

func executionDeadline(definition, executionType string, start time.Time) string {
	limit := 365 * 24 * time.Hour
	if executionType == "EXPRESS" {
		limit = 5 * time.Minute
	}
	var machine map[string]any
	if json.Unmarshal([]byte(definition), &machine) == nil {
		if seconds, ok := exactNumber(machine["TimeoutSeconds"]); ok {
			limit = min(limit, time.Duration(seconds*float64(time.Second)))
		}
	}
	return start.Add(limit).UTC().Format(time.RFC3339Nano)
}

func (p *Pack) executionDelay(req *spi.Request, delay time.Duration) (time.Duration, bool) {
	deadline, err := time.Parse(time.RFC3339Nano, first(req.Input, "_executionDeadline"))
	if err != nil {
		return delay, false
	}
	remaining := deadline.Sub(p.deps.Clock.Now())
	if remaining <= delay {
		return max(time.Duration(0), remaining), true
	}
	return delay, false
}

func (p *Pack) scheduleRetry(ctx context.Context, req *spi.Request, state string, input any, attempts map[int]int, variables map[string]any, delay time.Duration, hist []any) *walkResult {
	if delay <= 0 {
		return nil
	}
	switch first(req.Input, "_executionType") {
	case "EXPRESS":
		delay, timeout := p.executionDelay(req, delay)
		select {
		case <-p.deps.Clock.After(delay):
			if timeout {
				return &walkResult{out: input, status: "TIMED_OUT", cause: "States.Timeout", errorName: "States.Timeout", hist: hist}
			}
			return nil
		case <-ctx.Done():
			return &walkResult{out: input, status: "FAILED", cause: "States.Timeout", errorName: "States.Timeout", hist: hist}
		}
	case "STANDARD":
		return &walkResult{out: input, status: "RUNNING", hist: hist, pending: &pending{
			Token: p.deps.Rand.Hex(16), StateName: state, StateInput: input, Retries: attempts, Variables: variables, Retry: true,
			WaitUntil: p.deps.Clock.Now().Add(delay).UTC().Format(time.RFC3339Nano), Deadline: first(req.Input, "_executionDeadline"),
		}}
	default:
		return nil
	}
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

func applyCatchAssignments(state map[string]any, failure stateFailure, _ any, context map[string]any, variables map[string]any, random spi.Rand) bool {
	for _, raw := range asSlice(state["Catch"]) {
		catcher, _ := raw.(map[string]any)
		if matchesError(catcher["ErrorEquals"], failure.name) {
			return applyJSONPathAssignments(map[string]any{"Error": failure.name, "Cause": failure.cause}, context, variables, variables, random, catcher)
		}
	}
	return true
}

func recoverState(st map[string]any, failed walkResult, input any, attempts map[int]int, random spi.Rand) (string, any, time.Duration, bool, bool) {
	name := failed.errorName
	if name == "" {
		name = "States.TaskFailed"
	}
	failure := stateFailure{name: name, cause: failed.cause}
	if delay, retry := retryTask(st, name, attempts, random); retry {
		return "", input, delay, true, false
	}
	next, out, caught := catchTask(st, failure, input)
	return next, out, 0, false, caught
}

func recoverJSONata(state map[string]any, failed walkResult, input any, context map[string]any, variables map[string]any, random spi.Rand, attempts map[int]int) (string, any, time.Duration, bool, bool, bool) {
	name := failed.errorName
	if name == "" {
		name = "States.TaskFailed"
	}
	failure := stateFailure{name: name, cause: failed.cause}
	if delay, retry := retryTask(state, name, attempts, random); retry {
		return "", input, delay, true, false, true
	}
	next, out, caught, valid := catchJSONata(state, failure, input, context, variables, random)
	return next, out, 0, false, caught, valid
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
	if !ok {
		return input, false
	}
	tokens, ok := jsonPathTokens(path[1:])
	if !ok || len(tokens) == 0 {
		return input, false
	}
	var build func([]pathToken) (any, bool)
	build = func(tokens []pathToken) (any, bool) {
		if len(tokens) == 0 {
			return result, true
		}
		child, valid := build(tokens[1:])
		if !valid {
			return nil, false
		}
		switch token := tokens[0]; token.kind {
		case 'f':
			return map[string]any{token.key: child}, true
		default:
			return nil, false
		}
	}
	current := input
	for index, token := range tokens {
		last := index == len(tokens)-1
		switch token.kind {
		case 'f':
			object, valid := current.(map[string]any)
			if !valid {
				return input, false
			}
			if last {
				object[token.key] = result
				return input, true
			}
			next, exists := object[token.key]
			if !exists {
				next, valid = build(tokens[index+1:])
				if !valid {
					return input, false
				}
				object[token.key] = next
				return input, true
			}
			current = next
		case 'i':
			array, valid := current.([]any)
			position := token.start
			if position < 0 {
				position += len(array)
			}
			if !valid || position < 0 || position >= len(array) {
				return input, false
			}
			if last {
				array[position] = result
				return input, true
			}
			current = array[position]
		default:
			return input, false
		}
	}
	return input, false
}

func applyStateResult(state map[string]any, input, result any, context map[string]any, random spi.Rand, variables ...map[string]any) (any, bool) {
	if selector, ok := state["ResultSelector"].(map[string]any); ok {
		var valid bool
		result, valid = applyParamsValidated(selector, result, context, random, variables...)
		if !valid {
			return input, false
		}
	}
	return applyResultPath(state, input, result)
}

func applyDataPath(state map[string]any, key string, input any, variables ...map[string]any) (any, bool) {
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
	return jsonPathLookup(input, path, variables...)
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
		jsonataMu.Lock()
		defer jsonataMu.Unlock()
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

func applyJSONataState(state map[string]any, input, result any, context map[string]any, variables map[string]any, random spi.Rand, visibleVariables ...map[string]any) (any, bool) {
	visible := variables
	if len(visibleVariables) > 0 {
		visible = visibleVariables[0]
	}
	scope := jsonataScope{input: input, result: result, hasResult: true, context: context, variables: visible, random: random}
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
	return commitAssignments(variables, assignments)
}

func applyJSONPathAssignments(data any, context map[string]any, visibleVariables, variables map[string]any, random spi.Rand, owners ...map[string]any) bool {
	assignments := map[string]any{}
	for _, owner := range owners {
		assign, _ := owner["Assign"].(map[string]any)
		resolved, ok := applyParamsValidated(assign, data, context, random, visibleVariables)
		if !ok {
			return false
		}
		for rawName := range assign {
			name := strings.TrimSuffix(rawName, ".$")
			if !validVariableName(name) || name == "states" {
				return false
			}
			assignments[name] = resolved[name]
		}
	}
	return commitAssignments(variables, assignments)
}

func commitAssignments(variables, assignments map[string]any) bool {
	encoded, err := json.Marshal(assignments)
	if err != nil || len(encoded) > 256*1024 {
		return false
	}
	updated := maps.Clone(variables)
	for name, value := range assignments {
		encoded, err := json.Marshal(value)
		if err != nil || len(encoded) > 256*1024 {
			return false
		}
		updated[name] = value
	}
	encoded, err = json.Marshal(updated)
	if err != nil || len(encoded) > 10*1024*1024 {
		return false
	}
	for name, value := range assignments {
		variables[name] = cloneJSON(value)
	}
	return true
}

func cloneJSON(value any) any {
	switch value := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(value))
		for key, item := range value {
			cloned[key] = cloneJSON(item)
		}
		return cloned
	case []any:
		cloned := make([]any, len(value))
		for index, item := range value {
			cloned[index] = cloneJSON(item)
		}
		return cloned
	default:
		return value
	}
}

func taskPayload(st map[string]any, data any, context map[string]any, random spi.Rand, variables ...map[string]any) (any, bool) {
	params, ok := st["Parameters"].(map[string]any)
	if !ok {
		return data, true
	}
	return applyParamsValidated(params, data, context, random, variables...)
}

type mapDataset struct {
	values  []any
	keys    []any
	sources []string
}

func (p *Pack) mapItems(ctx context.Context, req *spi.Request, state map[string]any, data any, scope *jsonataScope, variables ...map[string]any) (any, string, bool) {
	reader, hasReader := state["ItemReader"].(map[string]any)
	if !hasReader {
		items := data
		if path := first(state, "ItemsPath"); path != "" {
			items = jsonPath(data, path, variables...)
		}
		switch items.(type) {
		case []any, map[string]any:
			return items, "STATE_DATA", true
		default:
			return nil, "", false
		}
	}
	resource := first(reader, "Resource")
	if resource != "arn:aws:states:::s3:getObject" && resource != "arn:aws:states:::s3:listObjectsV2" {
		return nil, "", false
	}
	parameters, _ := reader["Parameters"].(map[string]any)
	resolved, readerParametersOK := applyParamsValidated(parameters, data, nil, p.deps.Rand, variables...)
	if !readerParametersOK {
		return nil, "", false
	}
	var resolvedValue any = resolved
	if scope != nil {
		var valid bool
		resolvedValue, valid = evalJSONataValue(reader["Arguments"], *scope)
		if _, configured := reader["Arguments"]; !configured {
			return nil, "States.QueryEvaluationError", false
		}
		if !valid {
			return nil, "States.QueryEvaluationError", false
		}
	}
	input, valid := resolvedValue.(map[string]any)
	if !valid {
		if scope != nil {
			return nil, "States.QueryEvaluationError", false
		}
		return nil, "", false
	}
	config, _ := reader["ReaderConfig"].(map[string]any)
	if owner, exists := input["ExpectedBucketOwner"]; exists {
		expected, valid := owner.(string)
		if !valid || expected != req.Identity.Account {
			return nil, "", false
		}
	}
	if resource == "arn:aws:states:::s3:getObject" && first(config, "InputType") == "PARQUET" {
		if _, exists := input["VersionId"]; exists {
			return nil, "", false
		}
	}
	if resource == "arn:aws:states:::s3:listObjectsV2" {
		items, request := []any{}, maps.Clone(input)
		for {
			response, err := s3.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "ListObjectsV2", Input: request})
			if err != nil || response == nil {
				return nil, "", false
			}
			items = append(items, asSlice(response.Output["Contents"])...)
			truncated, _ := response.Output["IsTruncated"].(bool)
			if !truncated {
				break
			}
			token := first(response.Output, "NextContinuationToken")
			if token == "" {
				return nil, "", false
			}
			request = maps.Clone(request)
			request["ContinuationToken"] = token
		}
		if first(config, "Transformation") != "LOAD_AND_FLATTEN" {
			return limitReaderItems(items, "S3://"+first(input, "Bucket"), config, data, scope, variables...)
		}
		flattened := mapDataset{}
		for _, rawItem := range items {
			item, _ := rawItem.(map[string]any)
			payload := map[string]any{"Bucket": input["Bucket"], "Key": item["Key"]}
			nestedConfig := maps.Clone(config)
			delete(nestedConfig, "Transformation")
			delete(nestedConfig, "MaxItems")
			delete(nestedConfig, "MaxItemsPath")
			nestedReader := map[string]any{"Resource": "arn:aws:states:::s3:getObject", "ReaderConfig": nestedConfig, "Parameters": payload}
			if scope != nil {
				nestedReader["Arguments"] = payload
			}
			dataset, nestedSource, valid := p.mapItems(ctx, req, map[string]any{"ItemReader": nestedReader}, data, scope, variables...)
			if !valid {
				return nil, "", false
			}
			switch values := dataset.(type) {
			case []any:
				flattened.values = append(flattened.values, values...)
				flattened.keys = append(flattened.keys, make([]any, len(values))...)
				for range values {
					flattened.sources = append(flattened.sources, nestedSource)
				}
			case map[string]any:
				for _, key := range slices.Sorted(maps.Keys(values)) {
					flattened.values = append(flattened.values, values[key])
					flattened.keys = append(flattened.keys, key)
					flattened.sources = append(flattened.sources, nestedSource)
				}
			default:
				return nil, "", false
			}
		}
		return limitReaderItems(flattened, "", config, data, scope, variables...)
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
	switch key := strings.ToLower(first(input, "Key")); {
	case strings.HasSuffix(key, ".gz"):
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, "", false
		}
		body, readErr = io.ReadAll(reader)
		closeErr = reader.Close()
		if readErr != nil || closeErr != nil {
			return nil, "", false
		}
	case strings.HasSuffix(key, ".zstd"):
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, "", false
		}
		body, readErr = decoder.DecodeAll(body, nil)
		decoder.Close()
		if readErr != nil {
			return nil, "", false
		}
	}
	inputType := first(config, "InputType")
	manifestType := first(config, "ManifestType")
	if manifestType == "S3_INVENTORY" || inputType == "MANIFEST" {
		var manifest struct {
			SourceBucket      string `json:"sourceBucket"`
			DestinationBucket string `json:"destinationBucket"`
			Version           string `json:"version"`
			CreationTimestamp string `json:"creationTimestamp"`
			FileFormat        string `json:"fileFormat"`
			FileSchema        string `json:"fileSchema"`
			Files             []struct {
				Key string `json:"key"`
			} `json:"files"`
		}
		if json.Unmarshal(body, &manifest) != nil || manifest.SourceBucket == "" || manifest.DestinationBucket == "" || manifest.Version != "2016-11-30" || manifest.CreationTimestamp == "" || manifest.FileFormat != "CSV" || manifest.Files == nil {
			return nil, "", false
		}
		headers, err := csv.NewReader(strings.NewReader(manifest.FileSchema)).Read()
		if err != nil || len(headers) == 0 {
			return nil, "", false
		}
		configuredHeaders := make([]any, len(headers))
		for index, header := range headers {
			header = strings.TrimSpace(header)
			if header == "" {
				return nil, "", false
			}
			configuredHeaders[index] = header
		}
		inventoryItems := mapDataset{}
		for _, file := range manifest.Files {
			if file.Key == "" {
				return nil, "", false
			}
			nestedConfig := maps.Clone(config)
			delete(nestedConfig, "ManifestType")
			delete(nestedConfig, "MaxItems")
			delete(nestedConfig, "MaxItemsPath")
			nestedConfig["InputType"] = "CSV"
			nestedConfig["CSVHeaderLocation"] = "GIVEN"
			nestedConfig["CSVHeaders"] = configuredHeaders
			payload := map[string]any{"Bucket": input["Bucket"], "Key": file.Key}
			nestedReader := map[string]any{"Resource": "arn:aws:states:::s3:getObject", "ReaderConfig": nestedConfig, "Parameters": payload}
			if scope != nil {
				nestedReader["Arguments"] = payload
			}
			dataset, nestedSource, valid := p.mapItems(ctx, req, map[string]any{"ItemReader": nestedReader}, data, scope, variables...)
			values, array := dataset.([]any)
			if !valid || !array {
				return nil, "", false
			}
			inventoryItems.values = append(inventoryItems.values, values...)
			inventoryItems.keys = append(inventoryItems.keys, make([]any, len(values))...)
			for range values {
				inventoryItems.sources = append(inventoryItems.sources, nestedSource)
			}
		}
		return limitReaderItems(inventoryItems, "", config, data, scope, variables...)
	}
	if inputType == "" {
		inputType = "JSON"
	}
	if manifestType == "ATHENA_DATA" {
		records, err := csv.NewReader(bytes.NewReader(body)).ReadAll()
		if err != nil {
			return nil, "", false
		}
		items := mapDataset{}
		for _, record := range records {
			if len(record) != 1 {
				return nil, "", false
			}
			bucket, key, valid := s3URI(record[0])
			if !valid {
				return nil, "", false
			}
			nestedConfig := maps.Clone(config)
			delete(nestedConfig, "ManifestType")
			delete(nestedConfig, "MaxItems")
			delete(nestedConfig, "MaxItemsPath")
			payload := map[string]any{"Bucket": bucket, "Key": key}
			nestedReader := map[string]any{"Resource": "arn:aws:states:::s3:getObject", "ReaderConfig": nestedConfig, "Parameters": payload}
			if scope != nil {
				nestedReader["Arguments"] = payload
			}
			dataset, nestedSource, valid := p.mapItems(ctx, req, map[string]any{"ItemReader": nestedReader}, data, scope, variables...)
			values, array := dataset.([]any)
			if !valid || !array {
				return nil, "", false
			}
			items.values = append(items.values, values...)
			items.keys = append(items.keys, make([]any, len(values))...)
			for range values {
				items.sources = append(items.sources, nestedSource)
			}
		}
		return limitReaderItems(items, "", config, data, scope, variables...)
	}
	source := s3Source(input)
	switch inputType {
	case "JSON":
		var items any
		if json.Unmarshal(body, &items) != nil {
			return nil, "", false
		}
		if pointer, configured := config["ItemsPointer"].(string); configured {
			tokens, pointerValid := jsonPointerTokens(pointer)
			offset, found := jsonPointerOffset(body, tokens)
			if !pointerValid || !found || offset >= 16*1024*1024 {
				return nil, "", false
			}
			var valid bool
			items, valid = resolveJSONPointer(items, pointer)
			if !valid {
				return nil, "", false
			}
		}
		switch items.(type) {
		case []any, map[string]any:
		default:
			return nil, "", false
		}
		return limitReaderItems(items, source, config, data, scope, variables...)
	case "JSONL":
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		items := []any{}
		for {
			var item any
			if err := decoder.Decode(&item); errors.Is(err, io.EOF) {
				return limitReaderItems(items, source, config, data, scope, variables...)
			} else if err != nil {
				return nil, "", false
			}
			items = append(items, item)
		}
	case "CSV":
		parser := csv.NewReader(strings.NewReader(string(body)))
		parser.FieldsPerRecord = -1
		parser.LazyQuotes = true
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
			item := map[string]any{}
			for index, header := range headers {
				item[header] = ""
				if index < len(record) {
					item[header] = unescapeCSVField(record[index])
				}
			}
			items = append(items, item)
		}
		return limitReaderItems(items, source, config, data, scope, variables...)
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
		return limitReaderItems(items, source, config, data, scope, variables...)
	default:
		return nil, "", false
	}
}

func unescapeCSVField(value string) string {
	decoded := make([]byte, 0, len(value))
	for index := 0; index < len(value); index++ {
		if value[index] == '\\' {
			index++
			if index == len(value) {
				break
			}
		}
		decoded = append(decoded, value[index])
	}
	return string(decoded)
}

func jsonPointerTokens(pointer string) ([]string, bool) {
	if pointer == "" {
		return nil, true
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, false
	}
	tokens := make([]string, 0, strings.Count(pointer, "/"))
	for _, raw := range strings.Split(pointer[1:], "/") {
		for index := 0; index < len(raw); index++ {
			if raw[index] == '~' && (index+1 == len(raw) || raw[index+1] != '0' && raw[index+1] != '1') {
				return nil, false
			}
			if raw[index] == '~' {
				index++
			}
		}
		tokens = append(tokens, strings.ReplaceAll(strings.ReplaceAll(raw, "~1", "/"), "~0", "~"))
	}
	return tokens, true
}

func s3URI(value string) (string, string, bool) {
	bucket, key, valid := strings.Cut(strings.TrimPrefix(value, "s3://"), "/")
	return bucket, key, strings.HasPrefix(value, "s3://") && valid && bucket != "" && key != ""
}

func s3Source(input map[string]any) string {
	source := "S3://" + first(input, "Bucket")
	if key := first(input, "Key"); key != "" {
		source += "/" + key
	}
	return source
}

func jsonPointerOffset(body []byte, tokens []string) (int64, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	var scan func([]string) (int64, bool)
	scan = func(path []string) (int64, bool) {
		start := decoder.InputOffset()
		for start < int64(len(body)) && (body[start] == ' ' || body[start] == '\n' || body[start] == '\r' || body[start] == '\t' || body[start] == ':' || body[start] == ',') {
			start++
		}
		if len(path) == 0 {
			return start, true
		}
		token, err := decoder.Token()
		if err != nil {
			return 0, false
		}
		delimiter, composite := token.(json.Delim)
		if !composite {
			return 0, false
		}
		skip := func() bool {
			var value json.RawMessage
			return decoder.Decode(&value) == nil
		}
		switch delimiter {
		case '{':
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return 0, false
				}
				if key == path[0] {
					return scan(path[1:])
				}
				if !skip() {
					return 0, false
				}
			}
		case '[':
			index, err := strconv.Atoi(path[0])
			if err != nil || index < 0 || len(path[0]) > 1 && path[0][0] == '0' {
				return 0, false
			}
			for current := 0; decoder.More(); current++ {
				if current == index {
					return scan(path[1:])
				}
				if !skip() {
					return 0, false
				}
			}
		}
		return 0, false
	}
	return scan(tokens)
}

func resolveJSONPointer(value any, pointer string) (any, bool) {
	tokens, valid := jsonPointerTokens(pointer)
	if !valid {
		return nil, false
	}
	for _, token := range tokens {
		switch current := value.(type) {
		case map[string]any:
			var exists bool
			value, exists = current[token]
			if !exists {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(current) || len(token) > 1 && token[0] == '0' {
				return nil, false
			}
			value = current[index]
		default:
			return nil, false
		}
	}
	return value, true
}

func limitReaderItems(items any, source string, config map[string]any, data any, scope *jsonataScope, variables ...map[string]any) (any, string, bool) {
	value, hasValue := config["MaxItems"]
	path, hasPath := config["MaxItemsPath"].(string)
	if !hasValue && !hasPath {
		return items, source, true
	}
	if scope != nil {
		var valid bool
		value, valid = evalJSONataValue(value, *scope)
		if !valid || hasPath {
			return nil, "States.QueryEvaluationError", false
		}
	} else if hasValue == hasPath {
		return nil, "", false
	} else if hasPath {
		value = jsonPath(data, path, variables...)
	}
	maximum, numeric := exactNumber(value)
	limit := int(maximum)
	if !numeric || maximum != math.Trunc(maximum) || limit < 1 {
		if scope != nil {
			return nil, "States.QueryEvaluationError", false
		}
		return nil, "", false
	}
	switch collection := items.(type) {
	case []any:
		if limit < len(collection) {
			items = collection[:limit]
		}
	case mapDataset:
		if limit < len(collection.values) {
			collection.values = collection.values[:limit]
			collection.keys = collection.keys[:limit]
			collection.sources = collection.sources[:limit]
			items = collection
		}
	case map[string]any:
		if limit < len(collection) {
			keys := slices.Sorted(maps.Keys(collection))
			limited := make(map[string]any, limit)
			for _, key := range keys[:limit] {
				limited[key] = collection[key]
			}
			items = limited
		}
	default:
		return nil, "", false
	}
	return items, source, true
}

func batchMapItems(state map[string]any, data any, items []any, random spi.Rand, scope *jsonataScope, variables ...map[string]any) ([]any, bool) {
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
			value = jsonPath(data, path, variables...)
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
	var batchInput any
	rawBatchInput, hasBatchInput := batcher["BatchInput"]
	if hasBatchInput {
		if scope != nil {
			var valid bool
			batchInput, valid = evalJSONataValue(rawBatchInput, *scope)
			if !valid {
				return nil, false
			}
		} else {
			input, ok := rawBatchInput.(map[string]any)
			if !ok {
				return nil, false
			}
			var valid bool
			batchInput, valid = applyParamsValidated(input, data, nil, random, variables...)
			if !valid {
				return nil, false
			}
		}
	}
	makeBatch := func(values []any) map[string]any {
		batch := map[string]any{"Items": append([]any(nil), values...)}
		if hasBatchInput {
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
	if arn := first(req.Input, "_stateMachineArn", "stateMachineArn", "StateMachineArn"); arn != "" {
		return baseSMName(arn)
	}
	parts := strings.Split(first(req.Input, "_executionArn", "executionArn", "ExecutionArn"), ":")
	if len(parts) > 6 {
		return strings.Split(parts[6], "/")[0]
	}
	return "stateMachine"
}

func (p *Pack) mapItemResult(req *spi.Request, state, name, startTime string, input any, result walkResult) mapItemResult {
	machine := mapStateMachineName(req) + "/" + state
	inputJSON, _ := json.Marshal(input)
	metadata := map[string]any{
		"ExecutionArn": "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":execution:" + machine + ":" + name,
		"Input":        string(inputJSON), "InputDetails": map[string]any{"Included": true}, "Name": name,
		"StartDate": startTime, "StateMachineArn": p.smARN(req, machine), "Status": result.status,
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

func (p *Pack) storeMapItemExecution(ctx context.Context, req *spi.Request, mapRunARN, definition, executionType string, item mapItemResult) {
	metadata := item.metadata
	now := float64(p.deps.Clock.Now().Unix())
	record := map[string]any{
		"executionArn": metadata["ExecutionArn"], "stateMachineArn": metadata["StateMachineArn"], "name": metadata["Name"],
		"status": metadata["Status"], "startDate": now, "stopDate": now, "input": metadata["Input"], "mapRunArn": mapRunARN,
		"itemCount": float64(item.itemCount), "type": executionType, "definition": definition, "history": item.history,
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

func (p *Pack) writeMapResults(ctx context.Context, req *spi.Request, state map[string]any, data any, items []mapItemResult, mapRunARN string, scope *jsonataScope, variables ...map[string]any) (any, int, bool, bool) {
	writer, ok := state["ResultWriter"].(map[string]any)
	if !ok || len(writer) == 0 {
		return nil, 0, false, false
	}
	config, hasConfig := writer["WriterConfig"].(map[string]any)
	resource := first(writer, "Resource")
	parameters, hasParameters := writer["Parameters"].(map[string]any)
	if !hasParameters {
		parameters, hasParameters = writer["Arguments"].(map[string]any)
	}
	if resource == "" && !hasConfig || resource != "" && (resource != "arn:aws:states:::s3:putObject" || !hasParameters) {
		return nil, 0, false, false
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
		return nil, 0, false, false
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
			return formatted, 0, true, false
		}
		encoded, valid := encode(formatted)
		return string(encoded), 0, valid, false
	}
	resolved, writerParametersOK := applyParamsValidated(parameters, data, nil, p.deps.Rand, variables...)
	if !writerParametersOK {
		return nil, 0, false, false
	}
	if scope != nil {
		value, valid := evalJSONataValue(writer["Arguments"], *scope)
		if !valid {
			return nil, 0, false, true
		}
		resolved, valid = value.(map[string]any)
		if !valid {
			return nil, 0, false, true
		}
	}
	bucket, prefix := first(resolved, "Bucket"), strings.Trim(first(resolved, "Prefix"), "/")
	if bucket == "" {
		return nil, 0, false, false
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
			return nil, 0, false, false
		}
		resultFiles[status] = []any{map[string]any{"Key": key, "Size": len(body)}}
	}
	manifestKey := prefix + "/manifest.json"
	manifest, _ := json.Marshal(map[string]any{"DestinationBucket": bucket, "MapRunArn": mapRunARN, "ResultFiles": resultFiles})
	if !put(manifestKey, manifest) {
		return nil, 0, false, false
	}
	return map[string]any{"MapRunArn": mapRunARN, "ResultWriterDetails": map[string]any{"Bucket": bucket, "Key": manifestKey}}, len(items), true, false
}

func taskIdentity(st map[string]any, data any, context map[string]any, variables map[string]any, identity spi.Identity, random spi.Rand, scope *jsonataScope) (spi.Identity, bool) {
	credentials, ok := st["Credentials"].(map[string]any)
	if !ok {
		return identity, false
	}
	resolved, parametersOK := applyParamsValidated(credentials, data, context, random, variables)
	if !parametersOK {
		return identity, false
	}
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

func applyParams(params map[string]any, data any, context map[string]any, random spi.Rand, variables ...map[string]any) map[string]any {
	out, _ := applyParamsValidated(params, data, context, random, variables...)
	return out
}

func applyParamsValidated(params map[string]any, data any, context map[string]any, random spi.Rand, variables ...map[string]any) (map[string]any, bool) {
	out := map[string]any{}
	for k, v := range params {
		if strings.HasSuffix(k, ".$") {
			path := fmt.Sprint(v)
			if strings.HasPrefix(path, "States.") {
				var ok bool
				out[strings.TrimSuffix(k, ".$")], ok = evalIntrinsic(path, data, context, random, variables...)
				if !ok {
					return nil, false
				}
				continue
			}
			source := data
			if strings.HasPrefix(path, "$$.") {
				source, path = context, strings.TrimPrefix(path, "$")
			}
			var ok bool
			out[strings.TrimSuffix(k, ".$")], ok = jsonPathLookup(source, path, variables...)
			if !ok {
				return nil, false
			}
			continue
		}
		var valid bool
		out[k], valid = applyParamValue(v, data, context, random, variables...)
		if !valid {
			return nil, false
		}
	}
	return out, true
}

func applyParamValue(value, data any, context map[string]any, random spi.Rand, variables ...map[string]any) (any, bool) {
	switch value := value.(type) {
	case map[string]any:
		return applyParamsValidated(value, data, context, random, variables...)
	case []any:
		out := make([]any, len(value))
		for index, item := range value {
			var valid bool
			out[index], valid = applyParamValue(item, data, context, random, variables...)
			if !valid {
				return nil, false
			}
		}
		return out, true
	default:
		return value, true
	}
}

func evalIntrinsic(expression string, data any, context map[string]any, random spi.Rand, variables ...map[string]any) (any, bool) {
	open := strings.IndexByte(expression, '(')
	if open < len("States.") || !strings.HasSuffix(expression, ")") {
		return nil, false
	}
	name := expression[len("States."):open]
	rawArgs := splitIntrinsicArgs(expression[open+1 : len(expression)-1])
	args := make([]any, len(rawArgs))
	for i, raw := range rawArgs {
		var ok bool
		args[i], ok = evalIntrinsicArg(raw, data, context, random, variables...)
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

func evalIntrinsicArg(raw string, data any, context map[string]any, random spi.Rand, variables ...map[string]any) (any, bool) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "States.") {
		return evalIntrinsic(raw, data, context, random, variables...)
	}
	if strings.HasPrefix(raw, "$$.") {
		return jsonPathLookup(context, strings.TrimPrefix(raw, "$"), variables...)
	}
	if strings.HasPrefix(raw, "$") {
		return jsonPathLookup(data, raw, variables...)
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
	quote, escaped := rune(0), false
	for i, r := range raw {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && quote != 0 {
			escaped = true
			continue
		}
		if quote == r {
			quote = 0
			continue
		}
		if quote == 0 && (r == '\'' || r == '"') {
			quote = r
			continue
		}
		if quote != 0 {
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

func jsonPathFunctionArguments(raw string) ([]string, bool) {
	arguments := splitIntrinsicArgs(raw)
	for index, argument := range arguments {
		argument = strings.TrimSpace(argument)
		if argument == "" {
			return nil, false
		}
		if strings.HasPrefix(argument, "$") {
			if !validJSONPath(argument, false) {
				return nil, false
			}
		} else {
			var value any
			if json.Unmarshal([]byte(argument), &value) != nil {
				return nil, false
			}
		}
		arguments[index] = argument
	}
	return arguments, true
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

func selectedChoice(st map[string]any, data any, variables ...map[string]any) (string, map[string]any) {
	choices, _ := st["Choices"].([]any)
	for _, c := range choices {
		cm, _ := c.(map[string]any)
		if matchChoice(cm, data, variables...) {
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

func matchChoice(cm map[string]any, data any, variables ...map[string]any) bool {
	if rules := asSlice(cm["And"]); rules != nil {
		for _, raw := range rules {
			rule, _ := raw.(map[string]any)
			if !matchChoice(rule, data, variables...) {
				return false
			}
		}
		return len(rules) > 0
	}
	if rules := asSlice(cm["Or"]); rules != nil {
		for _, raw := range rules {
			rule, _ := raw.(map[string]any)
			if matchChoice(rule, data, variables...) {
				return true
			}
		}
		return false
	}
	if raw, ok := cm["Not"].(map[string]any); ok {
		return !matchChoice(raw, data, variables...)
	}
	got, present := jsonPathLookup(data, first(cm, "Variable"), variables...)
	if expected, ok := cm["IsPresent"]; ok {
		return present == toBool(expected)
	}
	if !present {
		return false
	}
	if operator, ok := cm["CollectionOperator"].(string); ok {
		right := cm["CollectionValue"]
		if path, ok := cm["CollectionPath"].(string); ok {
			var found bool
			right, found = jsonPathLookup(data, path, variables...)
			if !found {
				return operator == "nin"
			}
		}
		return jsonPathFilterCollection(operator, got, right)
	}
	if operator, ok := cm["ValueOperator"].(string); ok {
		right := cm["Value"]
		if path, ok := cm["ValuePath"].(string); ok {
			var found bool
			right, found = jsonPathLookup(data, path, variables...)
			if !found {
				return false
			}
		}
		return jsonPathFilterValue(operator, got, right)
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
	if pattern, ok := cm["StringRegex"].(*regexp.Regexp); ok {
		values := []any{got}
		if array, ok := got.([]any); ok {
			values = array
		}
		for _, value := range values {
			text := ""
			if _, ok := value.(string); ok || isBool(value) || isNumber(value) {
				text = fmt.Sprint(value)
			}
			if pattern.MatchString(text) {
				return true
			}
		}
		return false
	}
	if op, want, ok := choiceComparison(cm, "String", data, variables...); ok {
		left, leftOK := got.(string)
		right, rightOK := want.(string)
		return leftOK && rightOK && compareResult(strings.Compare(left, right), op)
	}
	if op, want, ok := choiceComparison(cm, "Numeric", data, variables...); ok {
		left, leftOK := choiceNumber(got)
		right, rightOK := choiceNumber(want)
		return leftOK && rightOK && numericComparison(left, right, op)
	}
	if op, want, ok := choiceComparison(cm, "Boolean", data, variables...); ok {
		left, leftOK := got.(bool)
		right, rightOK := want.(bool)
		return op == "Equals" && leftOK && rightOK && left == right
	}
	if op, want, ok := choiceComparison(cm, "Timestamp", data, variables...); ok {
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

func choiceComparison(rule map[string]any, prefix string, data any, variables ...map[string]any) (string, any, bool) {
	for _, op := range []string{"Equals", "LessThan", "LessThanEquals", "GreaterThan", "GreaterThanEquals"} {
		if value, exists := rule[prefix+op]; exists {
			return op, value, true
		}
		if path, exists := rule[prefix+op+"Path"].(string); exists {
			value, found := jsonPathLookup(data, path, variables...)
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

func jsonPath(data any, path string, variables ...map[string]any) any {
	value, _ := jsonPathLookup(data, path, variables...)
	return value
}

func jsonPathLookup(data any, path string, variables ...map[string]any) (any, bool) {
	rootData := data
	if len(path) > 1 && path[0] == '$' && path[1] != '.' && path[1] != '[' && path[1] != '$' {
		end := strings.IndexAny(path, ".[")
		if end < 0 {
			end = len(path)
		}
		if len(variables) == 0 {
			return nil, false
		}
		var ok bool
		data, ok = variables[0][path[1:end]]
		if !ok {
			return nil, false
		}
		path = "$" + path[end:]
	}
	if path == "$" || path == "" {
		return data, true
	}
	tokens, ok := jsonPathTokens(strings.TrimPrefix(path, "$"))
	if !ok {
		return nil, false
	}
	nodes := []any{data}
	multiple := false
	var filterVariables map[string]any
	resolveArguments := func(rawArguments []string) ([]any, bool) {
		arguments := make([]any, len(rawArguments))
		for index, raw := range rawArguments {
			var valid bool
			arguments[index], valid = evalIntrinsicArg(raw, rootData, nil, nil, variables...)
			if !valid {
				return nil, false
			}
		}
		return arguments, true
	}
	for _, token := range tokens {
		if token.kind == 'l' || token.kind == 'n' || token.kind == 'j' {
			value := any(nodes)
			if len(nodes) == 1 {
				value = nodes[0]
			}
			arguments, valid := resolveArguments(token.arguments)
			if !valid {
				return nil, false
			}
			if token.kind == 'l' {
				if len(arguments) > 0 {
					if len(arguments) != 1 {
						return nil, false
					}
					value = arguments[0]
				}
				length := -1
				switch value := value.(type) {
				case []any:
					length = len(value)
				case map[string]any:
					length = len(value)
				}
				if length < 0 {
					return nil, false
				}
				nodes, multiple = []any{float64(length)}, false
				continue
			}
			if token.kind == 'j' {
				switch token.key {
				case "keys":
					object, valid := value.(map[string]any)
					if !valid {
						return nil, false
					}
					keys := slices.Sorted(maps.Keys(object))
					nodes = []any{keys}
				case "first", "last", "index":
					array, valid := value.([]any)
					if !valid || len(array) == 0 {
						return nil, false
					}
					index := 0
					if token.key == "last" {
						index = len(array) - 1
					} else if token.key == "index" {
						index = token.start
						if index < 0 {
							index += len(array)
						}
						if index < 0 || index >= len(array) {
							return nil, false
						}
					}
					nodes = []any{array[index]}
				}
				multiple = false
				continue
			}
			array, _ := value.([]any)
			values := make([]float64, 0, len(array)+len(arguments))
			consume := func(items []any) {
				for _, item := range items {
					if number, numeric := choiceNumber(item); numeric {
						values = append(values, number)
					}
				}
			}
			consume(array)
			for _, argument := range arguments {
				if array, valid := argument.([]any); valid {
					consume(array)
				} else {
					consume([]any{argument})
				}
			}
			if len(values) == 0 {
				return nil, false
			}
			result := values[0]
			switch token.key {
			case "min":
				result = slices.Min(values)
			case "max":
				result = slices.Max(values)
			case "avg", "sum", "stddev":
				result = 0
				for _, number := range values {
					result += number
				}
				if token.key == "avg" {
					result /= float64(len(values))
				} else if token.key == "stddev" {
					average := result / float64(len(values))
					result = 0
					for _, number := range values {
						result += (number - average) * (number - average)
					}
					result = math.Sqrt(result / float64(len(values)))
				}
			}
			nodes, multiple = []any{result}, false
			continue
		}
		if token.kind == 'a' {
			value := any(nodes)
			if len(nodes) == 1 {
				value = nodes[0]
			}
			arguments, valid := resolveArguments(token.arguments)
			if !valid {
				return nil, false
			}
			switch token.key {
			case "concat":
				var result strings.Builder
				if array, valid := value.([]any); valid {
					for _, item := range array {
						if text, valid := item.(string); valid {
							result.WriteString(text)
						}
					}
				}
				for _, argument := range arguments {
					values := []any{argument}
					if array, valid := argument.([]any); valid {
						values = array
					}
					for _, item := range values {
						if item != nil {
							result.WriteString(fmt.Sprint(item))
						}
					}
				}
				nodes = []any{result.String()}
			case "append":
				array, valid := value.([]any)
				if !valid {
					return nil, false
				}
				nodes = []any{append(append([]any(nil), array...), arguments...)}
			}
			multiple = false
			continue
		}
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
				if array, ok := node.([]any); ok {
					index := token.start
					if index < 0 {
						index += len(array)
					}
					if index >= 0 && index < len(array) {
						next = append(next, array[index])
					}
				}
			case '*':
				switch node := node.(type) {
				case []any:
					next = append(next, node...)
				case map[string]any:
					for _, key := range slices.Sorted(maps.Keys(node)) {
						next = append(next, node[key])
					}
				}
			case 's':
				if array, ok := node.([]any); ok {
					start, end := token.start, token.end
					if start < 0 {
						start += len(array)
					}
					if end < 0 {
						end += len(array)
					}
					start, end = max(0, start), min(len(array), end)
					if start <= end {
						if token.step == 1 {
							next = append(next, array[start:end]...)
						} else {
							for index := start; index < end; index += token.step {
								next = append(next, array[index])
							}
						}
					}
				}
			case 'u':
				for _, selector := range token.selectors {
					switch selector.kind {
					case 'f':
						if object, ok := node.(map[string]any); ok {
							if value, exists := object[selector.key]; exists {
								next = append(next, value)
							}
						}
					case 'i':
						if array, ok := node.([]any); ok {
							index := selector.start
							if index < 0 {
								index += len(array)
							}
							if index >= 0 && index < len(array) {
								next = append(next, array[index])
							}
						}
					}
				}
			case 'r':
				var descend func(any)
				descend = func(value any) {
					switch value := value.(type) {
					case map[string]any:
						for _, key := range slices.Sorted(maps.Keys(value)) {
							child := value[key]
							if token.key == "*" || token.key == key {
								next = append(next, child)
							}
							descend(child)
						}
					case []any:
						for _, child := range value {
							if token.key == "*" {
								next = append(next, child)
							}
							descend(child)
						}
					}
				}
				descend(node)
			case 'q':
				var values []any
				switch node := node.(type) {
				case []any:
					values = node
				case map[string]any:
					values = []any{node}
				default:
					continue
				}
				if filterVariables == nil {
					filterVariables = map[string]any{"!": data}
					if len(variables) > 0 {
						maps.Copy(filterVariables, variables[0])
					}
				}
				for _, value := range values {
					if matchChoice(token.filter, value, filterVariables) {
						next = append(next, value)
					}
				}
			}
		}
		if token.kind == '*' || token.kind == 's' || token.kind == 'u' || token.kind == 'r' || token.kind == 'q' {
			multiple = true
		}
		nodes = next
	}
	if multiple {
		if nodes == nil {
			nodes = []any{}
		}
		return nodes, true
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
	step       int
	selectors  []pathToken
	filter     map[string]any
	arguments  []string
}

func jsonPathUnquote(value string) (string, bool) {
	if len(value) < 2 || value[0] != value[len(value)-1] || value[0] != '\'' && value[0] != '"' {
		return "", false
	}
	if value[0] == '"' {
		var decoded string
		err := json.Unmarshal([]byte(value), &decoded)
		return decoded, err == nil
	}
	var quoted strings.Builder
	quoted.WriteByte('"')
	for index := 1; index < len(value)-1; index++ {
		if value[index] == '"' {
			quoted.WriteByte('\\')
		}
		if value[index] == '\\' && index+1 < len(value)-1 && (value[index+1] == '\'' || value[index+1] == '"') {
			index++
			if value[index] == '"' {
				quoted.WriteByte('\\')
			}
		}
		quoted.WriteByte(value[index])
	}
	quoted.WriteByte('"')
	var decoded string
	err := json.Unmarshal([]byte(quoted.String()), &decoded)
	return decoded, err == nil
}

func jsonPathTokens(path string) ([]pathToken, bool) {
	var tokens []pathToken
	for len(path) > 0 {
		recursive := false
		if path[0] == '.' {
			recursive = len(path) > 1 && path[1] == '.'
			if recursive {
				path = path[2:]
			} else {
				path = path[1:]
			}
			if len(path) == 0 || path[0] == '.' || path[0] == '[' {
				return nil, false
			}
		} else if path[0] != '[' {
			return nil, false
		}
		if !recursive {
			for _, function := range []string{"length", "size"} {
				prefix := function + "("
				if strings.HasPrefix(path, prefix) && strings.HasSuffix(path, ")") {
					arguments, valid := jsonPathFunctionArguments(path[len(prefix) : len(path)-1])
					if !valid {
						return nil, false
					}
					tokens = append(tokens, pathToken{kind: 'l', arguments: arguments})
					path = ""
					break
				}
			}
			if path == "" {
				break
			}
			for _, function := range []string{"min", "max", "avg", "stddev", "sum"} {
				prefix := function + "("
				if strings.HasPrefix(path, prefix) && strings.HasSuffix(path, ")") {
					arguments, valid := jsonPathFunctionArguments(path[len(prefix) : len(path)-1])
					if !valid {
						return nil, false
					}
					tokens = append(tokens, pathToken{kind: 'n', key: function, arguments: arguments})
					path = ""
					break
				}
			}
			if path == "" {
				break
			}
			for _, function := range []string{"keys", "first", "last"} {
				if path == function+"()" {
					tokens = append(tokens, pathToken{kind: 'j', key: function})
					path = ""
					break
				}
			}
			if path == "" {
				break
			}
			if strings.HasPrefix(path, "index(") && strings.HasSuffix(path, ")") {
				index, err := strconv.Atoi(strings.TrimSpace(path[len("index(") : len(path)-1]))
				if err != nil {
					return nil, false
				}
				tokens = append(tokens, pathToken{kind: 'j', key: "index", start: index})
				break
			}
			for _, function := range []string{"concat", "append"} {
				prefix := function + "("
				if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, ")") {
					continue
				}
				arguments, valid := jsonPathFunctionArguments(path[len(prefix) : len(path)-1])
				if !valid {
					return nil, false
				}
				tokens = append(tokens, pathToken{kind: 'a', key: function, arguments: arguments})
				path = ""
				break
			}
			if path == "" {
				break
			}
		}
		if path[0] != '[' {
			var key strings.Builder
			end := 0
			for end < len(path) && path[end] != '.' && path[end] != '[' {
				if path[end] == '\\' {
					end++
					if end == len(path) {
						return nil, false
					}
				} else if character := path[end]; character < utf8.RuneSelf {
					first := key.Len() == 0
					wildcard := first && character == '*' && (end+1 == len(path) || path[end+1] == '.' || path[end+1] == '[')
					if !wildcard && !(character == '_' || character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || !first && character >= '0' && character <= '9') {
						return nil, false
					}
				}
				key.WriteByte(path[end])
				end++
			}
			if key.Len() == 0 {
				return nil, false
			}
			if recursive {
				tokens = append(tokens, pathToken{kind: 'r', key: key.String()})
			} else if path[:end] == "*" {
				tokens = append(tokens, pathToken{kind: '*'})
			} else {
				tokens = append(tokens, pathToken{kind: 'f', key: key.String()})
			}
			path = path[end:]
			continue
		}
		close, nested, quote := 1, 0, byte(0)
		for close < len(path) {
			switch {
			case quote != 0 && path[close] == '\\':
				close++
			case quote != 0 && path[close] == quote:
				quote = 0
			case quote == 0 && (path[close] == '\'' || path[close] == '"' || path[close] == '/'):
				quote = path[close]
			case quote == 0 && path[close] == '[':
				nested++
			case quote == 0 && path[close] == ']':
				if nested == 0 {
					goto memberClosed
				}
				nested--
			}
			close++
		}
		return nil, false
	memberClosed:
		member := path[1:close]
		path = path[close+1:]
		var parts []string
		start, nested, quote := 0, 0, byte(0)
		for index := 0; index < len(member); index++ {
			switch {
			case quote != 0 && member[index] == '\\':
				index++
			case quote != 0 && member[index] == quote:
				quote = 0
			case quote == 0 && (member[index] == '\'' || member[index] == '"' || member[index] == '/'):
				quote = member[index]
			case quote == 0 && member[index] == '[':
				nested++
			case quote == 0 && member[index] == ']':
				nested--
			case quote == 0 && nested == 0 && member[index] == ',':
				parts = append(parts, member[start:index])
				start = index + 1
			}
		}
		if len(parts) > 0 {
			parts = append(parts, member[start:])
			selectors := make([]pathToken, len(parts))
			for index, part := range parts {
				if key, quoted := jsonPathUnquote(part); quoted {
					selectors[index] = pathToken{kind: 'f', key: key}
					continue
				}
				value, err := strconv.Atoi(part)
				if err != nil {
					return nil, false
				}
				selectors[index] = pathToken{kind: 'i', start: value}
			}
			tokens = append(tokens, pathToken{kind: 'u', selectors: selectors})
			continue
		}
		switch {
		case strings.HasPrefix(member, "?(") && strings.HasSuffix(member, ")"):
			rule, valid := jsonPathFilterRule(member[2 : len(member)-1])
			if !valid {
				return nil, false
			}
			tokens = append(tokens, pathToken{kind: 'q', filter: rule})
		case len(member) >= 2 && (member[0] == '\'' || member[0] == '"'):
			key, valid := jsonPathUnquote(member)
			if !valid {
				return nil, false
			}
			tokens = append(tokens, pathToken{kind: 'f', key: key})
		case member == "*":
			tokens = append(tokens, pathToken{kind: '*'})
		case strings.Contains(member, ":"):
			bounds := strings.Split(member, ":")
			if len(bounds) < 2 || len(bounds) > 3 {
				return nil, false
			}
			start, end, step := 0, int(^uint(0)>>1), 1
			var err error
			if bounds[0] != "" {
				start, err = strconv.Atoi(bounds[0])
				if err != nil {
					return nil, false
				}
			}
			if bounds[1] != "" {
				end, err = strconv.Atoi(bounds[1])
				if err != nil {
					return nil, false
				}
			}
			if len(bounds) == 3 && bounds[2] != "" {
				step, err = strconv.Atoi(bounds[2])
				if err != nil || step <= 0 {
					return nil, false
				}
			}
			tokens = append(tokens, pathToken{kind: 's', start: start, end: end, step: step})
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

func jsonPathFilterRule(expression string) (map[string]any, bool) {
	expression = strings.TrimSpace(expression)
	if len(expression) >= 2 && expression[0] == '(' && expression[len(expression)-1] == ')' {
		depth, quote, grouped := 0, byte(0), true
		for index := 0; index < len(expression); index++ {
			switch {
			case quote != 0 && expression[index] == '\\':
				index++
			case quote != 0 && expression[index] == quote:
				quote = 0
			case quote == 0 && (expression[index] == '\'' || expression[index] == '"' || expression[index] == '/'):
				quote = expression[index]
			case quote == 0 && expression[index] == '(':
				depth++
			case quote == 0 && expression[index] == ')':
				depth--
				if depth == 0 && index != len(expression)-1 {
					grouped = false
				}
			}
		}
		if grouped && depth == 0 {
			return jsonPathFilterRule(expression[1 : len(expression)-1])
		}
	}
	for _, logical := range []struct {
		operator, field string
	}{{"||", "Or"}, {"&&", "And"}} {
		var parts []string
		start, depth, quote := 0, 0, byte(0)
		for index := 0; index < len(expression); index++ {
			switch {
			case quote != 0 && expression[index] == '\\':
				index++
			case quote != 0 && expression[index] == quote:
				quote = 0
			case quote == 0 && (expression[index] == '\'' || expression[index] == '"' || expression[index] == '/'):
				quote = expression[index]
			case quote == 0 && expression[index] == '(':
				depth++
			case quote == 0 && expression[index] == ')':
				depth--
			case quote == 0 && depth == 0 && strings.HasPrefix(expression[index:], logical.operator):
				parts = append(parts, expression[start:index])
				index++
				start = index + 1
			}
		}
		if len(parts) > 0 {
			parts = append(parts, expression[start:])
			rules := make([]any, len(parts))
			for index, part := range parts {
				rule, valid := jsonPathFilterRule(part)
				if !valid {
					return nil, false
				}
				rules[index] = rule
			}
			return map[string]any{logical.field: rules}, true
		}
	}
	if strings.HasPrefix(expression, "!") {
		rule, valid := jsonPathFilterRule(expression[1:])
		if !valid {
			return nil, false
		}
		return map[string]any{"Not": rule}, true
	}
	operator, operatorAt, quote := "", -1, byte(0)
	space := func(character byte) bool {
		return character == ' ' || character == '\t' || character == '\n' || character == '\r'
	}
	for index := 0; index < len(expression); index++ {
		switch {
		case quote != 0 && expression[index] == '\\':
			index++
		case quote != 0 && expression[index] == quote:
			quote = 0
		case quote == 0 && (expression[index] == '\'' || expression[index] == '"' || expression[index] == '/'):
			quote = expression[index]
		case quote == 0:
			for _, candidate := range []string{"subsetof", "contains", "noneof", "anyof", "empty", "size", "all", "nin", "in", "===", "!==", "==", "!=", "<=", ">=", "=~", "<", ">"} {
				lexical := candidate[0] >= 'a' && candidate[0] <= 'z'
				if lexical && (index == 0 || !space(expression[index-1]) || index+len(candidate) >= len(expression) || !space(expression[index+len(candidate)])) {
					continue
				}
				if len(expression)-index >= len(candidate) && (lexical && strings.EqualFold(expression[index:index+len(candidate)], candidate) || !lexical && strings.HasPrefix(expression[index:], candidate)) {
					operator, operatorAt = candidate, index
					break
				}
			}
			if operatorAt >= 0 {
				index = len(expression)
			}
		}
	}
	filterPath := func(raw string) (string, bool) {
		if strings.HasPrefix(raw, "@") {
			path := "$" + raw[1:]
			return path, validJSONPath(path, true)
		}
		path := raw
		if !validJSONPath(path, true) || strings.HasPrefix(path, "$$") {
			return "", false
		}
		if path == "$" || strings.HasPrefix(path, "$.") || strings.HasPrefix(path, "$[") {
			path = "$!" + path[1:]
		}
		return path, true
	}
	left := expression
	if operatorAt >= 0 {
		left = expression[:operatorAt]
	}
	left = strings.TrimSpace(left)
	left, valid := filterPath(left)
	if operatorAt < 0 {
		if !valid {
			return nil, false
		}
		return map[string]any{"Variable": left, "IsPresent": true}, true
	}
	rawRight := strings.TrimSpace(expression[operatorAt+len(operator):])
	if !valid {
		left, valid = filterPath(rawRight)
		if !valid {
			return nil, false
		}
		rawRight = strings.TrimSpace(expression[:operatorAt])
		operator = map[string]string{"<": ">", "<=": ">=", ">": "<", ">=": "<=", "==": "==", "!=": "!=", "===": "===", "!==": "!=="}[operator]
	}
	if operator == "===" {
		operator = "=="
	} else if operator == "!==" {
		operator = "!="
	}
	if operator == "=~" {
		end, escaped := 1, false
		for end < len(rawRight) && (rawRight[end] != '/' || escaped) {
			if escaped {
				escaped = false
			} else {
				escaped = rawRight[end] == '\\'
			}
			end++
		}
		if len(rawRight) < 2 || rawRight[0] != '/' || end == len(rawRight) {
			return nil, false
		}
		modifiers, comments, unicodeClasses := "", false, false
		for _, flag := range rawRight[end+1:] {
			switch flag {
			case 'i', 'm', 's':
				modifiers += string(flag)
			case 'x':
				comments = true
			case 'U':
				unicodeClasses = true
			case 'd', 'u':
			default:
				return nil, false
			}
		}
		if modifiers != "" {
			modifiers = "(?" + modifiers + ")"
		}
		expression := strings.ReplaceAll(rawRight[1:end], `\/`, "/")
		if comments {
			expression = jsonPathRegexComments(expression)
		}
		if unicodeClasses {
			expression = jsonPathRegexUnicodeClasses(expression)
		}
		pattern, err := regexp.Compile(modifiers + "^(?:" + expression + ")$")
		if err != nil {
			return nil, false
		}
		return map[string]any{"Variable": left, "StringRegex": pattern}, true
	}
	if slices.Contains([]string{"in", "nin", "contains", "all", "subsetof", "anyof", "noneof"}, operator) {
		rule := map[string]any{"Variable": left, "CollectionOperator": operator}
		if rightPath, pathOperand := filterPath(rawRight); pathOperand {
			rule["CollectionPath"] = rightPath
		} else if right, valid := jsonPathFilterLiteral(rawRight); valid {
			rule["CollectionValue"] = right
		} else {
			return nil, false
		}
		return rule, true
	}
	if operator == "size" || operator == "empty" {
		rule := map[string]any{"Variable": left, "ValueOperator": operator}
		if rightPath, pathOperand := filterPath(rawRight); pathOperand {
			rule["ValuePath"] = rightPath
		} else if right, valid := jsonPathFilterLiteral(rawRight); valid && (operator == "size" && isNumber(right) || operator == "empty" && isBool(right)) {
			rule["Value"] = right
		} else {
			return nil, false
		}
		return rule, true
	}
	suffix := map[string]string{"==": "Equals", "<": "LessThan", "<=": "LessThanEquals", ">": "GreaterThan", ">=": "GreaterThanEquals"}[operator]
	if rightPath, pathOperand := filterPath(rawRight); pathOperand {
		prefixes := []string{"String", "Numeric", "Timestamp"}
		if operator == "==" || operator == "!=" {
			prefixes = append(prefixes, "Boolean")
		}
		rules := make([]any, len(prefixes))
		for index, prefix := range prefixes {
			rules[index] = map[string]any{"Variable": left, prefix + "EqualsPath": rightPath}
			if suffix != "" {
				rules[index] = map[string]any{"Variable": left, prefix + suffix + "Path": rightPath}
			}
		}
		rule := map[string]any{"Or": rules}
		if operator == "!=" {
			return map[string]any{"Not": rule}, true
		}
		if suffix == "" {
			return nil, false
		}
		return rule, true
	}
	right, valid := jsonPathFilterLiteral(rawRight)
	if !valid {
		return nil, false
	}
	if right == nil {
		if operator != "==" && operator != "!=" {
			return nil, false
		}
		rule := map[string]any{"Variable": left, "IsNull": true}
		if operator == "!=" {
			return map[string]any{"Not": rule}, true
		}
		return rule, true
	}
	prefix := ""
	switch right.(type) {
	case string:
		prefix = "String"
	case float64:
		prefix = "Numeric"
	case bool:
		if operator != "==" && operator != "!=" {
			return nil, false
		}
		prefix = "Boolean"
	default:
		return nil, false
	}
	if operator == "!=" {
		return map[string]any{"Not": map[string]any{"Variable": left, prefix + "Equals": right}}, true
	}
	if suffix == "" {
		return nil, false
	}
	return map[string]any{"Variable": left, prefix + suffix: right}, true
}

func jsonPathRegexComments(pattern string) string {
	var result strings.Builder
	escaped, comment := false, false
	for _, character := range pattern {
		if comment {
			if character == '\n' || character == '\r' {
				comment = false
			}
			continue
		}
		if escaped {
			if !unicode.IsSpace(character) && character != '#' {
				result.WriteByte('\\')
			}
			result.WriteRune(character)
			escaped = false
			continue
		}
		if character == '\\' {
			escaped = true
			continue
		}
		if character == '#' {
			comment = true
			continue
		}
		if !unicode.IsSpace(character) {
			result.WriteRune(character)
		}
	}
	if escaped {
		result.WriteByte('\\')
	}
	return result.String()
}

func jsonPathRegexUnicodeClasses(pattern string) string {
	// ponytail: Go has no Java regex mode; extend this translation if Jayway-only Unicode properties or boundaries are needed.
	const space = `\x{0009}-\x{000D}\x{0020}\x{0085}\x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}`
	const word = `\pL\pM\p{Nd}\p{Pc}\x{200C}\x{200D}`
	replacements := map[byte]string{
		'd': `\p{Nd}`, 'D': `\P{Nd}`,
		's': "[" + space + "]", 'S': "[^" + space + "]",
		'w': "[" + word + "]", 'W': "[^" + word + "]",
	}
	var result strings.Builder
	inClass := false
	for index := 0; index < len(pattern); index++ {
		if pattern[index] != '\\' || index+1 == len(pattern) {
			result.WriteByte(pattern[index])
			if pattern[index] == '[' {
				inClass = true
			} else if pattern[index] == ']' {
				inClass = false
			}
			continue
		}
		if replacement, exists := replacements[pattern[index+1]]; exists {
			if inClass && strings.ContainsRune("dsw", rune(pattern[index+1])) {
				replacement = map[byte]string{'d': `\p{Nd}`, 's': space, 'w': word}[pattern[index+1]]
			}
			result.WriteString(replacement)
			index++
			continue
		}
		result.WriteByte(pattern[index])
		index++
		result.WriteByte(pattern[index])
	}
	return result.String()
}

func jsonPathFilterLiteral(raw string) (any, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 && (raw[0] == '\'' || raw[0] == '"') {
		return jsonPathUnquote(raw)
	}
	if len(raw) >= 2 && raw[0] == '[' && raw[len(raw)-1] == ']' {
		if strings.TrimSpace(raw[1:len(raw)-1]) == "" {
			return []any{}, true
		}
		var values []any
		start, nested, quote := 1, 0, byte(0)
		for index := 1; index < len(raw)-1; index++ {
			switch {
			case quote != 0 && raw[index] == '\\':
				index++
			case quote != 0 && raw[index] == quote:
				quote = 0
			case quote == 0 && (raw[index] == '\'' || raw[index] == '"'):
				quote = raw[index]
			case quote == 0 && raw[index] == '[':
				nested++
			case quote == 0 && raw[index] == ']':
				nested--
			case quote == 0 && nested == 0 && raw[index] == ',':
				value, valid := jsonPathFilterLiteral(raw[start:index])
				if !valid {
					return nil, false
				}
				values = append(values, value)
				start = index + 1
			}
		}
		value, valid := jsonPathFilterLiteral(raw[start : len(raw)-1])
		return append(values, value), valid
	}
	var value any
	if json.Unmarshal([]byte(raw), &value) != nil {
		return nil, false
	}
	return value, true
}

func jsonPathFilterCollection(operator string, left, right any) bool {
	rightValues, rightOK := right.([]any)
	equal := func(left, right any) bool {
		leftNumber, leftOK := choiceNumber(left)
		rightNumber, rightOK := choiceNumber(right)
		return leftOK && rightOK && leftNumber == rightNumber || reflect.DeepEqual(left, right)
	}
	contains := func(values []any, value any) bool {
		return slices.ContainsFunc(values, func(candidate any) bool { return equal(value, candidate) })
	}
	if operator == "in" || operator == "nin" {
		found := rightOK && contains(rightValues, left)
		return found != (operator == "nin")
	}
	if operator == "contains" {
		if leftString, ok := left.(string); ok {
			rightString, ok := right.(string)
			return ok && strings.Contains(leftString, rightString)
		}
		leftValues, ok := left.([]any)
		return ok && contains(leftValues, right)
	}
	leftValues, leftOK := left.([]any)
	if !leftOK || !rightOK {
		return false
	}
	switch operator {
	case "all":
		return !slices.ContainsFunc(rightValues, func(value any) bool { return !contains(leftValues, value) })
	case "subsetof":
		return !slices.ContainsFunc(leftValues, func(value any) bool { return !contains(rightValues, value) })
	case "anyof":
		return slices.ContainsFunc(leftValues, func(value any) bool { return contains(rightValues, value) })
	case "noneof":
		return !slices.ContainsFunc(leftValues, func(value any) bool { return contains(rightValues, value) })
	}
	return false
}

func jsonPathFilterValue(operator string, left, right any) bool {
	length := -1
	switch left := left.(type) {
	case string:
		length = len(utf16.Encode([]rune(left)))
	case []any:
		length = len(left)
	case map[string]any:
		length = len(left)
	}
	if length < 0 {
		return false
	}
	if operator == "empty" {
		empty, valid := right.(bool)
		return valid && (length == 0) == empty
	}
	size, valid := choiceNumber(right)
	return operator == "size" && valid && length == int(size)
}

func validJSONPath(path string, reference bool) bool {
	if !strings.HasPrefix(path, "$") {
		return false
	}
	root := 1
	if strings.HasPrefix(path, "$$") {
		root = 2
	} else if len(path) > 1 && path[1] != '.' && path[1] != '[' {
		root = strings.IndexAny(path, ".[")
		if root < 0 {
			root = len(path)
		}
		if !validVariableName(path[1:root]) {
			return false
		}
	}
	if root == len(path) {
		return true
	}
	tokens, valid := jsonPathTokens(path[root:])
	if !valid || len(tokens) == 0 {
		return false
	}
	if reference {
		for _, token := range tokens {
			if token.kind != 'f' && token.kind != 'i' {
				return false
			}
		}
	}
	return true
}

func validResultPath(path string) bool {
	return validJSONPath(path, true) && (len(path) == 1 || path[1] == '.' || path[1] == '[')
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

func validStatePayload(value any) bool {
	encoded, err := json.Marshal(value)
	return err == nil && len(encoded) <= 256*1024
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
	validateMachine(machine, "", typeName, &diagnostics, map[string]struct{}{})
	return diagnostics
}

func validateMachine(machine map[string]any, location, machineType string, diagnostics *[]map[string]any, labels map[string]struct{}, inheritedVariables ...map[string]struct{}) {
	start, _ := machine["StartAt"].(string)
	states, ok := machine["States"].(map[string]any)
	add := func(code, message, path string) {
		*diagnostics = append(*diagnostics, map[string]any{"severity": "ERROR", "code": code, "message": message, "location": location + path})
	}
	if comment, exists := machine["Comment"]; exists {
		if _, valid := comment.(string); !valid {
			add("SCHEMA_VALIDATION_FAILED", "Comment must be a string.", "/Comment")
		}
	}
	if version, exists := machine["Version"]; exists {
		if value, valid := version.(string); !valid || value != "1.0" {
			add("SCHEMA_VALIDATION_FAILED", "Version must be 1.0.", "/Version")
		}
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
	if value, exists := machine["TimeoutSeconds"]; location == "" && exists {
		seconds, numeric := exactNumber(value)
		if !numeric || seconds != math.Trunc(seconds) || seconds < 1 || seconds > 99999999 {
			add("SCHEMA_VALIDATION_FAILED", "TimeoutSeconds must be a positive integer.", "/TimeoutSeconds")
		}
	}
	outerVariables := map[string]struct{}{}
	if len(inheritedVariables) != 0 {
		outerVariables = inheritedVariables[0]
	}
	scopeVariables := map[string]struct{}{}
	for stateName, raw := range states {
		state, _ := raw.(map[string]any)
		stateQueryLanguage := first(state, "QueryLanguage")
		if stateQueryLanguage == "" {
			stateQueryLanguage = queryLanguage
		}
		owners := []any{state}
		owners = append(owners, asSlice(state["Choices"])...)
		owners = append(owners, asSlice(state["Catch"])...)
		for _, rawOwner := range owners {
			owner, _ := rawOwner.(map[string]any)
			assign, _ := owner["Assign"].(map[string]any)
			for rawVariable := range assign {
				variable := rawVariable
				if stateQueryLanguage != "JSONata" {
					variable = strings.TrimSuffix(variable, ".$")
				}
				if !validVariableName(variable) || variable == "states" {
					add("SCHEMA_VALIDATION_FAILED", "Assign contains an invalid variable name.", "/States/"+stateName+"/Assign/"+rawVariable)
				}
				if _, shadows := outerVariables[variable]; shadows {
					add("SCHEMA_VALIDATION_FAILED", "An inner scope cannot assign an outer variable.", "/States/"+stateName+"/Assign/"+rawVariable)
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
		if utf8.RuneCountInString(name) > 80 {
			add("SCHEMA_VALIDATION_FAILED", "State name must not exceed 80 characters.", "/States/"+name)
		}
		if comment, exists := state["Comment"]; exists {
			if _, valid := comment.(string); !valid {
				add("SCHEMA_VALIDATION_FAILED", "Comment must be a string.", "/States/"+name+"/Comment")
			}
		}
		typ, _ := state["Type"].(string)
		stateQueryLanguage := first(state, "QueryLanguage")
		if stateQueryLanguage == "" {
			stateQueryLanguage = queryLanguage
		} else if stateQueryLanguage != "JSONPath" && stateQueryLanguage != "JSONata" || queryLanguage == "JSONata" && stateQueryLanguage != "JSONata" {
			add("SCHEMA_VALIDATION_FAILED", "State QueryLanguage is not compatible with the state machine.", "/States/"+name+"/QueryLanguage")
		}
		isJSONata := stateQueryLanguage == "JSONata"
		validateAssign := func(owner map[string]any, path string) {
			if value, exists := owner["Assign"]; exists {
				if _, valid := value.(map[string]any); !valid {
					add("SCHEMA_VALIDATION_FAILED", "Assign must be an object.", path+"/Assign")
				}
			}
		}
		validateAssign(state, "/States/"+name)
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
		if !isJSONata {
			for _, field := range []string{"InputPath", "OutputPath", "ResultPath"} {
				if value, exists := state[field]; exists && value != nil {
					path, valid := value.(string)
					if !valid || field == "ResultPath" && !validResultPath(path) || field != "ResultPath" && !validJSONPath(path, false) {
						add("SCHEMA_VALIDATION_FAILED", field+" must be null or a path.", "/States/"+name+"/"+field)
					}
				}
			}
			if value, exists := state["Parameters"]; exists {
				if _, valid := value.(map[string]any); !valid {
					add("SCHEMA_VALIDATION_FAILED", "Parameters must be an object.", "/States/"+name+"/Parameters")
				}
			}
			if value, exists := state["ResultSelector"]; exists {
				_, valid := value.(map[string]any)
				if !valid || typ != "Task" && typ != "Parallel" && typ != "Map" {
					add("SCHEMA_VALIDATION_FAILED", "ResultSelector must be an object on a Task, Parallel, or Map state.", "/States/"+name+"/ResultSelector")
				}
			}
		}
		for field, value := range state {
			if isJSONata {
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
		if typ == "Pass" {
			if isJSONata {
				for _, field := range []string{"Result"} {
					if _, exists := state[field]; exists {
						add("SCHEMA_VALIDATION_FAILED", field+" is not supported by a JSONata Pass state.", "/States/"+name+"/"+field)
					}
				}
			}
		}
		if typ != "Task" {
			for _, field := range []string{"Arguments", "Credentials", "HeartbeatSeconds", "HeartbeatSecondsPath", "Resource", "TimeoutSeconds", "TimeoutSecondsPath"} {
				if _, exists := state[field]; exists {
					add("SCHEMA_VALIDATION_FAILED", field+" is only supported by Task states.", "/States/"+name+"/"+field)
				}
			}
		}
		for _, group := range []struct {
			owner  string
			fields []string
		}{
			{"Choice", []string{"Choices", "Default"}},
			{"Fail", []string{"Cause", "CausePath", "Error", "ErrorPath"}},
			{"Map", []string{"ItemBatcher", "ItemProcessor", "ItemReader", "Items", "ItemsPath", "ItemSelector", "Iterator", "Label", "MaxConcurrency", "MaxConcurrencyPath", "ResultWriter", "ToleratedFailureCount", "ToleratedFailureCountPath", "ToleratedFailurePercentage", "ToleratedFailurePercentagePath"}},
			{"Parallel", []string{"Branches"}},
			{"Pass", []string{"Result"}},
			{"Wait", []string{"Seconds", "SecondsPath", "Timestamp", "TimestampPath"}},
		} {
			if typ == group.owner {
				continue
			}
			for _, field := range group.fields {
				if _, exists := state[field]; exists {
					add("SCHEMA_VALIDATION_FAILED", field+" is only supported by "+group.owner+" states.", "/States/"+name+"/"+field)
				}
			}
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
			validFailPath := func(value any) bool {
				path, valid := value.(string)
				if !valid {
					return false
				}
				if strings.HasPrefix(path, "$") {
					return validJSONPath(path, true)
				}
				for _, intrinsic := range []string{"ArrayGetItem", "Base64Decode", "Base64Encode", "Format", "Hash", "JsonToString", "UUID"} {
					if strings.HasPrefix(path, "States."+intrinsic+"(") && strings.HasSuffix(path, ")") {
						return true
					}
				}
				return false
			}
			for _, field := range []string{"Error", "Cause"} {
				if value, exists := state[field]; exists {
					if _, valid := value.(string); !valid {
						add("SCHEMA_VALIDATION_FAILED", field+" must be a string.", "/States/"+name+"/"+field)
					}
				}
				if value, exists := state[field+"Path"]; exists && !validFailPath(value) {
					add("SCHEMA_VALIDATION_FAILED", field+"Path must be a reference path or supported intrinsic.", "/States/"+name+"/"+field+"Path")
				}
			}
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
		validateInteger := func(object map[string]any, location, field string, minimum, maximum float64) (float64, bool) {
			value, direct := object[field]
			pathValue, path := object[field+"Path"]
			if direct && path {
				add("SCHEMA_VALIDATION_FAILED", field+" and "+field+"Path are mutually exclusive.", location+"/"+field)
			}
			if reference, valid := pathValue.(string); path && (!valid || !validJSONPath(reference, true)) {
				add("SCHEMA_VALIDATION_FAILED", field+"Path must be a reference path.", location+"/"+field+"Path")
			}
			if !direct {
				return 0, false
			}
			if expression, stringValue := value.(string); isJSONata && stringValue && strings.HasPrefix(expression, "{%") && strings.HasSuffix(expression, "%}") {
				return 0, false
			}
			number, numeric := exactNumber(value)
			if !numeric || number != math.Trunc(number) || number < minimum || maximum > 0 && number > maximum {
				add("SCHEMA_VALIDATION_FAILED", field+" must be an integer in range.", location+"/"+field)
				return 0, false
			}
			return number, true
		}
		validateIntegerField := func(field string, minimum, maximum float64) (float64, bool) {
			return validateInteger(state, "/States/"+name, field, minimum, maximum)
		}
		if typ == "Wait" {
			validateIntegerField("Seconds", 0, 99999999)
			if value, exists := state["Timestamp"]; exists {
				expression, stringValue := value.(string)
				if !(isJSONata && stringValue && strings.HasPrefix(expression, "{%") && strings.HasSuffix(expression, "%}")) {
					_, validTimestamp := parseTimestamp(value)
					if !validTimestamp || !stringValue || !strings.Contains(expression, "T") || !strings.HasSuffix(expression, "Z") {
						add("SCHEMA_VALIDATION_FAILED", "Timestamp must be an RFC3339 UTC timestamp.", "/States/"+name+"/Timestamp")
					}
				}
			}
			if value, exists := state["TimestampPath"]; exists {
				reference, valid := value.(string)
				if !valid || !validJSONPath(reference, true) {
					add("SCHEMA_VALIDATION_FAILED", "TimestampPath must be a reference path.", "/States/"+name+"/TimestampPath")
				}
			}
		}
		if typ == "Task" {
			timeout, hasTimeout := validateIntegerField("TimeoutSeconds", 1, 99999999)
			heartbeat, hasHeartbeat := validateIntegerField("HeartbeatSeconds", 1, 99999999)
			maximumHeartbeat := 99999999.0
			if hasTimeout {
				maximumHeartbeat = timeout
			}
			if hasHeartbeat && heartbeat >= maximumHeartbeat {
				add("SCHEMA_VALIDATION_FAILED", "HeartbeatSeconds must be less than TimeoutSeconds.", "/States/"+name+"/HeartbeatSeconds")
			}
		}
		if typ == "Map" {
			validateIntegerField("MaxConcurrency", 0, math.MaxInt32)
			validateIntegerField("ToleratedFailureCount", 0, math.MaxInt32)
			validateIntegerField("ToleratedFailurePercentage", 0, 100)
		}
		if rawRetry, exists := state["Retry"]; exists {
			retriers, valid := rawRetry.([]any)
			if !valid || len(retriers) == 0 || typ != "Task" && typ != "Parallel" && typ != "Map" {
				add("SCHEMA_VALIDATION_FAILED", "Retry must be an array on a Task, Parallel, or Map state.", "/States/"+name+"/Retry")
			} else {
				for i, raw := range retriers {
					path := fmt.Sprintf("/States/%s/Retry/%d", name, i)
					retrier, object := raw.(map[string]any)
					errors, errorsValid := retrier["ErrorEquals"].([]any)
					if !object || !errorsValid || len(errors) == 0 {
						add("SCHEMA_VALIDATION_FAILED", "Retry ErrorEquals must be a non-empty array.", path+"/ErrorEquals")
						continue
					}
					for _, rawError := range errors {
						if _, valid := rawError.(string); !valid {
							add("SCHEMA_VALIDATION_FAILED", "Retry ErrorEquals entries must be strings.", path+"/ErrorEquals")
							break
						}
					}
					if slices.Contains(errors, any("States.ALL")) && (len(errors) != 1 || i != len(retriers)-1) {
						add("SCHEMA_VALIDATION_FAILED", "States.ALL must appear alone in the last retrier.", path+"/ErrorEquals")
					}
					for _, field := range []struct {
						name         string
						minimum, max float64
					}{{"IntervalSeconds", 1, 99999999}, {"MaxAttempts", 0, 99999999}, {"MaxDelaySeconds", 1, 31622400}} {
						if value, exists := retrier[field.name]; exists {
							number, numeric := exactNumber(value)
							if !numeric || number != math.Trunc(number) || number < field.minimum || number > field.max {
								add("SCHEMA_VALIDATION_FAILED", field.name+" must be an integer in range.", path+"/"+field.name)
							}
						}
					}
					if value, exists := retrier["BackoffRate"]; exists {
						backoff, numeric := exactNumber(value)
						if !numeric || backoff < 1 {
							add("SCHEMA_VALIDATION_FAILED", "BackoffRate must be at least 1.", path+"/BackoffRate")
						}
					}
					if raw, exists := retrier["JitterStrategy"]; exists {
						jitter, valid := raw.(string)
						if !valid || jitter != "FULL" && jitter != "NONE" {
							add("SCHEMA_VALIDATION_FAILED", "JitterStrategy must be FULL or NONE.", path+"/JitterStrategy")
						}
					}
				}
			}
		}
		if !supportedStateType(typ) {
			add("SCHEMA_VALIDATION_FAILED", "Unsupported state Type.", "/States/"+name+"/Type")
		}
		end, endValid := state["End"].(bool)
		_, hasEnd := state["End"]
		_, hasNext := state["Next"]
		if typ == "Succeed" || typ == "Fail" || typ == "Choice" {
			if hasEnd || hasNext {
				add("SCHEMA_VALIDATION_FAILED", "State type does not support Next or End.", "/States/"+name)
			}
		} else {
			if hasEnd == hasNext {
				add("SCHEMA_VALIDATION_FAILED", "State must have exactly one of Next or End.", "/States/"+name)
			}
			if hasEnd && (!endValid || !end) {
				add("SCHEMA_VALIDATION_FAILED", "End must be true.", "/States/"+name+"/End")
			}
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
		if rawCatch, exists := state["Catch"]; exists {
			catchers, valid := rawCatch.([]any)
			if !valid || len(catchers) == 0 || typ != "Task" && typ != "Parallel" && typ != "Map" {
				add("SCHEMA_VALIDATION_FAILED", "Catch must be a non-empty array on a Task, Parallel, or Map state.", "/States/"+name+"/Catch")
			}
			for i, raw := range catchers {
				path := fmt.Sprintf("/States/%s/Catch/%d", name, i)
				catcher, object := raw.(map[string]any)
				if !object {
					add("SCHEMA_VALIDATION_FAILED", "Catch entries must be objects.", path)
					continue
				}
				validateAssign(catcher, path)
				errors, errorsValid := catcher["ErrorEquals"].([]any)
				if !errorsValid || len(errors) == 0 {
					add("SCHEMA_VALIDATION_FAILED", "Catch ErrorEquals must be a non-empty array.", path+"/ErrorEquals")
				} else {
					for _, rawError := range errors {
						if _, valid := rawError.(string); !valid {
							add("SCHEMA_VALIDATION_FAILED", "Catch ErrorEquals entries must be strings.", path+"/ErrorEquals")
							break
						}
					}
					if slices.Contains(errors, any("States.ALL")) && (len(errors) != 1 || i != len(catchers)-1) {
						add("SCHEMA_VALIDATION_FAILED", "States.ALL must appear alone in the last catcher.", path+"/ErrorEquals")
					}
				}
				for field, value := range catcher {
					if jsonataReferences(value, "$states.errorOutput") && field != "Assign" && field != "Output" {
						add("SCHEMA_VALIDATION_FAILED", "$states.errorOutput is only available in Catch Assign or Output.", path+"/"+field)
					}
				}
				if value, resultPath := catcher["ResultPath"]; resultPath {
					if isJSONata {
						add("SCHEMA_VALIDATION_FAILED", "Catch ResultPath is not supported with JSONata.", path+"/ResultPath")
					} else if value != nil {
						reference, valid := value.(string)
						if !valid || !validResultPath(reference) {
							add("SCHEMA_VALIDATION_FAILED", "Catch ResultPath must be null or a path.", path+"/ResultPath")
						}
					}
				}
				if _, output := catcher["Output"]; output && !isJSONata {
					add("SCHEMA_VALIDATION_FAILED", "Catch Output requires JSONata.", path+"/Output")
				}
				checkTarget(catcher["Next"], fmt.Sprintf("/Catch/%d/Next", i))
			}
		}
		if typ == "Choice" {
			choices := asSlice(state["Choices"])
			if len(choices) == 0 {
				add("MISSING_REQUIRED_FIELD", "Choice must have Choices.", "/States/"+name+"/Choices")
			}
			operators := []string{"And", "Or", "Not", "IsBoolean", "IsNull", "IsNumeric", "IsPresent", "IsString", "IsTimestamp", "StringMatches", "BooleanEquals", "BooleanEqualsPath"}
			for _, prefix := range []string{"String", "Numeric", "Timestamp"} {
				for _, suffix := range []string{"Equals", "LessThan", "LessThanEquals", "GreaterThan", "GreaterThanEquals"} {
					operators = append(operators, prefix+suffix, prefix+suffix+"Path")
				}
			}
			var validateChoiceRule func(any, bool, string)
			validateChoiceRule = func(raw any, topLevel bool, path string) {
				choice, object := raw.(map[string]any)
				if !object {
					add("SCHEMA_VALIDATION_FAILED", "Choice rule must be an object.", path)
					return
				}
				validateAssign(choice, path)
				if !topLevel {
					if _, exists := choice["Next"]; exists {
						add("SCHEMA_VALIDATION_FAILED", "Next is only supported by top-level Choice rules.", path+"/Next")
					}
				}
				found := make([]string, 0, 1)
				for _, operator := range operators {
					if _, exists := choice[operator]; exists {
						found = append(found, operator)
					}
				}
				if isJSONata {
					condition, exists := choice["Condition"]
					expression, stringCondition := condition.(string)
					_, booleanCondition := condition.(bool)
					if !exists || len(found) != 0 || !booleanCondition && (!stringCondition || !strings.HasPrefix(expression, "{%") || !strings.HasSuffix(expression, "%}")) {
						add("SCHEMA_VALIDATION_FAILED", "JSONata Choice rules require one boolean Condition.", path+"/Condition")
					}
					return
				}
				if _, exists := choice["Condition"]; exists || len(found) != 1 {
					add("SCHEMA_VALIDATION_FAILED", "JSONPath Choice rules require exactly one comparison operator.", path)
					return
				}
				operator, operand := found[0], choice[found[0]]
				if operator == "And" || operator == "Or" {
					rules, valid := operand.([]any)
					if !valid || len(rules) == 0 {
						add("SCHEMA_VALIDATION_FAILED", operator+" must contain Choice rules.", path+"/"+operator)
						return
					}
					for i, rule := range rules {
						validateChoiceRule(rule, false, fmt.Sprintf("%s/%s/%d", path, operator, i))
					}
					return
				}
				if operator == "Not" {
					validateChoiceRule(operand, false, path+"/Not")
					return
				}
				variable, validVariable := choice["Variable"].(string)
				validOperand := false
				switch {
				case strings.HasSuffix(operator, "Path"):
					reference, valid := operand.(string)
					validOperand = valid && validJSONPath(reference, false)
				case strings.HasPrefix(operator, "Numeric"):
					_, validOperand = exactNumber(operand)
				case strings.HasPrefix(operator, "Boolean"), strings.HasPrefix(operator, "Is"):
					_, validOperand = operand.(bool)
				case strings.HasPrefix(operator, "Timestamp"):
					timestamp, valid := operand.(string)
					_, validOperand = parseTimestamp(timestamp)
					validOperand = valid && validOperand && strings.Contains(timestamp, "T") && strings.HasSuffix(timestamp, "Z")
				default:
					_, validOperand = operand.(string)
				}
				if !validVariable || !validJSONPath(variable, true) || !validOperand {
					add("SCHEMA_VALIDATION_FAILED", "Choice comparison has an invalid Variable or operand.", path)
				}
			}
			for i, raw := range choices {
				choice, _ := raw.(map[string]any)
				validateChoiceRule(raw, true, fmt.Sprintf("/States/%s/Choices/%d", name, i))
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
				validateMachine(branch, fmt.Sprintf("%s/States/%s/Branches/%d", location, name, i), machineType, diagnostics, labels, visibleVariables)
			}
		}
		if typ == "Map" {
			for _, field := range []string{"ItemBatcher", "ItemReader", "ResultWriter"} {
				if value, exists := state[field]; exists {
					if _, valid := value.(map[string]any); !valid {
						add("SCHEMA_VALIDATION_FAILED", field+" must be an object.", "/States/"+name+"/"+field)
					}
				}
			}
			if value, exists := state["ItemsPath"]; exists {
				reference, valid := value.(string)
				if !valid || !validJSONPath(reference, true) {
					add("SCHEMA_VALIDATION_FAILED", "ItemsPath must be a reference path.", "/States/"+name+"/ItemsPath")
				}
			}
			if value, exists := state["ItemSelector"]; exists && !isJSONata {
				if _, valid := value.(map[string]any); !valid {
					add("SCHEMA_VALIDATION_FAILED", "ItemSelector must be an object.", "/States/"+name+"/ItemSelector")
				}
			}
			_, hasProcessor := state["ItemProcessor"]
			_, hasIterator := state["Iterator"]
			if hasProcessor && hasIterator {
				add("SCHEMA_VALIDATION_FAILED", "ItemProcessor and Iterator are mutually exclusive.", "/States/"+name)
			}
			if _, hasSelector := state["ItemSelector"]; hasSelector {
				if _, hasParameters := state["Parameters"]; hasParameters {
					add("SCHEMA_VALIDATION_FAILED", "ItemSelector and Parameters are mutually exclusive.", "/States/"+name)
				}
			}
			for _, field := range []string{"ItemReader", "ResultWriter"} {
				configuration, _ := state[field].(map[string]any)
				if configuration == nil {
					continue
				}
				resource, hasResource := configuration["Resource"]
				resourceName, validResource := resource.(string)
				_, hasWriterConfig := configuration["WriterConfig"]
				if field == "ItemReader" && !hasResource || field == "ResultWriter" && !hasResource && !hasWriterConfig {
					add("MISSING_REQUIRED_FIELD", field+" has no required configuration.", "/States/"+name+"/"+field+"/Resource")
				} else if hasResource && (!validResource || resourceName == "") {
					add("SCHEMA_VALIDATION_FAILED", field+" Resource must be a string.", "/States/"+name+"/"+field+"/Resource")
				}
				if _, parameters := configuration["Parameters"]; parameters && isJSONata {
					add("SCHEMA_VALIDATION_FAILED", field+" Parameters is not supported with JSONata.", "/States/"+name+"/"+field+"/Parameters")
				}
				if _, arguments := configuration["Arguments"]; arguments && !isJSONata {
					add("SCHEMA_VALIDATION_FAILED", field+" Arguments requires JSONata.", "/States/"+name+"/"+field+"/Arguments")
				}
				if value, parameters := configuration["Parameters"]; parameters && !isJSONata {
					if _, valid := value.(map[string]any); !valid {
						add("SCHEMA_VALIDATION_FAILED", field+" Parameters must be an object.", "/States/"+name+"/"+field+"/Parameters")
					}
				}
				if value, arguments := configuration["Arguments"]; arguments && isJSONata {
					if _, object := value.(map[string]any); !object {
						expression, valid := value.(string)
						if !valid || !strings.HasPrefix(expression, "{%") || !strings.HasSuffix(expression, "%}") {
							add("SCHEMA_VALIDATION_FAILED", field+" Arguments must be an object or JSONata expression.", "/States/"+name+"/"+field+"/Arguments")
						}
					}
				}
				if field == "ResultWriter" {
					writerConfig, configValid := configuration["WriterConfig"].(map[string]any)
					if hasWriterConfig && !configValid {
						add("SCHEMA_VALIDATION_FAILED", "WriterConfig must be an object.", "/States/"+name+"/ResultWriter/WriterConfig")
					}
					for option, allowed := range map[string][]string{"Transformation": {"NONE", "COMPACT", "FLATTEN"}, "OutputType": {"JSON", "JSONL"}} {
						if value, exists := writerConfig[option]; exists {
							configured, valid := value.(string)
							if !valid || !slices.Contains(allowed, configured) {
								add("SCHEMA_VALIDATION_FAILED", "WriterConfig "+option+" is invalid.", "/States/"+name+"/ResultWriter/WriterConfig/"+option)
							}
						}
					}
					if validResource && resourceName != "" {
						payload := "Parameters"
						if isJSONata {
							payload = "Arguments"
						}
						if resourceName != "arn:aws:states:::s3:putObject" {
							add("SCHEMA_VALIDATION_FAILED", "ResultWriter Resource is not supported.", "/States/"+name+"/ResultWriter/Resource")
						} else if _, exists := configuration[payload]; !exists {
							add("MISSING_REQUIRED_FIELD", "ResultWriter export requires "+payload+".", "/States/"+name+"/ResultWriter/"+payload)
						}
					}
				}
			}
			if reader, _ := state["ItemReader"].(map[string]any); reader != nil {
				config, valid := reader["ReaderConfig"].(map[string]any)
				if _, exists := reader["ReaderConfig"]; exists && !valid {
					add("SCHEMA_VALIDATION_FAILED", "ReaderConfig must be an object.", "/States/"+name+"/ItemReader/ReaderConfig")
				}
				readerPath := "/States/" + name + "/ItemReader/ReaderConfig"
				inputType := first(config, "InputType")
				if value, exists := config["InputType"]; exists {
					configured, valid := value.(string)
					if !valid || !slices.Contains([]string{"CSV", "JSON", "JSONL", "PARQUET", "MANIFEST"}, configured) {
						add("SCHEMA_VALIDATION_FAILED", "InputType is invalid.", readerPath+"/InputType")
					}
				}
				payloadName := "Parameters"
				if isJSONata {
					payloadName = "Arguments"
				}
				payload, _ := reader[payloadName].(map[string]any)
				if inputType == "PARQUET" {
					if _, versionID := payload["VersionId"]; versionID {
						add("SCHEMA_VALIDATION_FAILED", "Parquet ItemReader does not support VersionId.", "/States/"+name+"/ItemReader/"+payloadName+"/VersionId")
					}
					if _, versionID := payload["VersionId.$"]; versionID {
						add("SCHEMA_VALIDATION_FAILED", "Parquet ItemReader does not support VersionId.", "/States/"+name+"/ItemReader/"+payloadName+"/VersionId.$")
					}
				}
				if value, exists := config["ManifestType"]; exists {
					manifestType, valid := value.(string)
					resource := first(reader, "Resource")
					if !valid || resource != "arn:aws:states:::s3:getObject" || manifestType == "ATHENA_DATA" && !slices.Contains([]string{"CSV", "JSONL", "PARQUET"}, inputType) || manifestType == "S3_INVENTORY" && inputType != "" || manifestType != "ATHENA_DATA" && manifestType != "S3_INVENTORY" {
						add("SCHEMA_VALIDATION_FAILED", "ManifestType is invalid for this ItemReader.", readerPath+"/ManifestType")
					}
				}
				if value, exists := config["Transformation"]; exists {
					transformation, valid := value.(string)
					resource := first(reader, "Resource")
					if !valid || transformation != "NONE" && transformation != "LOAD_AND_FLATTEN" || resource != "arn:aws:states:::s3:listObjectsV2" || transformation == "LOAD_AND_FLATTEN" && !slices.Contains([]string{"CSV", "JSON", "JSONL", "PARQUET"}, inputType) {
						add("SCHEMA_VALIDATION_FAILED", "Transformation is invalid for this ItemReader.", readerPath+"/Transformation")
					}
				}
				csvConfigured := false
				for field, allowed := range map[string][]string{"CSVDelimiter": {"COMMA", "PIPE", "SEMICOLON", "SPACE", "TAB"}, "CSVHeaderLocation": {"FIRST_ROW", "GIVEN"}} {
					if value, exists := config[field]; exists {
						csvConfigured = true
						configured, valid := value.(string)
						if !valid || !slices.Contains(allowed, configured) {
							add("SCHEMA_VALIDATION_FAILED", field+" is invalid.", readerPath+"/"+field)
						}
					}
				}
				if rawHeaders, exists := config["CSVHeaders"]; exists {
					csvConfigured = true
					headers, valid := rawHeaders.([]any)
					size := len(headers) - 1
					for _, rawHeader := range headers {
						header, stringHeader := rawHeader.(string)
						valid = valid && stringHeader
						size += len(header)
					}
					if !valid || len(headers) == 0 || size > 10*1024 || first(config, "CSVHeaderLocation") != "GIVEN" {
						add("SCHEMA_VALIDATION_FAILED", "CSVHeaders requires non-empty string headers up to 10 KiB and CSVHeaderLocation GIVEN.", readerPath+"/CSVHeaders")
					}
				} else if first(config, "CSVHeaderLocation") == "GIVEN" {
					add("MISSING_REQUIRED_FIELD", "CSVHeaderLocation GIVEN requires CSVHeaders.", readerPath+"/CSVHeaders")
				}
				if csvConfigured && inputType != "CSV" && inputType != "MANIFEST" {
					add("SCHEMA_VALIDATION_FAILED", "CSV fields require CSV or MANIFEST input.", readerPath)
				}
				validateInteger(config, "/States/"+name+"/ItemReader/ReaderConfig", "MaxItems", 1, 100000000)
				if value, exists := config["ItemsPointer"]; exists {
					pointer, valid := value.(string)
					inputType := first(config, "InputType")
					_, validPointer := jsonPointerTokens(pointer)
					if !valid || !validPointer || utf8.RuneCountInString(pointer) >= 2000 || inputType != "" && inputType != "JSON" {
						add("SCHEMA_VALIDATION_FAILED", "ItemsPointer must be a JSON Pointer for JSON input.", "/States/"+name+"/ItemReader/ReaderConfig/ItemsPointer")
					}
				}
				if _, path := config["MaxItemsPath"]; path && isJSONata {
					add("SCHEMA_VALIDATION_FAILED", "MaxItemsPath is not supported with JSONata.", "/States/"+name+"/ItemReader/ReaderConfig/MaxItemsPath")
				}
			}
			if batcher, _ := state["ItemBatcher"].(map[string]any); batcher != nil {
				path := "/States/" + name + "/ItemBatcher"
				_, items := batcher["MaxItemsPerBatch"]
				_, itemsPath := batcher["MaxItemsPerBatchPath"]
				_, bytes := batcher["MaxInputBytesPerBatch"]
				_, bytesPath := batcher["MaxInputBytesPerBatchPath"]
				if !items && !itemsPath && !bytes && !bytesPath {
					add("MISSING_REQUIRED_FIELD", "ItemBatcher requires an item or byte limit.", path)
				}
				validateInteger(batcher, path, "MaxItemsPerBatch", 1, 0)
				validateInteger(batcher, path, "MaxInputBytesPerBatch", 1, 262144)
				if value, exists := batcher["BatchInput"]; exists && !isJSONata {
					if _, valid := value.(map[string]any); !valid {
						add("SCHEMA_VALIDATION_FAILED", "BatchInput must be an object.", path+"/BatchInput")
					}
				}
				if isJSONata {
					for _, field := range []string{"MaxItemsPerBatchPath", "MaxInputBytesPerBatchPath"} {
						if _, exists := batcher[field]; exists {
							add("SCHEMA_VALIDATION_FAILED", field+" is not supported with JSONata.", path+"/"+field)
						}
					}
				}
			}
			processor, _ := state["ItemProcessor"].(map[string]any)
			if processor == nil {
				processor, _ = state["Iterator"].(map[string]any)
			}
			if processor != nil {
				processorConfig, configValid := processor["ProcessorConfig"].(map[string]any)
				if _, exists := processor["ProcessorConfig"]; exists && !configValid {
					add("SCHEMA_VALIDATION_FAILED", "ProcessorConfig must be an object.", "/States/"+name+"/ItemProcessor/ProcessorConfig")
				}
				mode, executionType := first(processorConfig, "Mode"), first(processorConfig, "ExecutionType")
				if value, exists := processorConfig["Mode"]; exists {
					if _, valid := value.(string); !valid {
						add("SCHEMA_VALIDATION_FAILED", "ProcessorConfig Mode must be a string.", "/States/"+name+"/ItemProcessor/ProcessorConfig/Mode")
					}
				}
				if value, exists := processorConfig["ExecutionType"]; exists {
					if _, valid := value.(string); !valid {
						add("SCHEMA_VALIDATION_FAILED", "ExecutionType must be a string.", "/States/"+name+"/ItemProcessor/ProcessorConfig/ExecutionType")
					}
				}
				if mode != "" && mode != "INLINE" && mode != "DISTRIBUTED" {
					add("SCHEMA_VALIDATION_FAILED", "ProcessorConfig Mode must be INLINE or DISTRIBUTED.", "/States/"+name+"/ItemProcessor/ProcessorConfig/Mode")
				}
				if executionType != "" && (mode != "DISTRIBUTED" || executionType != "STANDARD" && executionType != "EXPRESS") {
					add("SCHEMA_VALIDATION_FAILED", "ExecutionType must be STANDARD or EXPRESS for a Distributed Map.", "/States/"+name+"/ItemProcessor/ProcessorConfig/ExecutionType")
				}
				if mode == "DISTRIBUTED" && machineType == "EXPRESS" {
					add("SCHEMA_VALIDATION_FAILED", "Express workflows do not support Distributed Map.", "/States/"+name+"/ItemProcessor/ProcessorConfig/Mode")
				}
				if mode != "DISTRIBUTED" {
					for _, field := range []string{"ItemBatcher", "ItemReader", "ResultWriter", "ToleratedFailureCount", "ToleratedFailureCountPath", "ToleratedFailurePercentage", "ToleratedFailurePercentagePath"} {
						if _, exists := state[field]; exists {
							add("SCHEMA_VALIDATION_FAILED", field+" requires a Distributed Map.", "/States/"+name+"/"+field)
						}
					}
				}
				if rawLabel, exists := state["Label"]; exists {
					label, valid := rawLabel.(string)
					invalidCharacter := strings.ContainsAny(label, `?*<>{}[]:;,\|^~$#%&`+"`\"") || slices.ContainsFunc([]rune(label), func(character rune) bool {
						return unicode.IsSpace(character) || unicode.IsControl(character)
					})
					if !valid || utf8.RuneCountInString(label) < 1 || utf8.RuneCountInString(label) > 40 || invalidCharacter {
						add("SCHEMA_VALIDATION_FAILED", "Label contains invalid characters or length.", "/States/"+name+"/Label")
					} else if _, duplicate := labels[label]; duplicate && mode == "DISTRIBUTED" {
						add("SCHEMA_VALIDATION_FAILED", "Label must be unique within the state machine definition.", "/States/"+name+"/Label")
					} else if mode == "DISTRIBUTED" {
						labels[label] = struct{}{}
					}
					if mode != "DISTRIBUTED" {
						add("SCHEMA_VALIDATION_FAILED", "Label requires a Distributed Map.", "/States/"+name+"/Label")
					}
				}
				if _, objectItems := state["Items"].(map[string]any); isJSONata && objectItems && mode != "DISTRIBUTED" {
					add("SCHEMA_VALIDATION_FAILED", "Inline Map Items must be an array.", "/States/"+name+"/Items")
				}
				if processor["QueryLanguage"] == nil {
					processor = maps.Clone(processor)
					processor["QueryLanguage"] = queryLanguage
				}
				validateMachine(processor, location+"/States/"+name+"/ItemProcessor", machineType, diagnostics, labels, visibleVariables)
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
		start := unicode.IsLetter(character) || unicode.Is(unicode.Nl, character) || unicode.Is(unicode.Other_ID_Start, character)
		continuation := start || unicode.Is(unicode.Mn, character) || unicode.Is(unicode.Mc, character) || unicode.Is(unicode.Nd, character) || unicode.Is(unicode.Pc, character) || unicode.Is(unicode.Other_ID_Continue, character)
		if index == 0 && !start || index > 0 && !continuation {
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
		jsonataMu.Lock()
		_, err := jsonata.Compile(expression, false)
		jsonataMu.Unlock()
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

func stateInteger(state map[string]any, field string, input any, scope *jsonataScope, minimum, maximum float64, variables ...map[string]any) (float64, bool, bool) {
	value, configured := state[field]
	path, hasPath := state[field+"Path"].(string)
	if !configured && !hasPath {
		return 0, false, true
	}
	if configured == hasPath {
		return 0, true, false
	}
	if scope != nil {
		var valid bool
		value, valid = evalJSONataValue(value, *scope)
		if !valid || hasPath {
			return 0, true, false
		}
	} else if hasPath {
		value = jsonPath(input, path, variables...)
	}
	number, numeric := exactNumber(value)
	return number, true, numeric && number == math.Trunc(number) && number >= minimum && number <= maximum
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
