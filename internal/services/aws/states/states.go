// Package states is Step Functions-lite: SM records plus a Pass/Succeed/Fail/Wait/Task/Choice/Parallel walker.
package states

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
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
		"StartExecution", "StartSyncExecution", "StopExecution", "DescribeExecution", "ListExecutions", "GetExecutionHistory",
		"CreateActivity", "DeleteActivity", "DescribeActivity", "ListActivities", "GetActivityTask",
		"SendTaskSuccess", "SendTaskFailure", "SendTaskHeartbeat",
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
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := p.smARN(req, name)
		rec := map[string]any{
			"stateMachineArn": arn, "name": name, "definition": first(req.Input, "definition", "Definition"),
			"roleArn": first(req.Input, "roleArn", "RoleArn"), "type": first(req.Input, "type", "Type"),
			"status": "ACTIVE", "creationDate": float64(now),
		}
		if rec["type"] == "" {
			rec["type"] = "STANDARD"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sm").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"stateMachineArn": arn, "creationDate": float64(now)}}, nil
	case "UpdateStateMachine":
		name := smName(first(req.Input, "stateMachineArn", "StateMachineArn"))
		b, ok, _ := p.col(req, "sm").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if d := first(req.Input, "definition", "Definition"); d != "" {
			rec["definition"] = d
		}
		if r := first(req.Input, "roleArn", "RoleArn"); r != "" {
			rec["roleArn"] = r
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "sm").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"updateDate": float64(now)}}, nil
	case "DeleteStateMachine":
		_ = p.col(req, "sm").Delete(ctx, smName(first(req.Input, "stateMachineArn", "StateMachineArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeStateMachine":
		name := smName(first(req.Input, "stateMachineArn", "StateMachineArn"))
		b, ok, _ := p.col(req, "sm").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListStateMachines":
		return listCol(ctx, p.col(req, "sm"), "stateMachines")
	case "StartExecution", "StartSyncExecution":
		arn := first(req.Input, "stateMachineArn", "StateMachineArn")
		name := smName(arn)
		b, ok, _ := p.col(req, "sm").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "StateMachineDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var sm map[string]any
		_ = json.Unmarshal(b, &sm)
		if req.Operation == "StartSyncExecution" && first(sm, "type", "Type") != "EXPRESS" {
			return nil, &spi.Fault{Code: "StateMachineTypeNotSupported", HTTPStatus: 400, Fault: "client"}
		}
		exName := first(req.Input, "name", "Name")
		if exName == "" {
			exName = p.deps.Rand.Hex(8)
		}
		in := parseJSON(first(req.Input, "input", "Input"))
		def, _ := sm["definition"].(string)
		wr := p.walk(ctx, req, def, "", in)
		execARN := p.execARN(req, name, exName)
		ob, _ := json.Marshal(wr.out)
		rec := map[string]any{
			"executionArn": execARN, "stateMachineArn": arn, "name": exName, "status": wr.status,
			"startDate": float64(now), "input": first(req.Input, "input", "Input"),
			"output": string(ob), "cause": wr.cause, "history": wr.hist, "definition": def,
		}
		if wr.errorName != "" {
			rec["error"] = wr.errorName
		}
		if wr.status != "RUNNING" {
			rec["stopDate"] = float64(now)
		}
		if wr.pending != nil {
			wr.pending.ExecName = exName
			wr.pending.Definition = def
			pb, _ := json.Marshal(wr.pending)
			_ = p.col(req, "pending").Put(ctx, wr.pending.Token, pb)
			rec["pendingToken"] = wr.pending.Token
		}
		eb, _ := json.Marshal(rec)
		_ = p.col(req, "ex").Put(ctx, exName, eb)
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
		ex := execName(first(req.Input, "executionArn", "ExecutionArn"))
		b, ok, _ := p.col(req, "ex").Get(ctx, ex)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		rec["status"] = "ABORTED"
		rec["stopDate"] = float64(now)
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ex").Put(ctx, ex, nb)
		return &spi.Response{Output: map[string]any{"stopDate": float64(now)}}, nil
	case "DescribeExecution":
		ex := execName(first(req.Input, "executionArn", "ExecutionArn"))
		b, ok, _ := p.col(req, "ex").Get(ctx, ex)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		delete(rec, "history")
		return &spi.Response{Output: rec}, nil
	case "ListExecutions":
		return listCol(ctx, p.col(req, "ex"), "executions")
	case "GetExecutionHistory":
		ex := execName(first(req.Input, "executionArn", "ExecutionArn"))
		b, ok, _ := p.col(req, "ex").Get(ctx, ex)
		if !ok {
			return nil, &spi.Fault{Code: "ExecutionDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		hist := rec["history"]
		if hist == nil {
			hist = []any{}
		}
		return &spi.Response{Output: map[string]any{"events": hist}}, nil
	case "CreateActivity":
		name := first(req.Input, "name", "Name")
		arn := "arn:aws:states:" + req.Identity.Region + ":" + req.Identity.Account + ":activity:" + name
		rec := map[string]any{"activityArn": arn, "name": name, "creationDate": float64(now)}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "act").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "DeleteActivity":
		_ = p.col(req, "act").Delete(ctx, actName(first(req.Input, "activityArn", "ActivityArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeActivity":
		name := actName(first(req.Input, "activityArn", "ActivityArn"))
		b, ok, _ := p.col(req, "act").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ActivityDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListActivities":
		return listCol(ctx, p.col(req, "act"), "activities")
	case "GetActivityTask":
		want := first(req.Input, "activityArn", "ActivityArn")
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
		if _, found, _ := p.col(req, "pending").Get(ctx, first(req.Input, "taskToken", "TaskToken")); !found {
			return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "TagResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		var tags []any
		if b, ok, _ := p.col(req, "tag").Get(ctx, arn); ok {
			_ = json.Unmarshal(b, &tags)
		}
		indexes := map[string]int{}
		for i, tag := range tags {
			m, _ := tag.(map[string]any)
			indexes[first(m, "key", "Key")] = i
		}
		for _, tag := range asSlice(req.Input["tags"]) {
			m, _ := tag.(map[string]any)
			key := first(m, "key", "Key")
			if i, ok := indexes[key]; ok {
				tags[i] = tag
			} else {
				indexes[key] = len(tags)
				tags = append(tags, tag)
			}
		}
		b, _ := json.Marshal(tags)
		_ = p.col(req, "tag").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		var tags []any
		if b, ok, _ := p.col(req, "tag").Get(ctx, arn); ok {
			_ = json.Unmarshal(b, &tags)
		}
		drop := map[string]bool{}
		for _, key := range asSlice(req.Input["tagKeys"]) {
			drop[fmt.Sprint(key)] = true
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
		b, ok, _ := p.col(req, "tag").Get(ctx, first(req.Input, "resourceArn", "ResourceArn"))
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

type pending struct {
	Token, ActivityARN, StateName, ExecName, Definition string
	Input, StateInput                                   any
	Retries                                             map[int]int
}

type walkResult struct {
	out           any
	status, cause string
	errorName     string
	hist          []any
	pending       *pending
}

func (p *Pack) finishTask(ctx context.Context, req *spi.Request, now int64, ok bool) (*spi.Response, error) {
	tok := first(req.Input, "taskToken", "TaskToken")
	b, found, _ := p.col(req, "pending").Get(ctx, tok)
	if !found {
		return nil, &spi.Fault{Code: "InvalidToken", HTTPStatus: 400, Fault: "client"}
	}
	var pend pending
	_ = json.Unmarshal(b, &pend)
	_ = p.col(req, "pending").Delete(ctx, tok)
	exb, eok, _ := p.col(req, "ex").Get(ctx, pend.ExecName)
	if !eok {
		return &spi.Response{Output: map[string]any{}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(exb, &rec)
	data := parseJSON(first(req.Input, "output", "Output"))
	from, definition := pend.StateName, pend.Definition
	var sm map[string]any
	_ = json.Unmarshal([]byte(pend.Definition), &sm)
	states, _ := sm["States"].(map[string]any)
	st, _ := states[pend.StateName].(map[string]any)
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
			pend.Token = p.deps.Rand.Hex(16)
			pb, _ := json.Marshal(pend)
			_ = p.col(req, "pending").Put(ctx, pend.Token, pb)
			rec["pendingToken"] = pend.Token
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "ex").Put(ctx, pend.ExecName, nb)
			return &spi.Response{Output: map[string]any{}}, nil
		}
		next, out, caught := catchTask(st, failure, pend.StateInput)
		if caught && next == "" {
			failure = stateFailure{name: "States.Runtime", cause: "States.Runtime"}
		}
		if !caught || next == "" {
			rec["status"], rec["error"], rec["cause"], rec["stopDate"] = "FAILED", failure.name, failure.cause, float64(now)
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "ex").Put(ctx, pend.ExecName, nb)
			return &spi.Response{Output: map[string]any{}}, nil
		}
		sm["StartAt"] = next
		def, _ := json.Marshal(sm)
		definition, from, data = string(def), "", out
	} else if out, valid := applyResultPath(st, pend.StateInput, data); valid {
		data = out
	} else {
		rec["status"], rec["error"], rec["cause"], rec["stopDate"] = "FAILED", "States.Runtime", "States.Runtime", float64(now)
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ex").Put(ctx, pend.ExecName, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	}
	wr := p.walk(ctx, req, definition, from, data)
	ob, _ := json.Marshal(wr.out)
	rec["status"] = wr.status
	rec["output"] = string(ob)
	rec["cause"] = wr.cause
	rec["history"] = wr.hist
	if wr.errorName != "" {
		rec["error"] = wr.errorName
	} else {
		delete(rec, "error")
	}
	if wr.status != "RUNNING" {
		rec["stopDate"] = float64(now)
	}
	if wr.pending != nil {
		wr.pending.ExecName = pend.ExecName
		wr.pending.Definition = pend.Definition
		pb, _ := json.Marshal(wr.pending)
		_ = p.col(req, "pending").Put(ctx, wr.pending.Token, pb)
		rec["pendingToken"] = wr.pending.Token
	} else {
		delete(rec, "pendingToken")
	}
	nb, _ := json.Marshal(rec)
	_ = p.col(req, "ex").Put(ctx, pend.ExecName, nb)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) walk(ctx context.Context, req *spi.Request, def, from string, input any) walkResult {
	var sm map[string]any
	if err := json.Unmarshal([]byte(def), &sm); err != nil {
		return walkResult{out: input, status: "FAILED", cause: "InvalidDefinition"}
	}
	start, _ := sm["StartAt"].(string)
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
	retries := map[string]map[int]int{}
walkLoop:
	for hop := 0; hop < 64; hop++ {
		st, ok := states[cur].(map[string]any)
		if !ok {
			return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
		}
		typ, _ := st["Type"].(string)
		hist = append(hist, map[string]any{"type": typ + "StateEntered", "id": hop + 1, "name": cur})
		switch typ {
		case "Pass":
			result := data
			if params, ok := st["Parameters"].(map[string]any); ok {
				result = applyParams(params, data, nil)
			} else if value, ok := st["Result"]; ok {
				result = value
			}
			var valid bool
			data, valid = applyResultPath(st, data, result)
			if !valid {
				return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
			}
		case "Succeed":
			return walkResult{out: data, status: "SUCCEEDED", hist: hist}
		case "Fail":
			cause, _ := st["Cause"].(string)
			errorName, _ := st["Error"].(string)
			if errorName == "" {
				errorName = "States.TaskFailed"
			}
			if cause == "" {
				cause = errorName
			}
			return walkResult{out: data, status: "FAILED", cause: cause, errorName: errorName, hist: hist}
		case "Wait":
		case "Task":
			res := first(st, "Resource")
			payload := taskPayload(st, data)
			switch {
			case strings.Contains(res, ":function:") || strings.Contains(res, "lambda:invoke"):
				if retries[cur] == nil {
					retries[cur] = map[int]int{}
				}
				for {
					out, err := p.invokeLambda(ctx, req, res, st, payload)
					if err == nil {
						var valid bool
						data, valid = applyResultPath(st, data, out)
						if !valid {
							return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
						}
						break
					}
					failure := taskFailure(err)
					// ponytail: retry delays are instant like Wait states; add virtual scheduling when timing fidelity is required.
					if retryTask(st, failure.name, retries[cur]) {
						continue
					}
					next, out, caught := catchTask(st, failure, data)
					if !caught {
						return walkResult{out: data, status: "FAILED", cause: failure.cause, errorName: failure.name, hist: hist}
					}
					if next == "" {
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
					data = out
					cur = next
					continue walkLoop
				}
			case strings.Contains(res, ":activity:"):
				tok := p.deps.Rand.Hex(16)
				return walkResult{out: data, status: "RUNNING", hist: hist, pending: &pending{
					Token: tok, ActivityARN: res, StateName: cur, Input: payload, StateInput: data, Retries: retries[cur],
				}}
			default:
				return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
			}
		case "Choice":
			next := choiceNext(st, data)
			if next == "" {
				return walkResult{out: data, status: "FAILED", cause: "States.NoChoiceMatched", hist: hist}
			}
			cur = next
			continue
		case "Parallel":
			if retries[cur] == nil {
				retries[cur] = map[int]int{}
			}
			for {
				branches, _ := st["Branches"].([]any)
				var results []any
				var failed *walkResult
				for _, br := range branches {
					bm, _ := br.(map[string]any)
					bdef, _ := json.Marshal(bm)
					wr := p.walk(ctx, req, string(bdef), "", data)
					if wr.status != "SUCCEEDED" {
						failed = &wr
						break
					}
					results = append(results, wr.out)
				}
				if failed == nil {
					data = results
					break
				}
				next, out, retry, caught := recoverState(st, *failed, data, retries[cur])
				if retry {
					continue
				}
				if caught {
					if next == "" {
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
					data, cur = out, next
					continue walkLoop
				}
				failed.hist = hist
				return *failed
			}
		case "Map":
			path := first(st, "ItemsPath")
			var items any
			if path == "" {
				items = data
			} else {
				items = jsonPath(data, path)
			}
			arr, ok := items.([]any)
			if !ok {
				return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
			}
			iter, _ := st["Iterator"].(map[string]any)
			if iter == nil {
				iter, _ = st["ItemProcessor"].(map[string]any)
			}
			idef, _ := json.Marshal(iter)
			selector, _ := st["ItemSelector"].(map[string]any)
			if selector == nil {
				selector, _ = st["Parameters"].(map[string]any)
			}
			if retries[cur] == nil {
				retries[cur] = map[int]int{}
			}
			for {
				var results []any
				var failed *walkResult
				for index, item := range arr {
					iterationInput := item
					if selector != nil {
						iterationInput = applyParams(selector, data, map[string]any{"Map": map[string]any{"Item": map[string]any{
							"Index": float64(index), "Value": item, "Source": "STATE_DATA",
						}}})
					}
					wr := p.walk(ctx, req, string(idef), "", iterationInput)
					if wr.status != "SUCCEEDED" {
						failed = &wr
						break
					}
					results = append(results, wr.out)
				}
				if failed == nil {
					if results == nil {
						results = []any{}
					}
					data = results
					break
				}
				next, out, retry, caught := recoverState(st, *failed, data, retries[cur])
				if retry {
					continue
				}
				if caught {
					if next == "" {
						return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
					}
					data, cur = out, next
					continue walkLoop
				}
				failed.hist = hist
				return *failed
			}
		default:
			return walkResult{out: data, status: "FAILED", cause: "States.Runtime", hist: hist}
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

func taskFailure(err error) stateFailure {
	var fault *spi.Fault
	if errors.As(err, &fault) {
		return stateFailure{name: fault.Code, cause: fault.Error()}
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

func taskPayload(st map[string]any, data any) any {
	params, ok := st["Parameters"].(map[string]any)
	if !ok {
		return data
	}
	p := applyParams(params, data, nil)
	if pl, ok := p["Payload"]; ok {
		return pl
	}
	return p
}

func applyParams(params map[string]any, data any, context map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range params {
		if strings.HasSuffix(k, ".$") {
			path := fmt.Sprint(v)
			if strings.HasPrefix(path, "States.") {
				out[strings.TrimSuffix(k, ".$")], _ = evalIntrinsic(path, data, context)
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
			out[k] = applyParams(m, data, context)
			continue
		}
		out[k] = v
	}
	return out
}

func evalIntrinsic(expression string, data any, context map[string]any) (any, bool) {
	open := strings.IndexByte(expression, '(')
	if open < len("States.") || !strings.HasSuffix(expression, ")") {
		return nil, false
	}
	name := expression[len("States."):open]
	rawArgs := splitIntrinsicArgs(expression[open+1 : len(expression)-1])
	args := make([]any, len(rawArgs))
	for i, raw := range rawArgs {
		var ok bool
		args[i], ok = evalIntrinsicArg(raw, data, context)
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
	}
	return nil, false
}

func evalIntrinsicArg(raw string, data any, context map[string]any) (any, bool) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "States.") {
		return evalIntrinsic(raw, data, context)
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

func (p *Pack) invokeLambda(ctx context.Context, req *spi.Request, resource string, st map[string]any, payload any) (any, error) {
	name := ""
	if params, ok := st["Parameters"].(map[string]any); ok {
		ap := applyParams(params, payload, nil)
		name, _ = ap["FunctionName"].(string)
		if pl, ok := ap["Payload"]; ok {
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
	choices, _ := st["Choices"].([]any)
	for _, c := range choices {
		cm, _ := c.(map[string]any)
		if matchChoice(cm, data) {
			n, _ := cm["Next"].(string)
			return n
		}
	}
	d, _ := st["Default"].(string)
	return d
}

func matchChoice(cm map[string]any, data any) bool {
	got := jsonPath(data, first(cm, "Variable"))
	if eq, ok := cm["StringEquals"]; ok {
		return fmt.Sprint(got) == fmt.Sprint(eq)
	}
	if eq, ok := cm["NumericEquals"]; ok {
		return toFloat(got) == toFloat(eq)
	}
	if eq, ok := cm["BooleanEquals"]; ok {
		return toBool(got) == toBool(eq)
	}
	if _, ok := cm["IsPresent"]; ok {
		return got != nil
	}
	return false
}

func jsonPath(data any, path string) any {
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return data
	}
	cur := data
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		delete(rec, "history")
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
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

func smName(arn string) string {
	return lastSeg(arn, "stateMachine:")
}

func execName(arn string) string {
	return lastSeg(arn, ":")
}

func actName(arn string) string {
	return lastSeg(arn, "activity:")
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
