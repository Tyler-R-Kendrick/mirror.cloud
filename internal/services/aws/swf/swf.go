// Package swf stores domains, types, and executions (no activity workers).
package swf

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.swf", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements SWF-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.swf" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"RegisterDomain", "DescribeDomain", "ListDomains", "DeprecateDomain",
		"RegisterWorkflowType", "DescribeWorkflowType", "ListWorkflowTypes",
		"RegisterActivityType", "DescribeActivityType", "ListActivityTypes",
		"StartWorkflowExecution", "DescribeWorkflowExecution", "TerminateWorkflowExecution", "ListOpenWorkflowExecutions",
		"PollForActivityTask",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "RegisterDomain":
		name := first(req.Input, "name")
		if name == "" {
			return nil, &spi.Fault{Code: "UnknownResourceFault", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"name": name, "status": "REGISTERED", "description": first(req.Input, "description")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "swfdom").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeDomain":
		b, ok, _ := p.col(req, "swfdom").Get(ctx, first(req.Input, "name"))
		if !ok {
			return nil, &spi.Fault{Code: "UnknownResourceFault", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"domainInfo": rec}}, nil
	case "ListDomains":
		return listWrap(ctx, p.col(req, "swfdom"), "domainInfos")
	case "DeprecateDomain":
		_ = p.col(req, "swfdom").Delete(ctx, first(req.Input, "name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "RegisterWorkflowType":
		key := first(req.Input, "domain") + "/" + first(req.Input, "name") + "/" + first(req.Input, "version")
		rec := map[string]any{"domain": first(req.Input, "domain"), "name": first(req.Input, "name"), "version": first(req.Input, "version"), "status": "REGISTERED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "swfwf").Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeWorkflowType":
		wf, _ := req.Input["workflowType"].(map[string]any)
		key := first(req.Input, "domain") + "/" + first(wf, "name") + "/" + first(wf, "version")
		b, ok, _ := p.col(req, "swfwf").Get(ctx, key)
		rec := map[string]any{"name": first(wf, "name"), "version": first(wf, "version")}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		return &spi.Response{Output: map[string]any{"typeInfo": rec}}, nil
	case "ListWorkflowTypes":
		return listWrap(ctx, p.col(req, "swfwf"), "typeInfos")
	case "RegisterActivityType":
		key := first(req.Input, "domain") + "/" + first(req.Input, "name") + "/" + first(req.Input, "version")
		rec := map[string]any{"domain": first(req.Input, "domain"), "name": first(req.Input, "name"), "version": first(req.Input, "version"), "status": "REGISTERED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "swfact").Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeActivityType":
		at, _ := req.Input["activityType"].(map[string]any)
		key := first(req.Input, "domain") + "/" + first(at, "name") + "/" + first(at, "version")
		b, ok, _ := p.col(req, "swfact").Get(ctx, key)
		rec := map[string]any{"name": first(at, "name"), "version": first(at, "version")}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		return &spi.Response{Output: map[string]any{"typeInfo": rec}}, nil
	case "ListActivityTypes":
		return listWrap(ctx, p.col(req, "swfact"), "typeInfos")
	case "StartWorkflowExecution":
		id := first(req.Input, "workflowId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		run := p.deps.Rand.Hex(8)
		rec := map[string]any{"workflowId": id, "runId": run, "domain": first(req.Input, "domain"), "executionStatus": "OPEN"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "swfex:"+first(req.Input, "domain")).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"runId": run}}, nil
	case "DescribeWorkflowExecution":
		ex, _ := req.Input["execution"].(map[string]any)
		id := first(ex, "workflowId")
		b, ok, _ := p.col(req, "swfex:"+first(req.Input, "domain")).Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "UnknownResourceFault", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"executionInfo": rec}}, nil
	case "TerminateWorkflowExecution":
		id := first(req.Input, "workflowId")
		if id == "" {
			if ex, ok := req.Input["execution"].(map[string]any); ok {
				id = first(ex, "workflowId")
			}
		}
		_ = p.col(req, "swfex:"+first(req.Input, "domain")).Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListOpenWorkflowExecutions":
		return listWrap(ctx, p.col(req, "swfex:"+first(req.Input, "domain")), "executionInfos")
	case "PollForActivityTask":
		// ponytail: no activity workers; empty poll.
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.swf", req.Operation, "emulate")
	}
}

func listWrap(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	if in == nil {
		return ""
	}
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
