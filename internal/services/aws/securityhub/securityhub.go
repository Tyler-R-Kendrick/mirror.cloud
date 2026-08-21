// Package securityhub stores hub state, findings, and insights (no aggregator).
package securityhub

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.securityhub", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Security Hub-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.securityhub" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"EnableSecurityHub", "DisableSecurityHub", "DescribeHub",
		"BatchImportFindings", "GetFindings", "BatchUpdateFindings",
		"CreateInsight", "GetInsights", "DeleteInsight",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "EnableSecurityHub":
		rec := map[string]any{"HubArn": "arn:aws:securityhub:" + req.Identity.Region + ":" + req.Identity.Account + ":hub/default", "SubscribedAt": "2020-01-01T00:00:00Z"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "shhub").Put(ctx, "default", b)
		return &spi.Response{Output: rec}, nil
	case "DescribeHub":
		b, ok, _ := p.col(req, "shhub").Get(ctx, "default")
		if !ok {
			return nil, &spi.Fault{Code: "InvalidAccessException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DisableSecurityHub":
		_ = p.col(req, "shhub").Delete(ctx, "default")
		return &spi.Response{Output: map[string]any{}}, nil
	case "BatchImportFindings":
		findings := asSlice(req.Input["Findings"])
		for _, f := range findings {
			m, _ := f.(map[string]any)
			id := first(m, "Id")
			if id == "" {
				id = p.deps.Rand.Hex(8)
				m["Id"] = id
			}
			b, _ := json.Marshal(m)
			_ = p.col(req, "shfind").Put(ctx, id, b)
		}
		return &spi.Response{Output: map[string]any{"FailedCount": 0, "SuccessCount": len(findings)}}, nil
	case "GetFindings":
		kvs, _, _ := p.col(req, "shfind").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Findings": items}}, nil
	case "BatchUpdateFindings":
		ids := asSlice(req.Input["FindingIdentifiers"])
		for _, idv := range ids {
			m, _ := idv.(map[string]any)
			id := first(m, "Id")
			b, ok, _ := p.col(req, "shfind").Get(ctx, id)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			if v := req.Input["Note"]; v != nil {
				rec["Note"] = v
			}
			if v := req.Input["Workflow"]; v != nil {
				rec["Workflow"] = v
			}
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "shfind").Put(ctx, id, nb)
		}
		return &spi.Response{Output: map[string]any{"ProcessedFindings": ids, "UnprocessedFindings": []any{}}}, nil
	case "CreateInsight":
		arn := "arn:aws:securityhub:" + req.Identity.Region + ":" + req.Identity.Account + ":insight/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"InsightArn": arn, "Name": first(req.Input, "Name")}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["InsightArn"] = arn
		b, _ := json.Marshal(rec)
		_ = p.col(req, "shins").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"InsightArn": arn}}, nil
	case "GetInsights":
		arns := stringList(req.Input["InsightArns"])
		var items []any
		if len(arns) == 0 {
			kvs, _, _ := p.col(req, "shins").List(ctx, "", "", 0)
			for _, kv := range kvs {
				var rec map[string]any
				_ = json.Unmarshal(kv.Value, &rec)
				items = append(items, rec)
			}
		} else {
			for _, arn := range arns {
				b, ok, _ := p.col(req, "shins").Get(ctx, arn)
				if !ok {
					continue
				}
				var rec map[string]any
				_ = json.Unmarshal(b, &rec)
				items = append(items, rec)
			}
		}
		return &spi.Response{Output: map[string]any{"Insights": items}}, nil
	case "DeleteInsight":
		_ = p.col(req, "shins").Delete(ctx, first(req.Input, "InsightArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.securityhub", req.Operation, "emulate")
	}
}

func asSlice(v any) []any {
	a, _ := v.([]any)
	return a
}

func stringList(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	}
	return nil
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
