// Package accessanalyzer stores analyzer records (no IAM policy analysis).
package accessanalyzer

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.access-analyzer", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Access Analyzer-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.access-analyzer" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateAnalyzer", "GetAnalyzer", "ListAnalyzers", "DeleteAnalyzer",
		"ListFindings", "CreateArchiveRule", "DeleteArchiveRule",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "analyzerName", "AnalyzerName")
	switch req.Operation {
	case "CreateAnalyzer":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:access-analyzer:" + req.Identity.Region + ":" + req.Identity.Account + ":analyzer/" + name
		rec := map[string]any{"arn": arn, "name": name, "type": first(req.Input, "type", "Type"), "status": "ACTIVE"}
		if rec["type"] == "" {
			rec["type"] = "ACCOUNT"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "aa").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"arn": arn}}, nil
	case "GetAnalyzer":
		b, ok, _ := p.col(req, "aa").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"analyzer": rec}}, nil
	case "ListAnalyzers":
		return listWrap(ctx, p.col(req, "aa"), "analyzers")
	case "DeleteAnalyzer":
		_ = p.col(req, "aa").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListFindings":
		return &spi.Response{Output: map[string]any{"findings": []any{}}}, nil
	case "CreateArchiveRule":
		rule := first(req.Input, "ruleName")
		rec := map[string]any{"ruleName": rule, "analyzerName": name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "aarule").Put(ctx, name+"/"+rule, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteArchiveRule":
		_ = p.col(req, "aarule").Delete(ctx, name+"/"+first(req.Input, "ruleName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.access-analyzer", req.Operation, "emulate")
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
