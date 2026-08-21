// Package macie2 stores session and job records (no S3 content scan).
package macie2

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.macie2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Macie v2-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.macie2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"EnableMacie", "GetMacieSession", "DisableMacie",
		"CreateClassificationJob", "DescribeClassificationJob", "ListClassificationJobs",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "EnableMacie":
		rec := map[string]any{"status": "ENABLED", "findingPublishingFrequency": first(req.Input, "findingPublishingFrequency")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "macie").Put(ctx, "session", b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetMacieSession":
		b, ok, _ := p.col(req, "macie").Get(ctx, "session")
		if !ok {
			return nil, &spi.Fault{Code: "AccessDeniedException", HTTPStatus: 403, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DisableMacie":
		_ = p.col(req, "macie").Delete(ctx, "session")
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateClassificationJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"jobId": id, "name": first(req.Input, "name", "Name"), "jobStatus": "COMPLETE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "maciejob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"jobId": id}}, nil
	case "DescribeClassificationJob":
		id := first(req.Input, "jobId")
		b, ok, _ := p.col(req, "maciejob").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListClassificationJobs":
		return listWrap(ctx, p.col(req, "maciejob"), "items")
	default:
		return nil, spi.NotImplemented("aws.macie2", req.Operation, "emulate")
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
