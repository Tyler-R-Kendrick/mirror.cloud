// Package lexmodels stores bot and intent records (no NLU runtime).
package lexmodels

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.lex-models", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Lex Models-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.lex-models" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"PutBot", "GetBot", "GetBots", "DeleteBot",
		"PutIntent", "GetIntent", "DeleteIntent",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "PutBot":
		name := first(req.Input, "name", "Name")
		rec := map[string]any{"name": name, "status": "READY", "locale": first(req.Input, "locale"), "checksum": p.deps.Rand.Hex(8)}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lexbot").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetBot":
		name := first(req.Input, "name", "Name")
		b, ok, _ := p.col(req, "lexbot").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "GetBots":
		return listWrap(ctx, p.col(req, "lexbot"), "bots")
	case "DeleteBot":
		_ = p.col(req, "lexbot").Delete(ctx, first(req.Input, "name", "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutIntent":
		name := first(req.Input, "name", "Name")
		rec := map[string]any{"name": name, "checksum": p.deps.Rand.Hex(8)}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lexint").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetIntent":
		name := first(req.Input, "name", "Name")
		b, ok, _ := p.col(req, "lexint").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteIntent":
		_ = p.col(req, "lexint").Delete(ctx, first(req.Input, "name", "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.lex-models", req.Operation, "emulate")
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
