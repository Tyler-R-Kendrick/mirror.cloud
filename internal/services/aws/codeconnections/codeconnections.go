// Package codeconnections stores connection records (no git OAuth).
package codeconnections

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.codeconnections", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CodeConnections-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.codeconnections" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateConnection", "GetConnection", "ListConnections", "DeleteConnection", "CreateHost", "GetHost"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "ConnectionName")
	switch req.Operation {
	case "CreateConnection":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:codeconnections:" + req.Identity.Region + ":" + req.Identity.Account + ":connection/" + name
		rec := map[string]any{"ConnectionName": name, "ConnectionArn": arn, "ConnectionStatus": "AVAILABLE", "ProviderType": first(req.Input, "ProviderType")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ccn").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"ConnectionArn": arn}}, nil
	case "GetConnection":
		key := lastSlash(first(req.Input, "ConnectionArn", "ConnectionName"))
		b, ok, _ := p.col(req, "ccn").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Connection": rec}}, nil
	case "ListConnections":
		return listWrap(ctx, p.col(req, "ccn"), "Connections")
	case "DeleteConnection":
		_ = p.col(req, "ccn").Delete(ctx, lastSlash(first(req.Input, "ConnectionArn", "ConnectionName")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateHost":
		hname := first(req.Input, "Name")
		arn := "arn:aws:codeconnections:" + req.Identity.Region + ":" + req.Identity.Account + ":host/" + hname
		rec := map[string]any{"Name": hname, "HostArn": arn, "Status": "AVAILABLE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cchost").Put(ctx, hname, b)
		return &spi.Response{Output: map[string]any{"HostArn": arn}}, nil
	case "GetHost":
		key := lastSlash(first(req.Input, "HostArn", "Name"))
		b, ok, _ := p.col(req, "cchost").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	default:
		return nil, spi.NotImplemented("aws.codeconnections", req.Operation, "emulate")
	}
}

func lastSlash(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}
	return s
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
