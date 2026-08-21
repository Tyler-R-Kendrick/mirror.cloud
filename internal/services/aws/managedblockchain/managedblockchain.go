// Package managedblockchain stores network records (no Fabric/Ethereum).
package managedblockchain

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.managedblockchain", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Managed Blockchain-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.managedblockchain" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateNetwork", "GetNetwork", "ListNetworks", "DeleteNetwork", "CreateMember", "GetMember"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateNetwork":
		name := first(req.Input, "Name")
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": name, "Status": "AVAILABLE", "Framework": first(req.Input, "Framework")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mbn").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"NetworkId": id}}, nil
	case "GetNetwork":
		id := first(req.Input, "NetworkId")
		b, ok, _ := p.col(req, "mbn").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Network": rec}}, nil
	case "ListNetworks":
		return listWrap(ctx, p.col(req, "mbn"), "Networks")
	case "DeleteNetwork":
		_ = p.col(req, "mbn").Delete(ctx, first(req.Input, "NetworkId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateMember":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "MemberConfiguration"), "NetworkId": first(req.Input, "NetworkId"), "Status": "AVAILABLE"}
		if rec["Name"] == "" {
			rec["Name"] = first(req.Input, "Name")
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mbm").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"MemberId": id}}, nil
	case "GetMember":
		id := first(req.Input, "MemberId")
		b, ok, _ := p.col(req, "mbm").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Member": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.managedblockchain", req.Operation, "emulate")
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
