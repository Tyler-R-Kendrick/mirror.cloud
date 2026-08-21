// Package medialive stores channel and input records (no live transcode).
package medialive

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.medialive", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MediaLive-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.medialive" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateChannel", "DescribeChannel", "ListChannels", "DeleteChannel",
		"CreateInput", "DescribeInput", "DeleteInput",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateChannel":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "State": "IDLE", "Arn": "arn:aws:medialive:" + req.Identity.Region + ":" + req.Identity.Account + ":channel:" + id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mlch").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Channel": rec}}, nil
	case "DescribeChannel":
		return getWrap(ctx, p.col(req, "mlch"), first(req.Input, "ChannelId", "Id"), "Channel")
	case "ListChannels":
		return listWrap(ctx, p.col(req, "mlch"), "Channels")
	case "DeleteChannel":
		_ = p.col(req, "mlch").Delete(ctx, first(req.Input, "ChannelId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateInput":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "Type": first(req.Input, "Type"), "State": "DETACHED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mlin").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Input": rec}}, nil
	case "DescribeInput":
		return getWrap(ctx, p.col(req, "mlin"), first(req.Input, "InputId", "Id"), "Input")
	case "DeleteInput":
		_ = p.col(req, "mlin").Delete(ctx, first(req.Input, "InputId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.medialive", req.Operation, "emulate")
	}
}

func getWrap(ctx context.Context, c spi.Collection, id, key string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{key: rec}}, nil
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
