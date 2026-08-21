// Package mediapackage stores channel and origin-endpoint records (no packaging).
package mediapackage

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.mediapackage", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MediaPackage-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.mediapackage" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateChannel", "DescribeChannel", "ListChannels", "DeleteChannel",
		"CreateOriginEndpoint", "DescribeOriginEndpoint", "DeleteOriginEndpoint",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := first(req.Input, "Id")
	switch req.Operation {
	case "CreateChannel":
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "Arn": "arn:aws:mediapackage:" + req.Identity.Region + ":" + req.Identity.Account + ":channels/" + id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mpch").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeChannel":
		b, ok, _ := p.col(req, "mpch").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListChannels":
		return listWrap(ctx, p.col(req, "mpch"), "Channels")
	case "DeleteChannel":
		_ = p.col(req, "mpch").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateOriginEndpoint":
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "ChannelId": first(req.Input, "ChannelId"), "Url": "https://mediapackage.local/" + id + ".m3u8"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mpep").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeOriginEndpoint":
		b, ok, _ := p.col(req, "mpep").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteOriginEndpoint":
		_ = p.col(req, "mpep").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.mediapackage", req.Operation, "emulate")
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
