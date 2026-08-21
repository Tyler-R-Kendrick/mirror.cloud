// Package mediaconnect stores flow records (no media transport).
package mediaconnect

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.mediaconnect", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MediaConnect-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.mediaconnect" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateFlow", "DescribeFlow", "ListFlows", "DeleteFlow",
		"StartFlow", "StopFlow",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := first(req.Input, "FlowArn")
	switch req.Operation {
	case "CreateFlow":
		name := first(req.Input, "Name")
		arn = "arn:aws:mediaconnect:" + req.Identity.Region + ":" + req.Identity.Account + ":flow:" + name
		rec := map[string]any{"Name": name, "FlowArn": arn, "Status": "STANDBY"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mcflow").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Flow": rec}}, nil
	case "DescribeFlow":
		b, ok, _ := p.col(req, "mcflow").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Flow": rec}}, nil
	case "ListFlows":
		return listWrap(ctx, p.col(req, "mcflow"), "Flows")
	case "StartFlow", "StopFlow":
		b, ok, _ := p.col(req, "mcflow").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if req.Operation == "StartFlow" {
			rec["Status"] = "ACTIVE"
		} else {
			rec["Status"] = "STANDBY"
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "mcflow").Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{"FlowArn": arn, "Status": rec["Status"]}}, nil
	case "DeleteFlow":
		_ = p.col(req, "mcflow").Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.mediaconnect", req.Operation, "emulate")
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
