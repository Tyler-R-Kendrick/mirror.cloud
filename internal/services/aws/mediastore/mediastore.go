// Package mediastore stores container records (no origin CDN).
package mediastore

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.mediastore", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MediaStore-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.mediastore" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateContainer", "DescribeContainer", "ListContainers", "DeleteContainer",
		"PutContainerPolicy", "GetContainerPolicy",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "ContainerName")
	switch req.Operation {
	case "CreateContainer":
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidParameter", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"Name": name, "Status": "ACTIVE", "ARN": "arn:aws:mediastore:" + req.Identity.Region + ":" + req.Identity.Account + ":container/" + name,
			"Endpoint": "https://" + name + ".mediastore." + req.Identity.Region + ".amazonaws.com",
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "msct").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Container": rec}}, nil
	case "DescribeContainer":
		b, ok, _ := p.col(req, "msct").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ContainerNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Container": rec}}, nil
	case "ListContainers":
		return listWrap(ctx, p.col(req, "msct"), "Containers")
	case "DeleteContainer":
		_ = p.col(req, "msct").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutContainerPolicy":
		pol := first(req.Input, "Policy")
		_ = p.col(req, "mspol").Put(ctx, name, []byte(pol))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetContainerPolicy":
		b, ok, _ := p.col(req, "mspol").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "PolicyNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"Policy": string(b)}}, nil
	default:
		return nil, spi.NotImplemented("aws.mediastore", req.Operation, "emulate")
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
