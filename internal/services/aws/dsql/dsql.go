// Package dsql stores cluster records (no Aurora DSQL engine).
package dsql

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.dsql", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Aurora DSQL-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.dsql" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateCluster", "GetCluster", "ListClusters", "DeleteCluster", "UpdateCluster", "GetVpcEndpointServiceName"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCluster":
		id := p.deps.Rand.Hex(8)
		arn := "arn:aws:dsql:" + req.Identity.Region + ":" + req.Identity.Account + ":cluster/" + id
		rec := map[string]any{"identifier": id, "arn": arn, "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dsqlcl").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "GetCluster":
		id := first(req.Input, "identifier")
		b, ok, _ := p.col(req, "dsqlcl").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListClusters":
		return listWrap(ctx, p.col(req, "dsqlcl"), "clusters")
	case "DeleteCluster":
		_ = p.col(req, "dsqlcl").Delete(ctx, first(req.Input, "identifier"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "UpdateCluster":
		id := first(req.Input, "identifier")
		b, ok, _ := p.col(req, "dsqlcl").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "GetVpcEndpointServiceName":
		return &spi.Response{Output: map[string]any{"serviceName": "com.amazonaws." + req.Identity.Region + ".dsql"}}, nil
	default:
		return nil, spi.NotImplemented("aws.dsql", req.Operation, "emulate")
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
