// Package appmesh stores mesh and virtual-node records (no Envoy dataplane).
package appmesh

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.appmesh", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements App Mesh-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.appmesh" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateMesh", "DescribeMesh", "ListMeshes", "DeleteMesh",
		"CreateVirtualNode", "DescribeVirtualNode", "DeleteVirtualNode",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	mesh := first(req.Input, "meshName")
	switch req.Operation {
	case "CreateMesh":
		if mesh == "" {
			return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"meshName": mesh, "meshOwner": req.Identity.Account, "arn": "arn:aws:appmesh:" + req.Identity.Region + ":" + req.Identity.Account + ":mesh/" + mesh, "status": map[string]any{"status": "ACTIVE"}}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ammesh").Put(ctx, mesh, b)
		return &spi.Response{Output: map[string]any{"mesh": rec}}, nil
	case "DescribeMesh":
		b, ok, _ := p.col(req, "ammesh").Get(ctx, mesh)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"mesh": rec}}, nil
	case "ListMeshes":
		return listWrap(ctx, p.col(req, "ammesh"), "meshes")
	case "DeleteMesh":
		_ = p.col(req, "ammesh").Delete(ctx, mesh)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateVirtualNode":
		name := first(req.Input, "virtualNodeName")
		rec := map[string]any{"meshName": mesh, "virtualNodeName": name, "status": map[string]any{"status": "ACTIVE"}}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "amvn").Put(ctx, mesh+"/"+name, b)
		return &spi.Response{Output: map[string]any{"virtualNode": rec}}, nil
	case "DescribeVirtualNode":
		key := mesh + "/" + first(req.Input, "virtualNodeName")
		b, ok, _ := p.col(req, "amvn").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"virtualNode": rec}}, nil
	case "DeleteVirtualNode":
		_ = p.col(req, "amvn").Delete(ctx, mesh+"/"+first(req.Input, "virtualNodeName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.appmesh", req.Operation, "emulate")
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
