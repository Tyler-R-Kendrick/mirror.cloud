// Package dax stores cluster records (no DynamoDB accelerator).
package dax

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.dax", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements DAX-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.dax" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateCluster", "DescribeClusters", "DeleteCluster",
		"CreateParameterGroup", "DescribeParameterGroups", "DeleteParameterGroup",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCluster":
		name := first(req.Input, "ClusterName")
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidParameterValueException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"ClusterName": name, "Status": "available", "NodeType": first(req.Input, "NodeType"),
			"ClusterArn": "arn:aws:dax:" + req.Identity.Region + ":" + req.Identity.Account + ":cache/" + name,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "daxcl").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DescribeClusters":
		return listOrGet(ctx, p.col(req, "daxcl"), first(req.Input, "ClusterName"), "Clusters")
	case "DeleteCluster":
		_ = p.col(req, "daxcl").Delete(ctx, first(req.Input, "ClusterName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateParameterGroup":
		name := first(req.Input, "ParameterGroupName")
		rec := map[string]any{"ParameterGroupName": name, "Description": first(req.Input, "Description")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "daxpg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"ParameterGroup": rec}}, nil
	case "DescribeParameterGroups":
		return listOrGet(ctx, p.col(req, "daxpg"), first(req.Input, "ParameterGroupName"), "ParameterGroups")
	case "DeleteParameterGroup":
		_ = p.col(req, "daxpg").Delete(ctx, first(req.Input, "ParameterGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.dax", req.Operation, "emulate")
	}
}

func listOrGet(ctx context.Context, c spi.Collection, want, key string) (*spi.Response, error) {
	if want != "" {
		b, ok, _ := c.Get(ctx, want)
		if !ok {
			return &spi.Response{Output: map[string]any{key: []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{key: []any{rec}}}, nil
	}
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
