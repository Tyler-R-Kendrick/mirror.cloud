// Package memorydb stores cluster and user records (no Redis engine).
package memorydb

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.memorydb", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MemoryDB-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.memorydb" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateCluster", "DescribeClusters", "DeleteCluster",
		"CreateUser", "DescribeUsers", "DeleteUser",
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
			"Name": name, "Status": "available", "NodeType": first(req.Input, "NodeType"),
			"ARN": "arn:aws:memorydb:" + req.Identity.Region + ":" + req.Identity.Account + ":cluster/" + name,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mdbcl").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DescribeClusters":
		return listOrGet(ctx, p.col(req, "mdbcl"), first(req.Input, "ClusterName"), "Clusters")
	case "DeleteCluster":
		_ = p.col(req, "mdbcl").Delete(ctx, first(req.Input, "ClusterName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateUser":
		name := first(req.Input, "UserName")
		rec := map[string]any{"Name": name, "Status": "active", "AccessString": first(req.Input, "AccessString")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mdbu").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "DescribeUsers":
		return listOrGet(ctx, p.col(req, "mdbu"), first(req.Input, "UserName"), "Users")
	case "DeleteUser":
		_ = p.col(req, "mdbu").Delete(ctx, first(req.Input, "UserName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.memorydb", req.Operation, "emulate")
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
