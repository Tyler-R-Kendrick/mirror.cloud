// Package cloudhsmv2 stores cluster records (no HSM hardware or PKCS#11).
package cloudhsmv2

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.cloudhsmv2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CloudHSM v2-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.cloudhsmv2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateCluster", "DescribeClusters", "DeleteCluster",
		"CreateHsm", "DeleteHsm", "DescribeBackups",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCluster":
		id := "cluster-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"ClusterId": id, "State": "ACTIVE", "HsmType": first(req.Input, "HsmType")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "hsmcl").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "DescribeClusters":
		return listOrGet(ctx, p.col(req, "hsmcl"), first(req.Input, "ClusterId"), "Clusters")
	case "DeleteCluster":
		_ = p.col(req, "hsmcl").Delete(ctx, first(req.Input, "ClusterId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateHsm":
		id := "hsm-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"HsmId": id, "ClusterId": first(req.Input, "ClusterId"), "State": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "hsm").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Hsm": rec}}, nil
	case "DeleteHsm":
		_ = p.col(req, "hsm").Delete(ctx, first(req.Input, "HsmId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeBackups":
		return &spi.Response{Output: map[string]any{"Backups": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.cloudhsmv2", req.Operation, "emulate")
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
