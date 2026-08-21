// Package lightsail stores instance and static-IP records (no VMs).
package lightsail

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.lightsail", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Lightsail-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.lightsail" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateInstances", "GetInstance", "GetInstances", "DeleteInstance",
		"AllocateStaticIp", "GetStaticIp", "ReleaseStaticIp",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateInstances":
		names := stringList(req.Input, "instanceNames", "InstanceNames")
		if len(names) == 0 {
			if n := first(req.Input, "instanceName", "InstanceName"); n != "" {
				names = []string{n}
			}
		}
		var ops []any
		for _, name := range names {
			rec := map[string]any{"name": name, "state": map[string]any{"name": "running"}, "blueprintId": first(req.Input, "blueprintId"), "bundleId": first(req.Input, "bundleId")}
			b, _ := json.Marshal(rec)
			_ = p.col(req, "lsinst").Put(ctx, name, b)
			ops = append(ops, map[string]any{"operationType": "CreateInstance", "status": "Succeeded", "resourceName": name})
		}
		return &spi.Response{Output: map[string]any{"operations": ops}}, nil
	case "GetInstance":
		name := first(req.Input, "instanceName", "InstanceName")
		b, ok, _ := p.col(req, "lsinst").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"instance": rec}}, nil
	case "GetInstances":
		return listWrap(ctx, p.col(req, "lsinst"), "instances")
	case "DeleteInstance":
		_ = p.col(req, "lsinst").Delete(ctx, first(req.Input, "instanceName", "InstanceName"))
		return &spi.Response{Output: map[string]any{"operations": []any{map[string]any{"status": "Succeeded"}}}}, nil
	case "AllocateStaticIp":
		name := first(req.Input, "staticIpName", "StaticIpName")
		rec := map[string]any{"name": name, "ipAddress": "192.0.2.10", "isAttached": false}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lsip").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"operations": []any{map[string]any{"status": "Succeeded"}}}}, nil
	case "GetStaticIp":
		name := first(req.Input, "staticIpName", "StaticIpName")
		b, ok, _ := p.col(req, "lsip").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"staticIp": rec}}, nil
	case "ReleaseStaticIp":
		_ = p.col(req, "lsip").Delete(ctx, first(req.Input, "staticIpName", "StaticIpName"))
		return &spi.Response{Output: map[string]any{"operations": []any{map[string]any{"status": "Succeeded"}}}}, nil
	default:
		return nil, spi.NotImplemented("aws.lightsail", req.Operation, "emulate")
	}
}

func stringList(in map[string]any, keys ...string) []string {
	for _, k := range keys {
		if arr, ok := in[k].([]any); ok {
			var out []string
			for _, v := range arr {
				if s, ok := v.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
	}
	return nil
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
