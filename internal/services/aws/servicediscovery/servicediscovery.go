// Package servicediscovery stores Cloud Map namespaces, services, and instances (no DNS).
package servicediscovery

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.servicediscovery", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Cloud Map-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.servicediscovery" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateHttpNamespace", "CreatePrivateDnsNamespace", "GetNamespace", "ListNamespaces", "DeleteNamespace",
		"CreateService", "GetService", "ListServices", "DeleteService",
		"RegisterInstance", "GetInstance", "ListInstances", "DeregisterInstance",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateHttpNamespace", "CreatePrivateDnsNamespace":
		id := p.deps.Rand.Hex(8)
		name := first(req.Input, "Name")
		rec := map[string]any{"Id": id, "Name": name, "Type": map[string]string{"CreateHttpNamespace": "HTTP", "CreatePrivateDnsNamespace": "DNS_PRIVATE"}[req.Operation]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sdns").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"OperationId": id, "Namespace": rec}}, nil
	case "GetNamespace":
		id := first(req.Input, "Id")
		b, ok, _ := p.col(req, "sdns").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NamespaceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Namespace": rec}}, nil
	case "ListNamespaces":
		return listWrap(ctx, p.col(req, "sdns"), "Namespaces")
	case "DeleteNamespace":
		_ = p.col(req, "sdns").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{"OperationId": p.deps.Rand.Hex(8)}}, nil
	case "CreateService":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "NamespaceId": first(req.Input, "NamespaceId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sdsvc").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Service": rec}}, nil
	case "GetService":
		id := first(req.Input, "Id")
		b, ok, _ := p.col(req, "sdsvc").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ServiceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Service": rec}}, nil
	case "ListServices":
		return listWrap(ctx, p.col(req, "sdsvc"), "Services")
	case "DeleteService":
		_ = p.col(req, "sdsvc").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "RegisterInstance":
		id := first(req.Input, "InstanceId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "ServiceId": first(req.Input, "ServiceId"), "Attributes": req.Input["Attributes"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sdinst").Put(ctx, first(req.Input, "ServiceId")+"/"+id, b)
		return &spi.Response{Output: map[string]any{"OperationId": id}}, nil
	case "GetInstance":
		key := first(req.Input, "ServiceId") + "/" + first(req.Input, "InstanceId")
		b, ok, _ := p.col(req, "sdinst").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "InstanceNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Instance": rec}}, nil
	case "ListInstances":
		return listWrap(ctx, p.col(req, "sdinst"), "Instances")
	case "DeregisterInstance":
		_ = p.col(req, "sdinst").Delete(ctx, first(req.Input, "ServiceId")+"/"+first(req.Input, "InstanceId"))
		return &spi.Response{Output: map[string]any{"OperationId": p.deps.Rand.Hex(8)}}, nil
	default:
		return nil, spi.NotImplemented("aws.servicediscovery", req.Operation, "emulate")
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
