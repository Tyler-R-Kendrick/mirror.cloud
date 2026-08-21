// Package connect stores instance and user records (no contact center).
package connect

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.connect", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Connect-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.connect" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateInstance", "DescribeInstance", "ListInstances", "DeleteInstance",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateInstance":
		id := p.deps.Rand.Hex(8)
		arn := "arn:aws:connect:" + req.Identity.Region + ":" + req.Identity.Account + ":instance/" + id
		rec := map[string]any{"Id": id, "Arn": arn, "InstanceAlias": first(req.Input, "InstanceAlias"), "InstanceStatus": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cninst").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Id": id, "Arn": arn}}, nil
	case "DescribeInstance":
		id := first(req.Input, "InstanceId", "Id")
		b, ok, _ := p.col(req, "cninst").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Instance": rec}}, nil
	case "ListInstances":
		return listWrap(ctx, p.col(req, "cninst"), "InstanceSummaryList")
	case "DeleteInstance":
		_ = p.col(req, "cninst").Delete(ctx, first(req.Input, "InstanceId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateUser":
		id := p.deps.Rand.Hex(8)
		inst := first(req.Input, "InstanceId")
		rec := map[string]any{"Id": id, "Username": first(req.Input, "Username"), "InstanceId": inst}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cnuser").Put(ctx, inst+"/"+id, b)
		return &spi.Response{Output: map[string]any{"UserId": id, "UserArn": "arn:aws:connect:" + req.Identity.Region + ":" + req.Identity.Account + ":instance/" + inst + "/agent/" + id}}, nil
	case "DescribeUser":
		key := first(req.Input, "InstanceId") + "/" + first(req.Input, "UserId")
		b, ok, _ := p.col(req, "cnuser").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "ListUsers":
		return listWrap(ctx, p.col(req, "cnuser"), "UserSummaryList")
	case "DeleteUser":
		_ = p.col(req, "cnuser").Delete(ctx, first(req.Input, "InstanceId")+"/"+first(req.Input, "UserId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.connect", req.Operation, "emulate")
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
