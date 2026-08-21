// Package s3control stores access points and public-access-block records (no account S3 enforcement).
package s3control

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.s3control", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements S3 Control-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.s3control" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateAccessPoint", "GetAccessPoint", "ListAccessPoints", "DeleteAccessPoint",
		"PutPublicAccessBlock", "GetPublicAccessBlock", "DeletePublicAccessBlock",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	switch req.Operation {
	case "CreateAccessPoint":
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidRequest", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:s3:" + req.Identity.Region + ":" + req.Identity.Account + ":accesspoint/" + name
		rec := map[string]any{"Name": name, "Bucket": first(req.Input, "Bucket"), "AccessPointArn": arn, "NetworkOrigin": "Internet"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "s3cap").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"AccessPointArn": arn}}, nil
	case "GetAccessPoint":
		b, ok, _ := p.col(req, "s3cap").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchAccessPoint", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListAccessPoints":
		return listWrap(ctx, p.col(req, "s3cap"), "AccessPointList")
	case "DeleteAccessPoint":
		_ = p.col(req, "s3cap").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutPublicAccessBlock":
		cfg, _ := req.Input["PublicAccessBlockConfiguration"].(map[string]any)
		if cfg == nil {
			cfg = map[string]any{}
		}
		b, _ := json.Marshal(cfg)
		_ = p.col(req, "s3cpab").Put(ctx, pabKey(req), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetPublicAccessBlock":
		b, ok, _ := p.col(req, "s3cpab").Get(ctx, pabKey(req))
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchPublicAccessBlockConfiguration", HTTPStatus: 404, Fault: "client"}
		}
		var cfg map[string]any
		_ = json.Unmarshal(b, &cfg)
		return &spi.Response{Output: map[string]any{"PublicAccessBlockConfiguration": cfg}}, nil
	case "DeletePublicAccessBlock":
		_ = p.col(req, "s3cpab").Delete(ctx, pabKey(req))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.s3control", req.Operation, "emulate")
	}
}

func pabKey(req *spi.Request) string {
	if b := first(req.Input, "Bucket"); b != "" {
		return "bucket:" + b
	}
	return "account:" + req.Identity.Account
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
