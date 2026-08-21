// Package codeartifact stores domain and repository records (no artifact registry).
package codeartifact

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.codeartifact", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CodeArtifact-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.codeartifact" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDomain", "DescribeDomain", "ListDomains", "DeleteDomain",
		"CreateRepository", "DescribeRepository", "DeleteRepository",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	domain := first(req.Input, "domain")
	switch req.Operation {
	case "CreateDomain":
		if domain == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"name": domain, "status": "Active",
			"arn": "arn:aws:codeartifact:" + req.Identity.Region + ":" + req.Identity.Account + ":domain/" + domain,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cadom").Put(ctx, domain, b)
		return &spi.Response{Output: map[string]any{"domain": rec}}, nil
	case "DescribeDomain":
		b, ok, _ := p.col(req, "cadom").Get(ctx, domain)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"domain": rec}}, nil
	case "ListDomains":
		return listWrap(ctx, p.col(req, "cadom"), "domains")
	case "DeleteDomain":
		_ = p.col(req, "cadom").Delete(ctx, domain)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateRepository":
		name := first(req.Input, "repository")
		rec := map[string]any{"name": name, "domainName": domain, "arn": "arn:aws:codeartifact:" + req.Identity.Region + ":" + req.Identity.Account + ":repository/" + domain + "/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "carepo").Put(ctx, domain+"/"+name, b)
		return &spi.Response{Output: map[string]any{"repository": rec}}, nil
	case "DescribeRepository":
		key := domain + "/" + first(req.Input, "repository")
		b, ok, _ := p.col(req, "carepo").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"repository": rec}}, nil
	case "DeleteRepository":
		_ = p.col(req, "carepo").Delete(ctx, domain+"/"+first(req.Input, "repository"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.codeartifact", req.Operation, "emulate")
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
