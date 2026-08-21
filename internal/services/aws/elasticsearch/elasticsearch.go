// Package elasticsearch stores Elasticsearch Service domain records (not OpenSearch, no cluster).
package elasticsearch

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.elasticsearch", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Elasticsearch Service-lite (legacy es API, distinct from OpenSearch).
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.elasticsearch" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateElasticsearchDomain", "DescribeElasticsearchDomain", "DescribeElasticsearchDomains",
		"ListDomainNames", "UpdateElasticsearchDomainConfig", "DeleteElasticsearchDomain",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "DomainName")
	switch req.Operation {
	case "CreateElasticsearchDomain", "UpdateElasticsearchDomainConfig":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"DomainName": name, "ElasticsearchVersion": first(req.Input, "ElasticsearchVersion"),
			"ARN":          "arn:aws:es:" + req.Identity.Region + ":" + req.Identity.Account + ":domain/" + name,
			"DomainStatus": map[string]any{"DomainName": name, "Created": true, "Deleted": false, "Processing": false, "Engine": "Elasticsearch"},
		}
		if rec["ElasticsearchVersion"] == "" {
			rec["ElasticsearchVersion"] = "7.10"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "esdom").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"DomainStatus": rec["DomainStatus"], "DomainName": name, "ElasticsearchVersion": rec["ElasticsearchVersion"]}}, nil
	case "DescribeElasticsearchDomain":
		b, ok, _ := p.col(req, "esdom").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"DomainStatus": rec["DomainStatus"]}}, nil
	case "DescribeElasticsearchDomains":
		names := stringList(req.Input["DomainNames"])
		var items []any
		if len(names) == 0 {
			kvs, _, _ := p.col(req, "esdom").List(ctx, "", "", 0)
			for _, kv := range kvs {
				var rec map[string]any
				_ = json.Unmarshal(kv.Value, &rec)
				items = append(items, rec["DomainStatus"])
			}
		} else {
			for _, n := range names {
				b, ok, _ := p.col(req, "esdom").Get(ctx, n)
				if !ok {
					continue
				}
				var rec map[string]any
				_ = json.Unmarshal(b, &rec)
				items = append(items, rec["DomainStatus"])
			}
		}
		return &spi.Response{Output: map[string]any{"DomainStatusList": items}}, nil
	case "ListDomainNames":
		kvs, _, _ := p.col(req, "esdom").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, map[string]any{"DomainName": kv.Key, "EngineType": "Elasticsearch"})
		}
		return &spi.Response{Output: map[string]any{"DomainNames": names}}, nil
	case "DeleteElasticsearchDomain":
		b, ok, _ := p.col(req, "esdom").Get(ctx, name)
		_ = p.col(req, "esdom").Delete(ctx, name)
		st := map[string]any{"DomainName": name, "Deleted": true}
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			if m, ok := rec["DomainStatus"].(map[string]any); ok {
				m["Deleted"] = true
				st = m
			}
		}
		return &spi.Response{Output: map[string]any{"DomainStatus": st}}, nil
	default:
		return nil, spi.NotImplemented("aws.elasticsearch", req.Operation, "emulate")
	}
}

func stringList(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	}
	return nil
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
