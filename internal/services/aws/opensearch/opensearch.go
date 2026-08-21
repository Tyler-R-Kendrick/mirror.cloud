// Package opensearch is OpenSearch Service-lite: domain records plus an in-memory document store.
package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.es", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements OpenSearch-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.es" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDomain", "DescribeDomain", "DescribeDomains", "ListDomainNames", "DeleteDomain",
		"UpdateDomainConfig", "DescribeDomainConfig",
		"AddTags", "ListTags", "RemoveTags",
		"IndexDocument", "GetDocument", "DeleteDocument", "Search",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP != nil {
		p.fill(req)
	}
	name := first(req.Input, "DomainName", "domainName")
	switch req.Operation {
	case "CreateDomain":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:es:" + req.Identity.Region + ":" + req.Identity.Account + ":domain/" + name
		rec := map[string]any{
			"DomainName": name, "ARN": arn, "Created": true, "Deleted": false, "Processing": false,
			"EngineVersion": first(req.Input, "EngineVersion", "engineVersion"),
			"Endpoint":      name + "." + req.Identity.Region + ".es.localhost.localstack.cloud",
			"DomainId":      req.Identity.Account + "/" + name,
		}
		if rec["EngineVersion"] == "" {
			rec["EngineVersion"] = "OpenSearch_2.11"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "osdom").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"DomainStatus": rec}}, nil
	case "DescribeDomain", "DescribeDomainConfig":
		b, ok, _ := p.col(req, "osdom").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if req.Operation == "DescribeDomainConfig" {
			return &spi.Response{Output: map[string]any{"DomainConfig": rec}}, nil
		}
		return &spi.Response{Output: map[string]any{"DomainStatus": rec}}, nil
	case "DescribeDomains":
		names := stringList(req.Input["DomainNames"])
		var items []any
		for _, n := range names {
			b, ok, _ := p.col(req, "osdom").Get(ctx, n)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"DomainStatusList": items}}, nil
	case "ListDomainNames":
		kvs, _, _ := p.col(req, "osdom").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			items = append(items, map[string]any{"DomainName": kv.Key, "EngineType": "OpenSearch"})
		}
		return &spi.Response{Output: map[string]any{"DomainNames": items}}, nil
	case "DeleteDomain":
		_ = p.col(req, "osdom").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{"DomainStatus": map[string]any{"DomainName": name, "Deleted": true}}}, nil
	case "UpdateDomainConfig":
		b, ok, _ := p.col(req, "osdom").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if v := first(req.Input, "EngineVersion"); v != "" {
			rec["EngineVersion"] = v
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "osdom").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"DomainConfig": rec}}, nil
	case "AddTags":
		arn := first(req.Input, "ARN")
		b, _ := json.Marshal(req.Input["TagList"])
		_ = p.col(req, "ostag").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTags":
		b, ok, _ := p.col(req, "ostag").Get(ctx, first(req.Input, "ARN"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"TagList": tags}}, nil
	case "RemoveTags":
		_ = p.col(req, "ostag").Delete(ctx, first(req.Input, "ARN"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "IndexDocument":
		idx, id := first(req.Input, "Index", "index"), first(req.Input, "Id", "id")
		if idx == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		src := req.Input["Document"]
		if src == nil {
			src = req.Input["_source"]
		}
		if src == nil {
			src = stripMeta(req.Input)
		}
		b, _ := json.Marshal(src)
		_ = p.col(req, docCol(name, idx)).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"_index": idx, "_id": id, "result": "created", "_shards": map[string]any{"total": 1, "successful": 1}}}, nil
	case "GetDocument":
		idx, id := first(req.Input, "Index", "index"), first(req.Input, "Id", "id")
		b, ok, _ := p.col(req, docCol(name, idx)).Get(ctx, id)
		if !ok {
			return &spi.Response{Output: map[string]any{"found": false, "_index": idx, "_id": id}}, nil
		}
		var src any
		_ = json.Unmarshal(b, &src)
		return &spi.Response{Output: map[string]any{"found": true, "_index": idx, "_id": id, "_source": src}}, nil
	case "DeleteDocument":
		idx, id := first(req.Input, "Index", "index"), first(req.Input, "Id", "id")
		_ = p.col(req, docCol(name, idx)).Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"_index": idx, "_id": id, "result": "deleted"}}, nil
	case "Search":
		idx := first(req.Input, "Index", "index")
		kvs, _, _ := p.col(req, docCol(name, idx)).List(ctx, "", "", 0)
		q := req.Input["query"]
		if q == nil {
			if body, ok := req.Input["body"].(map[string]any); ok {
				q = body["query"]
			}
		}
		var hits []any
		for _, kv := range kvs {
			var src any
			_ = json.Unmarshal(kv.Value, &src)
			if !matchQuery(q, src) {
				continue
			}
			hits = append(hits, map[string]any{"_index": idx, "_id": kv.Key, "_source": src, "_score": 1})
		}
		return &spi.Response{Output: map[string]any{
			"hits":      map[string]any{"total": map[string]any{"value": len(hits), "relation": "eq"}, "hits": hits},
			"timed_out": false,
		}}, nil
	default:
		return nil, spi.NotImplemented("aws.es", req.Operation, "emulate")
	}
}

func (p *Pack) fill(req *spi.Request) {
	if req.HTTP == nil {
		return
	}
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	for i, part := range parts {
		if (part == "domain" || part == "opensearch") && i+1 < len(parts) && parts[i+1] != "domain" && parts[i+1] != "list" {
			if _, ok := req.Input["DomainName"]; !ok {
				req.Input["DomainName"] = parts[i+1]
			}
		}
		if part == "_doc" || part == "_search" {
			if i > 0 {
				req.Input["Index"] = parts[i-1]
			}
			if part == "_doc" && i+1 < len(parts) {
				req.Input["Id"] = parts[i+1]
			}
		}
		if part == "_aws" && i+2 < len(parts) && parts[i+1] == "opensearch" {
			req.Input["DomainName"] = parts[i+2]
		}
	}
}

func docCol(domain, index string) string {
	if domain == "" {
		domain = "_"
	}
	return "osdoc:" + domain + ":" + index
}

func stripMeta(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		switch k {
		case "DomainName", "domainName", "Index", "index", "Id", "id", "query", "body":
			continue
		}
		out[k] = v
	}
	return out
}

// ponytail: match_all / term / match / query_string substring only; upgrade is a real query DSL.
func matchQuery(q, src any) bool {
	if q == nil {
		return true
	}
	qm, ok := q.(map[string]any)
	if !ok {
		return strings.Contains(fmt.Sprint(src), fmt.Sprint(q))
	}
	if _, ok := qm["match_all"]; ok || len(qm) == 0 {
		return true
	}
	if term, ok := qm["term"].(map[string]any); ok {
		for k, v := range term {
			return fmt.Sprint(field(src, k)) == fmt.Sprint(v)
		}
	}
	if m, ok := qm["match"].(map[string]any); ok {
		for k, v := range m {
			return strings.Contains(strings.ToLower(fmt.Sprint(field(src, k))), strings.ToLower(fmt.Sprint(v)))
		}
	}
	if qs, ok := qm["query_string"].(map[string]any); ok {
		return strings.Contains(strings.ToLower(fmt.Sprint(src)), strings.ToLower(fmt.Sprint(qs["query"])))
	}
	return strings.Contains(fmt.Sprint(src), fmt.Sprint(q))
}

func field(src any, key string) any {
	m, ok := src.(map[string]any)
	if !ok {
		return nil
	}
	return m[key]
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
