// Package cloudfront stores distribution and invalidation records (no CDN edge).
package cloudfront

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.cloudfront", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CloudFront-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.cloudfront" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDistribution", "GetDistribution", "ListDistributions", "DeleteDistribution",
		"GetDistributionConfig", "UpdateDistribution",
		"CreateInvalidation", "GetInvalidation", "ListInvalidations",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP != nil && req.Operation != "" {
		p.fill(req)
	}
	switch req.Operation {
	case "CreateDistribution":
		id := "E" + strings.ToUpper(p.deps.Rand.Hex(8))
		domain := strings.ToLower(id) + ".cloudfront.net"
		caller := first(req.Input, "CallerReference")
		if caller == "" {
			caller = between(first(req.Input, "_body"), "<CallerReference>", "</CallerReference>")
		}
		origin := first(req.Input, "DomainName")
		if origin == "" {
			origin = between(first(req.Input, "_body"), "<DomainName>", "</DomainName>")
		}
		rec := map[string]any{
			"Id": id, "DomainName": domain, "Status": "Deployed", "CallerReference": caller,
			"OriginDomainName": origin, "ARN": "arn:aws:cloudfront::" + req.Identity.Account + ":distribution/" + id,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfd").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Distribution": rec, "Id": id, "DomainName": domain, "Status": "Deployed", "ETag": id}}, nil
	case "GetDistribution", "GetDistributionConfig":
		id := distID(req)
		b, ok, _ := p.col(req, "cfd").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchDistribution", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if req.Operation == "GetDistributionConfig" {
			return &spi.Response{Output: map[string]any{"DistributionConfig": rec, "ETag": rec["Id"]}}, nil
		}
		return &spi.Response{Output: map[string]any{"Distribution": rec, "Id": rec["Id"], "Status": rec["Status"], "ETag": rec["Id"]}}, nil
	case "ListDistributions":
		kvs, _, _ := p.col(req, "cfd").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Items": items, "Quantity": len(items), "IsTruncated": false}}, nil
	case "UpdateDistribution":
		id := distID(req)
		b, ok, _ := p.col(req, "cfd").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchDistribution", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if o := between(first(req.Input, "_body"), "<DomainName>", "</DomainName>"); o != "" {
			rec["OriginDomainName"] = o
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "cfd").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Distribution": rec, "ETag": rec["Id"]}}, nil
	case "DeleteDistribution":
		_ = p.col(req, "cfd").Delete(ctx, distID(req))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "CreateInvalidation":
		id := distID(req)
		iid := "I" + strings.ToUpper(p.deps.Rand.Hex(8))
		rec := map[string]any{"Id": iid, "Status": "Completed", "DistributionId": id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfinv:"+id).Put(ctx, iid, b)
		return &spi.Response{Output: map[string]any{"Invalidation": rec, "Id": iid, "Status": "Completed"}}, nil
	case "GetInvalidation":
		id := distID(req)
		iid := invID(req)
		b, ok, _ := p.col(req, "cfinv:"+id).Get(ctx, iid)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchInvalidation", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Invalidation": rec, "Id": rec["Id"], "Status": rec["Status"]}}, nil
	case "ListInvalidations":
		id := distID(req)
		kvs, _, _ := p.col(req, "cfinv:"+id).List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Items": items, "Quantity": len(items)}}, nil
	default:
		return nil, spi.NotImplemented("aws.cloudfront", req.Operation, "emulate")
	}
}

func (p *Pack) fill(req *spi.Request) {
	if req.HTTP == nil {
		return
	}
	if distID(req) == "" {
		parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
		for i, pth := range parts {
			if pth == "distribution" && i+1 < len(parts) {
				req.Input["Id"] = parts[i+1]
			}
			if pth == "invalidation" && i+1 < len(parts) {
				req.Input["InvalidationId"] = parts[i+1]
			}
		}
	}
}

func distID(req *spi.Request) string {
	return first(req.Input, "Id", "DistributionId")
}

func invID(req *spi.Request) string {
	return first(req.Input, "InvalidationId", "Id")
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

func between(s, a, b string) string {
	i := strings.Index(s, a)
	if i < 0 {
		return ""
	}
	s = s[i+len(a):]
	j := strings.Index(s, b)
	if j < 0 {
		return ""
	}
	return strings.TrimSpace(s[:j])
}
