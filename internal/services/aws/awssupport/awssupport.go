// Package awssupport stores Support cases (no AWS Support backend).
package awssupport

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.support", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Support-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.support" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateCase", "DescribeCases", "ResolveCase",
		"DescribeServices", "DescribeSeverityLevels", "AddCommunicationToCase",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCase":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"caseId": id, "subject": first(req.Input, "subject"), "status": "opened",
			"serviceCode": first(req.Input, "serviceCode"), "severityCode": first(req.Input, "severityCode"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "supcase").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"caseId": id}}, nil
	case "DescribeCases":
		id := first(req.Input, "caseIdList")
		if id == "" {
			if arr, ok := req.Input["caseIdList"].([]any); ok && len(arr) > 0 {
				id, _ = arr[0].(string)
			}
		}
		return listOrGet(ctx, p.col(req, "supcase"), id, "cases")
	case "ResolveCase":
		id := first(req.Input, "caseId")
		_ = p.col(req, "supcase").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"finalCaseStatus": "resolved", "initialCaseStatus": "opened"}}, nil
	case "DescribeServices":
		return &spi.Response{Output: map[string]any{"services": []any{map[string]any{"code": "amazon-s3", "name": "Amazon Simple Storage Service"}}}}, nil
	case "DescribeSeverityLevels":
		return &spi.Response{Output: map[string]any{"severityLevels": []any{map[string]any{"code": "low", "name": "Low"}}}}, nil
	case "AddCommunicationToCase":
		return &spi.Response{Output: map[string]any{"result": true}}, nil
	default:
		return nil, spi.NotImplemented("aws.support", req.Operation, "emulate")
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
