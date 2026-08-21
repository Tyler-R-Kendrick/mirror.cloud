// Package tagging is Resource Groups Tagging API (in-memory tag map).
package tagging

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.tagging", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements tagging.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.tagging" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"GetResources", "TagResources", "UntagResources", "GetTagKeys", "GetTagValues",
		"GetComplianceSummary", "StartReportCreation", "DescribeReportCreation", "ListRequiredTags"}
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("rtags")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "TagResources":
		tags, _ := req.Input["Tags"].(map[string]any)
		for _, arn := range asStrings(req.Input["ResourceARNList"]) {
			cur := map[string]any{}
			if b, ok, _ := p.col(req).Get(ctx, arn); ok {
				_ = json.Unmarshal(b, &cur)
			}
			for k, v := range tags {
				cur[k] = v
			}
			b, _ := json.Marshal(cur)
			_ = p.col(req).Put(ctx, arn, b)
		}
		return &spi.Response{Output: map[string]any{"FailedResourcesMap": map[string]any{}}}, nil
	case "UntagResources":
		keys := asStrings(req.Input["TagKeys"])
		for _, arn := range asStrings(req.Input["ResourceARNList"]) {
			cur := map[string]any{}
			if b, ok, _ := p.col(req).Get(ctx, arn); ok {
				_ = json.Unmarshal(b, &cur)
			}
			for _, k := range keys {
				delete(cur, k)
			}
			b, _ := json.Marshal(cur)
			_ = p.col(req).Put(ctx, arn, b)
		}
		return &spi.Response{Output: map[string]any{"FailedResourcesMap": map[string]any{}}}, nil
	case "GetResources":
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		var maps []any
		for _, kv := range kvs {
			var tags map[string]any
			_ = json.Unmarshal(kv.Value, &tags)
			var list []any
			for k, v := range tags {
				list = append(list, map[string]any{"Key": k, "Value": v})
			}
			maps = append(maps, map[string]any{"ResourceARN": kv.Key, "Tags": list})
		}
		return &spi.Response{Output: map[string]any{"ResourceTagMappingList": maps}}, nil
	case "GetTagKeys":
		keys := p.allKeys(ctx, req)
		var out []any
		for _, k := range keys {
			out = append(out, k)
		}
		return &spi.Response{Output: map[string]any{"TagKeys": out}}, nil
	case "GetTagValues":
		want := str(req.Input["Key"])
		seen := map[string]bool{}
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		var vals []any
		for _, kv := range kvs {
			var tags map[string]any
			_ = json.Unmarshal(kv.Value, &tags)
			if v, ok := tags[want]; ok {
				s := str(v)
				if !seen[s] {
					seen[s] = true
					vals = append(vals, s)
				}
			}
		}
		return &spi.Response{Output: map[string]any{"TagValues": vals}}, nil
	case "GetComplianceSummary":
		return &spi.Response{Output: map[string]any{"SummaryList": []any{}}}, nil
	case "StartReportCreation":
		_ = p.col(req).Put(ctx, "_report", []byte("RUNNING"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeReportCreation":
		b, ok, _ := p.col(req).Get(ctx, "_report")
		st := "FAILED"
		if ok {
			st = string(b)
		}
		return &spi.Response{Output: map[string]any{"Status": st}}, nil
	case "ListRequiredTags":
		return &spi.Response{Output: map[string]any{"RequiredTags": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.tagging", req.Operation, "emulate")
	}
}

func (p *Pack) allKeys(ctx context.Context, req *spi.Request) []string {
	seen := map[string]bool{}
	kvs, _, _ := p.col(req).List(ctx, "", "", 0)
	for _, kv := range kvs {
		if kv.Key == "_report" {
			continue
		}
		var tags map[string]any
		_ = json.Unmarshal(kv.Value, &tags)
		for k := range tags {
			seen[k] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func asStrings(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if t != "" {
			return []string{t}
		}
	}
	return nil
}

func str(v any) string { s, _ := v.(string); return s }
