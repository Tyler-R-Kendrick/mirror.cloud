// Package ce stores Cost Explorer monitors (no CUR, canned zero cost).
package ce

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ce", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Cost Explorer-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.ce" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateAnomalyMonitor", "GetAnomalyMonitors", "DeleteAnomalyMonitor",
		"CreateCostCategoryDefinition", "DescribeCostCategoryDefinition", "DeleteCostCategoryDefinition",
		"GetCostAndUsage",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func lastSeg(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateAnomalyMonitor":
		name := first(req.Input, "MonitorName", "AnomalyMonitor")
		if name == "" {
			if m, ok := req.Input["AnomalyMonitor"].(map[string]any); ok {
				name = first(m, "MonitorName")
			}
		}
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:ce::" + req.Identity.Account + ":anomalymonitor/" + name
		rec := map[string]any{"MonitorName": name, "MonitorArn": arn, "MonitorType": "CUSTOM"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cemon").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"MonitorArn": arn}}, nil
	case "GetAnomalyMonitors":
		return listOrGet(ctx, p.col(req, "cemon"), lastSeg(first(req.Input, "MonitorArn")), "AnomalyMonitors")
	case "DeleteAnomalyMonitor":
		_ = p.col(req, "cemon").Delete(ctx, lastSeg(first(req.Input, "MonitorArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateCostCategoryDefinition":
		name := first(req.Input, "Name")
		arn := "arn:aws:ce::" + req.Identity.Account + ":costcategory/" + name
		rec := map[string]any{"Name": name, "CostCategoryArn": arn}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cecc").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"CostCategoryArn": arn}}, nil
	case "DescribeCostCategoryDefinition":
		name := lastSeg(first(req.Input, "CostCategoryArn"))
		b, ok, _ := p.col(req, "cecc").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"CostCategory": rec}}, nil
	case "DeleteCostCategoryDefinition":
		_ = p.col(req, "cecc").Delete(ctx, lastSeg(first(req.Input, "CostCategoryArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetCostAndUsage":
		return &spi.Response{Output: map[string]any{
			"ResultsByTime": []any{map[string]any{
				"TimePeriod": map[string]any{"Start": "2020-01-01", "End": "2020-01-02"},
				"Total":      map[string]any{"UnblendedCost": map[string]any{"Amount": "0", "Unit": "USD"}},
			}},
		}}, nil
	default:
		return nil, spi.NotImplemented("aws.ce", req.Operation, "emulate")
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
	return listWrap(ctx, c, key)
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
