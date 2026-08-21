// Package quicksight stores datasets and dashboards (no BI engine).
package quicksight

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.quicksight", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements QuickSight-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.quicksight" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDataSet", "DescribeDataSet", "ListDataSets", "DeleteDataSet",
		"CreateDashboard", "DescribeDashboard", "ListDashboards", "DeleteDashboard",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateDataSet":
		id := first(req.Input, "DataSetId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		arn := "arn:aws:quicksight:" + req.Identity.Region + ":" + req.Identity.Account + ":dataset/" + id
		rec := map[string]any{"DataSetId": id, "Name": first(req.Input, "Name"), "Arn": arn}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "qsds").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Arn": arn, "DataSetId": id, "Status": 201}}, nil
	case "DescribeDataSet":
		id := first(req.Input, "DataSetId")
		b, ok, _ := p.col(req, "qsds").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"DataSet": rec}}, nil
	case "ListDataSets":
		return listWrap(ctx, p.col(req, "qsds"), "DataSetSummaries")
	case "DeleteDataSet":
		_ = p.col(req, "qsds").Delete(ctx, first(req.Input, "DataSetId"))
		return &spi.Response{Output: map[string]any{"Status": 200}}, nil
	case "CreateDashboard":
		id := first(req.Input, "DashboardId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		arn := "arn:aws:quicksight:" + req.Identity.Region + ":" + req.Identity.Account + ":dashboard/" + id
		rec := map[string]any{"DashboardId": id, "Name": first(req.Input, "Name"), "Arn": arn}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "qsdash").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Arn": arn, "DashboardId": id, "Status": 202}}, nil
	case "DescribeDashboard":
		id := first(req.Input, "DashboardId")
		b, ok, _ := p.col(req, "qsdash").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Dashboard": rec}}, nil
	case "ListDashboards":
		return listWrap(ctx, p.col(req, "qsdash"), "DashboardSummaryList")
	case "DeleteDashboard":
		_ = p.col(req, "qsdash").Delete(ctx, first(req.Input, "DashboardId"))
		return &spi.Response{Output: map[string]any{"Status": 204}}, nil
	default:
		return nil, spi.NotImplemented("aws.quicksight", req.Operation, "emulate")
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
