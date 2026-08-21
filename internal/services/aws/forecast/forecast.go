// Package forecast stores dataset and predictor records (no ML forecasting).
package forecast

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.forecast", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Forecast-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.forecast" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDataset", "DescribeDataset", "ListDatasets", "DeleteDataset",
		"CreatePredictor", "DescribePredictor", "DeletePredictor",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateDataset":
		name := first(req.Input, "DatasetName")
		arn := "arn:aws:forecast:" + req.Identity.Region + ":" + req.Identity.Account + ":dataset/" + name
		rec := map[string]any{"DatasetName": name, "DatasetArn": arn, "Status": "ACTIVE", "Domain": first(req.Input, "Domain")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fcds").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"DatasetArn": arn}}, nil
	case "DescribeDataset":
		arn := first(req.Input, "DatasetArn")
		name := lastSlash(arn)
		b, ok, _ := p.col(req, "fcds").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListDatasets":
		return listWrap(ctx, p.col(req, "fcds"), "Datasets")
	case "DeleteDataset":
		_ = p.col(req, "fcds").Delete(ctx, lastSlash(first(req.Input, "DatasetArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePredictor":
		name := first(req.Input, "PredictorName")
		arn := "arn:aws:forecast:" + req.Identity.Region + ":" + req.Identity.Account + ":predictor/" + name
		rec := map[string]any{"PredictorName": name, "PredictorArn": arn, "Status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fcpred").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"PredictorArn": arn}}, nil
	case "DescribePredictor":
		name := lastSlash(first(req.Input, "PredictorArn"))
		b, ok, _ := p.col(req, "fcpred").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeletePredictor":
		_ = p.col(req, "fcpred").Delete(ctx, lastSlash(first(req.Input, "PredictorArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.forecast", req.Operation, "emulate")
	}
}

func lastSlash(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	return s
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
