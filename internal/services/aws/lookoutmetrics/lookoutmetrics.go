// Package lookoutmetrics stores detector and alert records (no anomaly ML).
package lookoutmetrics

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.lookoutmetrics", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Lookout for Metrics-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.lookoutmetrics" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateAnomalyDetector", "DescribeAnomalyDetector", "ListAnomalyDetectors", "DeleteAnomalyDetector",
		"CreateAlert", "DescribeAlert", "ListAlerts",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateAnomalyDetector":
		name := first(req.Input, "AnomalyDetectorName")
		arn := "arn:aws:lookoutmetrics:" + req.Identity.Region + ":" + req.Identity.Account + ":AnomalyDetector:" + name
		rec := map[string]any{"AnomalyDetectorName": name, "AnomalyDetectorArn": arn, "Status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lmad").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"AnomalyDetectorArn": arn}}, nil
	case "DescribeAnomalyDetector":
		arn := first(req.Input, "AnomalyDetectorArn")
		name := lastColon(arn)
		b, ok, _ := p.col(req, "lmad").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListAnomalyDetectors":
		return listWrap(ctx, p.col(req, "lmad"), "AnomalyDetectorSummaryList")
	case "DeleteAnomalyDetector":
		_ = p.col(req, "lmad").Delete(ctx, lastColon(first(req.Input, "AnomalyDetectorArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateAlert":
		name := first(req.Input, "AlertName")
		arn := "arn:aws:lookoutmetrics:" + req.Identity.Region + ":" + req.Identity.Account + ":Alert:" + name
		rec := map[string]any{"AlertName": name, "AlertArn": arn, "AnomalyDetectorArn": first(req.Input, "AnomalyDetectorArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lmalert").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"AlertArn": arn}}, nil
	case "DescribeAlert":
		name := lastColon(first(req.Input, "AlertArn"))
		b, ok, _ := p.col(req, "lmalert").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListAlerts":
		return listWrap(ctx, p.col(req, "lmalert"), "AlertSummaryList")
	default:
		return nil, spi.NotImplemented("aws.lookoutmetrics", req.Operation, "emulate")
	}
}

func lastColon(s string) string {
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
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
