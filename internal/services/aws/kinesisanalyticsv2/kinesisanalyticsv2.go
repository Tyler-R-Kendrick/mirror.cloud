// Package kinesisanalyticsv2 stores Flink application records (no Flink runtime).
package kinesisanalyticsv2

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.kinesisanalyticsv2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Managed Service for Apache Flink-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.kinesisanalyticsv2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateApplication", "DescribeApplication", "ListApplications", "DeleteApplication", "StartApplication", "StopApplication"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "ApplicationName")
	switch req.Operation {
	case "CreateApplication":
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:kinesisanalytics:" + req.Identity.Region + ":" + req.Identity.Account + ":application/" + name
		rec := map[string]any{"ApplicationName": name, "ApplicationARN": arn, "ApplicationStatus": "READY", "RuntimeEnvironment": first(req.Input, "RuntimeEnvironment")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "kav2").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"ApplicationDetail": rec}}, nil
	case "DescribeApplication":
		b, ok, _ := p.col(req, "kav2").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ApplicationDetail": rec}}, nil
	case "ListApplications":
		return listWrap(ctx, p.col(req, "kav2"), "ApplicationSummaries")
	case "DeleteApplication":
		_ = p.col(req, "kav2").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartApplication":
		return &spi.Response{Output: map[string]any{}}, nil
	case "StopApplication":
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.kinesisanalyticsv2", req.Operation, "emulate")
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
