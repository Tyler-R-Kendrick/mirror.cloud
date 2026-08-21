// Package mwaa stores Airflow environment records (no scheduler or workers).
package mwaa

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.mwaa", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MWAA-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.mwaa" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateEnvironment", "GetEnvironment", "ListEnvironments", "UpdateEnvironment", "DeleteEnvironment",
		"CreateCliToken",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	switch req.Operation {
	case "CreateEnvironment", "UpdateEnvironment":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:airflow:" + req.Identity.Region + ":" + req.Identity.Account + ":environment/" + name
		rec := map[string]any{"Name": name, "Arn": arn, "Status": "AVAILABLE", "AirflowVersion": first(req.Input, "AirflowVersion")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mwaaenv").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Arn": arn}}, nil
	case "GetEnvironment":
		b, ok, _ := p.col(req, "mwaaenv").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Environment": rec}}, nil
	case "ListEnvironments":
		kvs, _, _ := p.col(req, "mwaaenv").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"Environments": names}}, nil
	case "DeleteEnvironment":
		_ = p.col(req, "mwaaenv").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateCliToken":
		return &spi.Response{Output: map[string]any{"CliToken": p.deps.Rand.Hex(16), "WebServerHostname": name + ".airflow.local"}}, nil
	default:
		return nil, spi.NotImplemented("aws.mwaa", req.Operation, "emulate")
	}
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
