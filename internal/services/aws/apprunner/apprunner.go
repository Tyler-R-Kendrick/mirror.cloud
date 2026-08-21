// Package apprunner stores service records (no container deploy).
package apprunner

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.apprunner", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements App Runner-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.apprunner" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateService", "DescribeService", "ListServices", "DeleteService", "PauseService", "ResumeService"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func lastSlash(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "ServiceName")
	if name == "" {
		name = lastSlash(first(req.Input, "ServiceArn"))
	}
	switch req.Operation {
	case "CreateService":
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:apprunner:" + req.Identity.Region + ":" + req.Identity.Account + ":service/" + name
		rec := map[string]any{"ServiceName": name, "ServiceArn": arn, "Status": "RUNNING", "ServiceId": p.deps.Rand.Hex(8)}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "arsvc").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Service": rec}}, nil
	case "DescribeService":
		b, ok, _ := p.col(req, "arsvc").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Service": rec}}, nil
	case "ListServices":
		return listWrap(ctx, p.col(req, "arsvc"), "ServiceSummaryList")
	case "DeleteService":
		_ = p.col(req, "arsvc").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PauseService":
		return &spi.Response{Output: map[string]any{"Service": map[string]any{"Status": "PAUSED"}}}, nil
	case "ResumeService":
		return &spi.Response{Output: map[string]any{"Service": map[string]any{"Status": "RUNNING"}}}, nil
	default:
		return nil, spi.NotImplemented("aws.apprunner", req.Operation, "emulate")
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
