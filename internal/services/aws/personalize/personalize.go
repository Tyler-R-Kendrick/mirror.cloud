// Package personalize stores dataset groups and solutions (no ML recommendations).
package personalize

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.personalize", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Personalize-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.personalize" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDatasetGroup", "DescribeDatasetGroup", "ListDatasetGroups", "DeleteDatasetGroup",
		"CreateSolution", "DescribeSolution", "DeleteSolution",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateDatasetGroup":
		name := first(req.Input, "name", "Name")
		arn := "arn:aws:personalize:" + req.Identity.Region + ":" + req.Identity.Account + ":dataset-group/" + name
		rec := map[string]any{"name": name, "datasetGroupArn": arn, "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "psdg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"datasetGroupArn": arn}}, nil
	case "DescribeDatasetGroup":
		name := lastSlash(first(req.Input, "datasetGroupArn"))
		b, ok, _ := p.col(req, "psdg").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"datasetGroup": rec}}, nil
	case "ListDatasetGroups":
		return listWrap(ctx, p.col(req, "psdg"), "datasetGroups")
	case "DeleteDatasetGroup":
		_ = p.col(req, "psdg").Delete(ctx, lastSlash(first(req.Input, "datasetGroupArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateSolution":
		name := first(req.Input, "name", "Name")
		arn := "arn:aws:personalize:" + req.Identity.Region + ":" + req.Identity.Account + ":solution/" + name
		rec := map[string]any{"name": name, "solutionArn": arn, "status": "ACTIVE", "datasetGroupArn": first(req.Input, "datasetGroupArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "pssol").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"solutionArn": arn}}, nil
	case "DescribeSolution":
		name := lastSlash(first(req.Input, "solutionArn"))
		b, ok, _ := p.col(req, "pssol").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"solution": rec}}, nil
	case "DeleteSolution":
		_ = p.col(req, "pssol").Delete(ctx, lastSlash(first(req.Input, "solutionArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.personalize", req.Operation, "emulate")
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
