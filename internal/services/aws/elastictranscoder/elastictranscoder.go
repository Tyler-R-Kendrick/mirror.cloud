// Package elastictranscoder stores pipeline and job records (no transcode).
package elastictranscoder

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.elastictranscoder", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Elastic Transcoder-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.elastictranscoder" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreatePipeline", "ReadPipeline", "ListPipelines", "DeletePipeline",
		"CreateJob", "ReadJob", "ListJobsByPipeline",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreatePipeline":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "Status": "Active", "Arn": "arn:aws:elastictranscoder:" + req.Identity.Region + ":" + req.Identity.Account + ":pipeline/" + id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "etpipe").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Pipeline": rec}}, nil
	case "ReadPipeline":
		id := first(req.Input, "Id")
		b, ok, _ := p.col(req, "etpipe").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Pipeline": rec}}, nil
	case "ListPipelines":
		return listWrap(ctx, p.col(req, "etpipe"), "Pipelines")
	case "DeletePipeline":
		_ = p.col(req, "etpipe").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{"Success": true}}, nil
	case "CreateJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "PipelineId": first(req.Input, "PipelineId"), "Status": "Complete"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "etjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Job": rec}}, nil
	case "ReadJob":
		id := first(req.Input, "Id")
		b, ok, _ := p.col(req, "etjob").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Job": rec}}, nil
	case "ListJobsByPipeline":
		return listWrap(ctx, p.col(req, "etjob"), "Jobs")
	default:
		return nil, spi.NotImplemented("aws.elastictranscoder", req.Operation, "emulate")
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
