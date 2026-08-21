// Package mediaconvert stores queues, templates, and jobs (no transcode).
package mediaconvert

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.mediaconvert", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MediaConvert-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.mediaconvert" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateQueue", "GetQueue", "ListQueues", "DeleteQueue",
		"CreateJobTemplate", "GetJobTemplate", "ListJobTemplates", "DeleteJobTemplate",
		"CreateJob", "GetJob", "ListJobs", "CancelJob",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateQueue":
		name := first(req.Input, "Name")
		if name == "" {
			return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"Name": name, "Status": "ACTIVE", "Arn": "arn:aws:mediaconvert:" + req.Identity.Region + ":" + req.Identity.Account + ":queues/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mcq").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Queue": rec}}, nil
	case "GetQueue":
		return wrapGet(ctx, p.col(req, "mcq"), first(req.Input, "Name"), "Queue")
	case "ListQueues":
		return listWrap(ctx, p.col(req, "mcq"), "Queues")
	case "DeleteQueue":
		_ = p.col(req, "mcq").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateJobTemplate":
		name := first(req.Input, "Name")
		rec := map[string]any{"Name": name, "Arn": "arn:aws:mediaconvert:" + req.Identity.Region + ":" + req.Identity.Account + ":jobTemplates/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mctpl").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"JobTemplate": rec}}, nil
	case "GetJobTemplate":
		return wrapGet(ctx, p.col(req, "mctpl"), first(req.Input, "Name"), "JobTemplate")
	case "ListJobTemplates":
		return listWrap(ctx, p.col(req, "mctpl"), "JobTemplates")
	case "DeleteJobTemplate":
		_ = p.col(req, "mctpl").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"Id": id, "Status": "COMPLETE", "Queue": first(req.Input, "Queue"),
			"Role": first(req.Input, "Role"), "JobTemplate": first(req.Input, "JobTemplate"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mcjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Job": rec}}, nil
	case "GetJob":
		return wrapGet(ctx, p.col(req, "mcjob"), first(req.Input, "Id"), "Job")
	case "ListJobs":
		return listWrap(ctx, p.col(req, "mcjob"), "Jobs")
	case "CancelJob":
		id := first(req.Input, "Id")
		b, ok, _ := p.col(req, "mcjob").Get(ctx, id)
		rec := map[string]any{"Id": id, "Status": "CANCELED"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["Status"] = "CANCELED"
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "mcjob").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"Job": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.mediaconvert", req.Operation, "emulate")
	}
}

func wrapGet(ctx context.Context, c spi.Collection, id, wrap string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
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
