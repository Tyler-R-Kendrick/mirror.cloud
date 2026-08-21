// Package xray stores trace segments and groups (no sampling daemon).
package xray

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.xray", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements X-Ray-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.xray" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"PutTraceSegments", "BatchGetTraces", "GetTraceSummaries", "GetServiceGraph",
		"CreateGroup", "GetGroup", "GetGroups", "UpdateGroup", "DeleteGroup",
		"PutTelemetryRecords", "GetSamplingRules",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "PutTraceSegments":
		segs, _ := req.Input["TraceSegmentDocuments"].([]any)
		var ids []any
		for _, s := range segs {
			raw, _ := s.(string)
			var doc map[string]any
			if json.Unmarshal([]byte(raw), &doc) != nil {
				doc = map[string]any{"document": raw}
			}
			tid := first(doc, "trace_id", "id")
			if tid == "" {
				tid = p.deps.Rand.Hex(16)
			}
			doc["trace_id"] = tid
			b, _ := json.Marshal(doc)
			_ = p.col(req, "xray").Put(ctx, tid, b)
			ids = append(ids, tid)
		}
		return &spi.Response{Output: map[string]any{"UnprocessedTraceSegments": []any{}, "TraceIds": ids}}, nil
	case "BatchGetTraces":
		ids := stringList(req.Input["TraceIds"])
		var traces []any
		for _, id := range ids {
			b, ok, _ := p.col(req, "xray").Get(ctx, id)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			traces = append(traces, map[string]any{"Id": id, "Segments": []any{rec}})
		}
		return &spi.Response{Output: map[string]any{"Traces": traces, "UnprocessedTraceIds": []any{}}}, nil
	case "GetTraceSummaries":
		kvs, _, _ := p.col(req, "xray").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, map[string]any{"Id": kv.Key, "Duration": rec["end_time"]})
		}
		return &spi.Response{Output: map[string]any{"TraceSummaries": items}}, nil
	case "GetServiceGraph":
		kvs, _, _ := p.col(req, "xray").List(ctx, "", "", 0)
		var services []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			name := first(rec, "name")
			if name == "" {
				name = kv.Key
			}
			services = append(services, map[string]any{"Name": name, "Type": rec["origin"]})
		}
		return &spi.Response{Output: map[string]any{"Services": services}}, nil
	case "CreateGroup":
		name := first(req.Input, "GroupName")
		arn := "arn:aws:xray:" + req.Identity.Region + ":" + req.Identity.Account + ":group/" + name
		rec := map[string]any{"GroupName": name, "GroupARN": arn, "FilterExpression": first(req.Input, "FilterExpression")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "xrayg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Group": rec}}, nil
	case "GetGroup":
		name := first(req.Input, "GroupName")
		b, ok, _ := p.col(req, "xrayg").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Group": rec}}, nil
	case "GetGroups":
		kvs, _, _ := p.col(req, "xrayg").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Groups": items}}, nil
	case "UpdateGroup":
		name := first(req.Input, "GroupName")
		b, ok, _ := p.col(req, "xrayg").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if f := first(req.Input, "FilterExpression"); f != "" {
			rec["FilterExpression"] = f
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "xrayg").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"Group": rec}}, nil
	case "DeleteGroup":
		_ = p.col(req, "xrayg").Delete(ctx, first(req.Input, "GroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutTelemetryRecords":
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetSamplingRules":
		return &spi.Response{Output: map[string]any{"SamplingRuleRecords": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.xray", req.Operation, "emulate")
	}
}

func stringList(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	}
	return nil
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
