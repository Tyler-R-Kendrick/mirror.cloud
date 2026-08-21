// Package emr stores EMR clusters and steps (no Hadoop/YARN).
package emr

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.elasticmapreduce", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EMR-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.elasticmapreduce" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"RunJobFlow", "DescribeCluster", "ListClusters", "TerminateJobFlows",
		"AddJobFlowSteps", "ListSteps", "DescribeStep", "SetTerminationProtection",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "RunJobFlow":
		id := "j-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "Status": map[string]any{"State": "WAITING"}}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "emr").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"JobFlowId": id}}, nil
	case "DescribeCluster":
		id := first(req.Input, "ClusterId")
		b, ok, _ := p.col(req, "emr").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Cluster": rec}}, nil
	case "ListClusters":
		kvs, _, _ := p.col(req, "emr").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Clusters": items}}, nil
	case "TerminateJobFlows":
		ids := stringList(req.Input["JobFlowIds"])
		for _, id := range ids {
			b, ok, _ := p.col(req, "emr").Get(ctx, id)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["Status"] = map[string]any{"State": "TERMINATED"}
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "emr").Put(ctx, id, nb)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "AddJobFlowSteps":
		cid := first(req.Input, "JobFlowId")
		sid := "s-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": sid, "ClusterId": cid, "Status": map[string]any{"State": "COMPLETED"}}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "emrstep:"+cid).Put(ctx, sid, b)
		return &spi.Response{Output: map[string]any{"StepIds": []any{sid}}}, nil
	case "ListSteps":
		return listCol(ctx, p.col(req, "emrstep:"+first(req.Input, "ClusterId")), "Steps")
	case "DescribeStep":
		b, ok, _ := p.col(req, "emrstep:"+first(req.Input, "ClusterId")).Get(ctx, first(req.Input, "StepId"))
		if !ok {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Step": rec}}, nil
	case "SetTerminationProtection":
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.elasticmapreduce", req.Operation, "emulate")
	}
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
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
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
