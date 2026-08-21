// Package codepipeline stores pipelines and executions (no CodeBuild/CodeDeploy run).
package codepipeline

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.codepipeline", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CodePipeline-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.codepipeline" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreatePipeline", "GetPipeline", "GetPipelineState", "ListPipelines", "UpdatePipeline", "DeletePipeline",
		"StartPipelineExecution", "GetPipelineExecution", "ListPipelineExecutions", "StopPipelineExecution",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreatePipeline", "UpdatePipeline":
		pl, _ := req.Input["pipeline"].(map[string]any)
		name := first(pl, "name", "Name")
		if name == "" {
			name = first(req.Input, "name", "Name")
		}
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		if pl == nil {
			pl = map[string]any{"name": name}
		}
		pl["name"] = name
		b, _ := json.Marshal(pl)
		_ = p.col(req, "cp").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"pipeline": pl}}, nil
	case "GetPipeline":
		name := first(req.Input, "name", "Name")
		b, ok, _ := p.col(req, "cp").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "PipelineNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"pipeline": rec}}, nil
	case "GetPipelineState":
		name := first(req.Input, "name", "Name")
		b, ok, _ := p.col(req, "cp").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "PipelineNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"pipelineName": rec["name"], "stageStates": []any{}}}, nil
	case "ListPipelines":
		kvs, _, _ := p.col(req, "cp").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, map[string]any{"name": rec["name"]})
		}
		return &spi.Response{Output: map[string]any{"pipelines": items}}, nil
	case "DeletePipeline":
		_ = p.col(req, "cp").Delete(ctx, first(req.Input, "name", "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartPipelineExecution":
		name := first(req.Input, "name", "Name")
		id := name + "-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"pipelineName": name, "pipelineExecutionId": id, "status": "Succeeded"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cpex").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"pipelineExecutionId": id}}, nil
	case "GetPipelineExecution":
		id := first(req.Input, "pipelineExecutionId")
		b, ok, _ := p.col(req, "cpex").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "PipelineExecutionNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"pipelineExecution": rec}}, nil
	case "ListPipelineExecutions":
		kvs, _, _ := p.col(req, "cpex").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"pipelineExecutionSummaries": items}}, nil
	case "StopPipelineExecution":
		id := first(req.Input, "pipelineExecutionId")
		b, ok, _ := p.col(req, "cpex").Get(ctx, id)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["status"] = "Stopped"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "cpex").Put(ctx, id, nb)
		}
		return &spi.Response{Output: map[string]any{"pipelineExecutionId": id}}, nil
	default:
		return nil, spi.NotImplemented("aws.codepipeline", req.Operation, "emulate")
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
