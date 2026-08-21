// Package pipes stores EventBridge Pipes records (no source polling or target invoke).
package pipes

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.pipes", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EventBridge Pipes-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.pipes" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreatePipe", "DescribePipe", "ListPipes", "UpdatePipe", "DeletePipe",
		"StartPipe", "StopPipe",
		"TagResource", "UntagResource", "ListTagsForResource",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	switch req.Operation {
	case "CreatePipe":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:pipes:" + req.Identity.Region + ":" + req.Identity.Account + ":pipe/" + name
		rec := map[string]any{
			"Name": name, "Arn": arn, "Source": first(req.Input, "Source"), "Target": first(req.Input, "Target"),
			"RoleArn": first(req.Input, "RoleArn"), "CurrentState": "RUNNING", "DesiredState": "RUNNING",
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "pipe").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Name": name, "Arn": arn, "CurrentState": "RUNNING"}}, nil
	case "DescribePipe":
		b, ok, _ := p.col(req, "pipe").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListPipes":
		kvs, _, _ := p.col(req, "pipe").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Pipes": items}}, nil
	case "UpdatePipe":
		b, ok, _ := p.col(req, "pipe").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if t := first(req.Input, "Target"); t != "" {
			rec["Target"] = t
		}
		if r := first(req.Input, "RoleArn"); r != "" {
			rec["RoleArn"] = r
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "pipe").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"Name": name, "Arn": rec["Arn"], "CurrentState": rec["CurrentState"]}}, nil
	case "DeletePipe":
		_ = p.col(req, "pipe").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartPipe", "StopPipe":
		b, ok, _ := p.col(req, "pipe").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		state := "RUNNING"
		if req.Operation == "StopPipe" {
			state = "STOPPED"
		}
		rec["CurrentState"] = state
		rec["DesiredState"] = state
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "pipe").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"Name": name, "Arn": rec["Arn"], "CurrentState": state}}, nil
	case "TagResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		b, _ := json.Marshal(req.Input["tags"])
		_ = p.col(req, "pipetag").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		_ = p.col(req, "pipetag").Delete(ctx, first(req.Input, "resourceArn", "ResourceArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "pipetag").Get(ctx, first(req.Input, "resourceArn", "ResourceArn"))
		var tags any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"tags": tags}}, nil
	default:
		return nil, spi.NotImplemented("aws.pipes", req.Operation, "emulate")
	}
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
