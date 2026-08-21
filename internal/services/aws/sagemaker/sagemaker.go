// Package sagemaker stores notebooks, models, and endpoints (no training/inference).
package sagemaker

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.sagemaker", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements SageMaker-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.sagemaker" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateNotebookInstance", "DescribeNotebookInstance", "ListNotebookInstances", "DeleteNotebookInstance",
		"CreateModel", "DescribeModel", "ListModels", "DeleteModel",
		"CreateEndpointConfig", "DescribeEndpointConfig", "DeleteEndpointConfig",
		"CreateEndpoint", "DescribeEndpoint", "ListEndpoints", "DeleteEndpoint",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateNotebookInstance":
		name := first(req.Input, "NotebookInstanceName")
		rec := map[string]any{"NotebookInstanceName": name, "NotebookInstanceStatus": "InService", "InstanceType": first(req.Input, "InstanceType")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "smnb").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"NotebookInstanceArn": "arn:aws:sagemaker:" + req.Identity.Region + ":" + req.Identity.Account + ":notebook-instance/" + name}}, nil
	case "DescribeNotebookInstance":
		return get(ctx, p.col(req, "smnb"), first(req.Input, "NotebookInstanceName"))
	case "ListNotebookInstances":
		return listWrap(ctx, p.col(req, "smnb"), "NotebookInstances")
	case "DeleteNotebookInstance":
		_ = p.col(req, "smnb").Delete(ctx, first(req.Input, "NotebookInstanceName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateModel":
		name := first(req.Input, "ModelName")
		rec := map[string]any{"ModelName": name, "ExecutionRoleArn": first(req.Input, "ExecutionRoleArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "smmodel").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"ModelArn": "arn:aws:sagemaker:" + req.Identity.Region + ":" + req.Identity.Account + ":model/" + name}}, nil
	case "DescribeModel":
		return get(ctx, p.col(req, "smmodel"), first(req.Input, "ModelName"))
	case "ListModels":
		return listWrap(ctx, p.col(req, "smmodel"), "Models")
	case "DeleteModel":
		_ = p.col(req, "smmodel").Delete(ctx, first(req.Input, "ModelName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEndpointConfig":
		name := first(req.Input, "EndpointConfigName")
		rec := map[string]any{"EndpointConfigName": name, "ProductionVariants": req.Input["ProductionVariants"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "smec").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"EndpointConfigArn": "arn:aws:sagemaker:" + req.Identity.Region + ":" + req.Identity.Account + ":endpoint-config/" + name}}, nil
	case "DescribeEndpointConfig":
		return get(ctx, p.col(req, "smec"), first(req.Input, "EndpointConfigName"))
	case "DeleteEndpointConfig":
		_ = p.col(req, "smec").Delete(ctx, first(req.Input, "EndpointConfigName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEndpoint":
		name := first(req.Input, "EndpointName")
		rec := map[string]any{"EndpointName": name, "EndpointStatus": "InService", "EndpointConfigName": first(req.Input, "EndpointConfigName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "smep").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"EndpointArn": "arn:aws:sagemaker:" + req.Identity.Region + ":" + req.Identity.Account + ":endpoint/" + name}}, nil
	case "DescribeEndpoint":
		return get(ctx, p.col(req, "smep"), first(req.Input, "EndpointName"))
	case "ListEndpoints":
		return listWrap(ctx, p.col(req, "smep"), "Endpoints")
	case "DeleteEndpoint":
		_ = p.col(req, "smep").Delete(ctx, first(req.Input, "EndpointName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.sagemaker", req.Operation, "emulate")
	}
}

func get(ctx context.Context, c spi.Collection, id string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
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
