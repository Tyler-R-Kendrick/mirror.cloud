// Package dms stores replication instances, endpoints, and tasks (no CDC engine).
package dms

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.dms", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements DMS-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.dms" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateReplicationInstance", "DescribeReplicationInstances", "DeleteReplicationInstance",
		"CreateEndpoint", "DescribeEndpoints", "DeleteEndpoint",
		"CreateReplicationTask", "DescribeReplicationTasks", "StartReplicationTask", "StopReplicationTask", "DeleteReplicationTask",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateReplicationInstance":
		id := first(req.Input, "ReplicationInstanceIdentifier")
		if id == "" {
			return nil, &spi.Fault{Code: "InvalidParameterValueException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"ReplicationInstanceIdentifier": id, "ReplicationInstanceStatus": "available",
			"ReplicationInstanceArn":   "arn:aws:dms:" + req.Identity.Region + ":" + req.Identity.Account + ":rep:" + id,
			"ReplicationInstanceClass": first(req.Input, "ReplicationInstanceClass"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dmsri").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ReplicationInstance": rec}}, nil
	case "DescribeReplicationInstances":
		return listOrGet(ctx, p.col(req, "dmsri"), first(req.Input, "ReplicationInstanceIdentifier"), "ReplicationInstances")
	case "DeleteReplicationInstance":
		id := first(req.Input, "ReplicationInstanceIdentifier")
		_ = p.col(req, "dmsri").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"ReplicationInstance": map[string]any{"ReplicationInstanceIdentifier": id, "ReplicationInstanceStatus": "deleting"}}}, nil
	case "CreateEndpoint":
		id := first(req.Input, "EndpointIdentifier")
		if id == "" {
			return nil, &spi.Fault{Code: "InvalidParameterValueException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"EndpointIdentifier": id, "Status": "active", "EngineName": first(req.Input, "EngineName"),
			"EndpointArn": "arn:aws:dms:" + req.Identity.Region + ":" + req.Identity.Account + ":endpoint:" + id,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dmsep").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Endpoint": rec}}, nil
	case "DescribeEndpoints":
		return listOrGet(ctx, p.col(req, "dmsep"), first(req.Input, "EndpointIdentifier"), "Endpoints")
	case "DeleteEndpoint":
		id := first(req.Input, "EndpointIdentifier")
		_ = p.col(req, "dmsep").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"Endpoint": map[string]any{"EndpointIdentifier": id, "Status": "deleting"}}}, nil
	case "CreateReplicationTask":
		id := first(req.Input, "ReplicationTaskIdentifier")
		if id == "" {
			return nil, &spi.Fault{Code: "InvalidParameterValueException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"ReplicationTaskIdentifier": id, "Status": "ready",
			"ReplicationTaskArn": "arn:aws:dms:" + req.Identity.Region + ":" + req.Identity.Account + ":task:" + id,
			"MigrationType":      first(req.Input, "MigrationType"),
			"SourceEndpointArn":  first(req.Input, "SourceEndpointArn"),
			"TargetEndpointArn":  first(req.Input, "TargetEndpointArn"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "dmstk").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ReplicationTask": rec}}, nil
	case "DescribeReplicationTasks":
		return listOrGet(ctx, p.col(req, "dmstk"), first(req.Input, "ReplicationTaskIdentifier"), "ReplicationTasks")
	case "StartReplicationTask":
		return p.patchTask(ctx, req, "running")
	case "StopReplicationTask":
		return p.patchTask(ctx, req, "stopped")
	case "DeleteReplicationTask":
		id := first(req.Input, "ReplicationTaskIdentifier")
		_ = p.col(req, "dmstk").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"ReplicationTask": map[string]any{"ReplicationTaskIdentifier": id, "Status": "deleting"}}}, nil
	default:
		return nil, spi.NotImplemented("aws.dms", req.Operation, "emulate")
	}
}

func (p *Pack) patchTask(ctx context.Context, req *spi.Request, status string) (*spi.Response, error) {
	id := first(req.Input, "ReplicationTaskIdentifier")
	b, ok, _ := p.col(req, "dmstk").Get(ctx, id)
	rec := map[string]any{"ReplicationTaskIdentifier": id}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	rec["Status"] = status
	nb, _ := json.Marshal(rec)
	_ = p.col(req, "dmstk").Put(ctx, id, nb)
	return &spi.Response{Output: map[string]any{"ReplicationTask": rec}}, nil
}

func listOrGet(ctx context.Context, c spi.Collection, want, key string) (*spi.Response, error) {
	if want != "" {
		b, ok, _ := c.Get(ctx, want)
		if !ok {
			return &spi.Response{Output: map[string]any{key: []any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{key: []any{rec}}}, nil
	}
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
