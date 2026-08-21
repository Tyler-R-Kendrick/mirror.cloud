// Package workspaces stores workspace records (no VDI or directory).
package workspaces

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.workspaces", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements WorkSpaces-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.workspaces" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateWorkspaces", "DescribeWorkspaces", "StopWorkspaces", "StartWorkspaces",
		"RebootWorkspaces", "TerminateWorkspaces", "ModifyWorkspaceState",
		"DescribeWorkspaceBundles", "DescribeWorkspaceDirectories",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateWorkspaces":
		item := wsItem(req.Input)
		id := "ws-" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"WorkspaceId": id, "State": "AVAILABLE",
			"UserName": first(item, "UserName"), "DirectoryId": first(item, "DirectoryId"), "BundleId": first(item, "BundleId"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ws").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"PendingRequests": []any{rec}}}, nil
	case "DescribeWorkspaces":
		id := firstID(req.Input, "WorkspaceId", "WorkspaceIds")
		return listOrGet(ctx, p.col(req, "ws"), id, "Workspaces")
	case "StopWorkspaces", "StartWorkspaces", "RebootWorkspaces", "ModifyWorkspaceState":
		id := firstID(req.Input, "WorkspaceId", "WorkspaceIds")
		state := map[string]string{"StopWorkspaces": "STOPPED", "StartWorkspaces": "AVAILABLE", "RebootWorkspaces": "AVAILABLE", "ModifyWorkspaceState": first(req.Input, "WorkspaceState")}[req.Operation]
		if state == "" {
			state = "AVAILABLE"
		}
		b, ok, _ := p.col(req, "ws").Get(ctx, id)
		rec := map[string]any{"WorkspaceId": id, "State": state}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["State"] = state
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ws").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "TerminateWorkspaces":
		_ = p.col(req, "ws").Delete(ctx, firstID(req.Input, "WorkspaceId", "WorkspaceIds"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeWorkspaceBundles":
		return &spi.Response{Output: map[string]any{"Bundles": []any{}}}, nil
	case "DescribeWorkspaceDirectories":
		return &spi.Response{Output: map[string]any{"Directories": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.workspaces", req.Operation, "emulate")
	}
}

func wsItem(in map[string]any) map[string]any {
	if arr, ok := in["Workspaces"].([]any); ok && len(arr) > 0 {
		if m, ok := arr[0].(map[string]any); ok {
			return m
		}
	}
	return in
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
	return listWrap(ctx, c, key)
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

func firstID(in map[string]any, scalar, list string) string {
	if s := first(in, scalar); s != "" {
		return s
	}
	if arr, ok := in[list].([]any); ok && len(arr) > 0 {
		if s, ok := arr[0].(string); ok {
			return s
		}
		if m, ok := arr[0].(map[string]any); ok {
			return first(m, scalar, "WorkspaceId")
		}
	}
	return ""
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
