package execution

import (
	"context"
	"strings"
)

// MiniflareBackend adapts a live MiniflareSession to the Backend contract so
// B-IR execute effects and Worker bindings hit the same workerd KV objects.
type MiniflareBackend struct {
	Session *MiniflareSession
}

func (m *MiniflareBackend) Identity() (string, string) {
	id := m.Session.Identity()
	name, ver, _ := strings.Cut(id, "/")
	if name == "" {
		name = "miniflare"
	}
	if ver == "" {
		ver = "unknown"
	}
	return name, ver
}

func (m *MiniflareBackend) Describe() []Descriptor {
	return []Descriptor{
		{Name: "kv.get", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectRead, Backend: "miniflare",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "key": {Type: "string", Required: true}, "withMetadata": {Type: "bool"}},
			Output: map[string]Field{"found": {Type: "bool"}, "value": {Type: "string"}, "metadata": {Type: "json"}}},
		{Name: "kv.put", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectWrite, Backend: "miniflare",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "key": {Type: "string", Required: true}, "value": {Type: "string", Required: true}, "expiration": {Type: "int"}, "expirationTtl": {Type: "int"}, "metadata": {Type: "json"}},
			Output: map[string]Field{"stored": {Type: "bool"}}},
		{Name: "kv.delete", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectWrite, Backend: "miniflare",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "key": {Type: "string", Required: true}},
			Output: map[string]Field{"deleted": {Type: "bool"}}},
		{Name: "kv.list", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectRead, Backend: "miniflare",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "prefix": {Type: "string"}},
			Output: map[string]Field{"keys": {Type: "json"}, "listComplete": {Type: "bool"}}},
		{Name: "d1.query", Version: 1, Resource: "database", Body: BodyNone, Effect: EffectWrite, Backend: "miniflare",
			Input:  map[string]Field{"database": {Type: "string", Required: true}, "sql": {Type: "string", Required: true}, "binds": {Type: "json"}},
			Output: map[string]Field{"results": {Type: "json"}, "success": {Type: "bool"}}},
		{Name: "r2.put", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectWrite, Backend: "miniflare",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}, "key": {Type: "string", Required: true}, "value": {Type: "string", Required: true}},
			Output: map[string]Field{"stored": {Type: "bool"}}},
		{Name: "r2.get", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectRead, Backend: "miniflare",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}, "key": {Type: "string", Required: true}},
			Output: map[string]Field{"found": {Type: "bool"}, "value": {Type: "string"}}},
		{Name: "r2.delete", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectWrite, Backend: "miniflare",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}, "key": {Type: "string", Required: true}},
			Output: map[string]Field{"deleted": {Type: "bool"}}},
		{Name: "r2.list", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectRead, Backend: "miniflare",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}},
			Output: map[string]Field{"objects": {Type: "json"}}},
		{Name: "queue.send", Version: 1, Resource: "queue", Body: BodyNone, Effect: EffectWrite, Backend: "miniflare",
			Input:  map[string]Field{"queue": {Type: "string", Required: true}, "body": {Type: "string"}},
			Output: map[string]Field{"sent": {Type: "bool"}}},
	}
}

func (m *MiniflareBackend) Call(ctx context.Context, req Request) (Response, error) {
	args := map[string]any{}
	for k, v := range req.Args {
		args[k] = v
	}
	// Miniflare rejects expirationTtl/expiration of 0; omit "no deadline".
	if ttl, ok := asInt(args["expirationTtl"]); !ok || ttl <= 0 {
		delete(args, "expirationTtl")
	} else {
		args["expirationTtl"] = ttl
	}
	if exp, ok := asInt(args["expiration"]); !ok || exp <= 0 {
		delete(args, "expiration")
	} else {
		args["expiration"] = exp
	}
	if m, ok := args["metadata"].(string); ok && m == "" {
		delete(args, "metadata")
	}
	out, err := m.Session.Call(ctx, req.Action, args)
	if err != nil {
		return Response{}, err
	}
	if req.Action == "kv.get" || req.Action == "r2.get" {
		if found, _ := out["found"].(bool); !found {
			return Response{}, &Failure{Class: ClassAbsent, Action: req.Action, Detail: "not found"}
		}
	}
	if req.Action == "kv.list" {
		// Normalize listComplete naming if helper used list_complete.
		if _, ok := out["listComplete"]; !ok {
			if v, ok := out["list_complete"]; ok {
				out["listComplete"] = v
			}
		}
	}
	return Response{Output: out}, nil
}

// Ensure MiniflareBackend is a Backend.
var _ Backend = (*MiniflareBackend)(nil)
