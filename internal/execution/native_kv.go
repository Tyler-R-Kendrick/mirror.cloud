package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// NativeKV serves kv.get/put/delete/list against the same Store collections
// the cloudflare.api B-IR bundle uses (`cfkv:{namespace_id}`). It is the
// default Executor backend so execute effects stay native when no external
// runtime is selected.
type NativeKV struct {
	Store spi.Store
	Clock spi.Clock
}

func (n *NativeKV) Identity() (string, string) { return "native-kv", "1" }

func (n *NativeKV) Describe() []Descriptor {
	return []Descriptor{
		{Name: "kv.get", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectRead, Backend: "native-kv",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "key": {Type: "string", Required: true}, "withMetadata": {Type: "bool"}},
			Output: map[string]Field{"found": {Type: "bool"}, "value": {Type: "string"}, "metadata": {Type: "json"}}},
		{Name: "kv.put", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectWrite, Backend: "native-kv",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "key": {Type: "string", Required: true}, "value": {Type: "string", Required: true}, "expiration": {Type: "int"}, "expirationTtl": {Type: "int"}, "metadata": {Type: "string"}},
			Output: map[string]Field{"stored": {Type: "bool"}}},
		{Name: "kv.delete", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectWrite, Backend: "native-kv",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "key": {Type: "string", Required: true}},
			Output: map[string]Field{"deleted": {Type: "bool"}}},
		{Name: "kv.list", Version: 1, Resource: "entry", Body: BodyNone, Effect: EffectRead, Backend: "native-kv",
			Input:  map[string]Field{"namespace": {Type: "string", Required: true}, "prefix": {Type: "string"}},
			Output: map[string]Field{"keys": {Type: "json"}, "listComplete": {Type: "bool"}}},
		{Name: "r2.put", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectWrite, Backend: "native-kv",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}, "key": {Type: "string", Required: true}, "value": {Type: "string", Required: true}},
			Output: map[string]Field{"stored": {Type: "bool"}}},
		{Name: "r2.get", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectRead, Backend: "native-kv",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}, "key": {Type: "string", Required: true}},
			Output: map[string]Field{"found": {Type: "bool"}, "value": {Type: "string"}}},
		{Name: "r2.delete", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectWrite, Backend: "native-kv",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}, "key": {Type: "string", Required: true}},
			Output: map[string]Field{"deleted": {Type: "bool"}}},
		{Name: "r2.list", Version: 1, Resource: "object", Body: BodyNone, Effect: EffectRead, Backend: "native-kv",
			Input:  map[string]Field{"bucket": {Type: "string", Required: true}},
			Output: map[string]Field{"objects": {Type: "json"}}},
		{Name: "queue.send", Version: 1, Resource: "queue", Body: BodyNone, Effect: EffectWrite, Backend: "native-kv",
			Input:  map[string]Field{"queue": {Type: "string", Required: true}, "body": {Type: "string"}},
			Output: map[string]Field{"sent": {Type: "bool"}}},
		{Name: "d1.query", Version: 1, Resource: "database", Body: BodyNone, Effect: EffectWrite, Backend: "native-kv",
			Input:  map[string]Field{"database": {Type: "string", Required: true}, "sql": {Type: "string", Required: true}, "binds": {Type: "json"}},
			Output: map[string]Field{"results": {Type: "json"}, "success": {Type: "bool"}}},
	}
}

func (n *NativeKV) Call(ctx context.Context, req Request) (Response, error) {
	switch req.Action {
	case "kv.get", "kv.put", "kv.delete", "kv.list":
		ns, _ := req.Args["namespace"].(string)
		if ns == "" {
			return Response{}, &Failure{Class: ClassValidation, Action: req.Action, Detail: "namespace required"}
		}
		col := n.Store.Scope(req.Ref.Account, req.Ref.Region).Collection("cfkv:" + ns)
		switch req.Action {
		case "kv.get":
			return n.get(ctx, col, req)
		case "kv.put":
			return n.put(ctx, col, req)
		case "kv.delete":
			return n.del(ctx, col, req)
		default:
			return n.list(ctx, col, req)
		}
	case "r2.put", "r2.get", "r2.delete", "r2.list":
		return n.r2(ctx, req)
	case "queue.send":
		return n.queueSend(ctx, req)
	case "d1.query":
		// ponytail: native d1 has no SQL engine; empty success. Real SQL: miniflare/celld.
		return Response{Output: map[string]any{"results": []any{}, "success": true}}, nil
	default:
		return Response{}, &Failure{Class: ClassUnsupported, Action: req.Action, Detail: "native-kv closed set"}
	}
}

func (n *NativeKV) get(ctx context.Context, col spi.Collection, req Request) (Response, error) {
	key, _ := req.Args["key"].(string)
	raw, ok, err := col.Get(ctx, key)
	if err != nil {
		return Response{}, err
	}
	if !ok {
		return Response{}, &Failure{Class: ClassAbsent, Action: "kv.get", Detail: "key not found"}
	}
	rec, err := decodeEntry(raw)
	if err != nil {
		return Response{}, err
	}
	if n.expired(rec) {
		return Response{}, &Failure{Class: ClassAbsent, Action: "kv.get", Detail: "key not found"}
	}
	out := map[string]any{"found": true, "value": rec.Value}
	if withMeta, _ := req.Args["withMetadata"].(bool); withMeta && rec.Metadata != "" {
		var meta any
		if json.Unmarshal([]byte(rec.Metadata), &meta) == nil {
			out["metadata"] = meta
		} else {
			out["metadata"] = rec.Metadata
		}
	}
	return Response{Output: out}, nil
}

func (n *NativeKV) put(ctx context.Context, col spi.Collection, req Request) (Response, error) {
	key, _ := req.Args["key"].(string)
	val, _ := req.Args["value"].(string)
	rec := entryRec{Name: key, Value: val}
	if exp, ok := asInt(req.Args["expiration"]); ok && exp > 0 {
		rec.Expiration = exp
	} else if ttl, ok := asInt(req.Args["expirationTtl"]); ok && ttl > 0 {
		rec.Expiration = n.Clock.Now().Unix() + ttl
	}
	if m, ok := req.Args["metadata"].(string); ok {
		rec.Metadata = m
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return Response{}, err
	}
	if err := col.Put(ctx, key, raw); err != nil {
		return Response{}, err
	}
	return Response{Output: map[string]any{"stored": true}}, nil
}

func (n *NativeKV) del(ctx context.Context, col spi.Collection, req Request) (Response, error) {
	key, _ := req.Args["key"].(string)
	raw, ok, err := col.Get(ctx, key)
	if err != nil {
		return Response{}, err
	}
	if !ok {
		return Response{}, &Failure{Class: ClassAbsent, Action: "kv.delete", Detail: "key not found"}
	}
	rec, err := decodeEntry(raw)
	if err != nil {
		return Response{}, err
	}
	if n.expired(rec) {
		return Response{}, &Failure{Class: ClassAbsent, Action: "kv.delete", Detail: "key not found"}
	}
	if err := col.Delete(ctx, key); err != nil {
		return Response{}, err
	}
	return Response{Output: map[string]any{"deleted": true}}, nil
}

func (n *NativeKV) list(ctx context.Context, col spi.Collection, req Request) (Response, error) {
	prefix, _ := req.Args["prefix"].(string)
	entries, _, err := col.List(ctx, prefix, "", 0)
	if err != nil {
		return Response{}, err
	}
	keys := make([]any, 0, len(entries))
	for _, e := range entries {
		rec, err := decodeEntry(e.Value)
		if err != nil || n.expired(rec) {
			continue
		}
		item := map[string]any{"name": rec.Name}
		if rec.Name == "" {
			item["name"] = e.Key
		}
		if rec.Expiration > 0 {
			item["expiration"] = rec.Expiration
		}
		if rec.Metadata != "" {
			var meta any
			if json.Unmarshal([]byte(rec.Metadata), &meta) == nil {
				item["metadata"] = meta
			}
		}
		keys = append(keys, item)
	}
	return Response{Output: map[string]any{"keys": keys, "listComplete": true}}, nil
}

type entryRec struct {
	Name       string `json:"name"`
	Value      string `json:"value"`
	Expiration int64  `json:"expiration"`
	Metadata   string `json:"metadata"`
}

func decodeEntry(raw []byte) (entryRec, error) {
	var rec entryRec
	if err := json.Unmarshal(raw, &rec); err != nil {
		return rec, fmt.Errorf("native-kv: decode entry: %w", err)
	}
	return rec, nil
}

func (n *NativeKV) expired(rec entryRec) bool {
	if rec.Expiration <= 0 || n.Clock == nil {
		return false
	}
	return rec.Expiration <= n.Clock.Now().Unix()
}

func asInt(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int64:
		return t, true
	case float64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n, err == nil
	}
	return 0, false
}

// RegistryExecutor adapts a Registry to spi.Executor.
type RegistryExecutor struct {
	Reg *Registry
}

func (e RegistryExecutor) Execute(ctx context.Context, account, region, action string, args map[string]any) (map[string]any, error) {
	kind := "entry"
	if e.Reg != nil {
		if d, ok := e.Reg.Lookup(action); ok {
			kind = d.Resource
		}
	}
	resp, err := e.Reg.Dispatch(ctx, Request{
		Ref:    Ref{Environment: "local", Account: account, Region: region, Kind: kind, ID: action},
		Action: action,
		Args:   args,
	})
	if err != nil {
		return nil, err
	}
	return resp.Output, nil
}

func (n *NativeKV) r2(ctx context.Context, req Request) (Response, error) {
	bucket, _ := req.Args["bucket"].(string)
	if bucket == "" {
		return Response{}, &Failure{Class: ClassValidation, Action: req.Action, Detail: "bucket required"}
	}
	col := n.Store.Scope(req.Ref.Account, req.Ref.Region).Collection("cfr2:" + bucket)
	key, _ := req.Args["key"].(string)
	switch req.Action {
	case "r2.put":
		val, _ := req.Args["value"].(string)
		if key == "" {
			return Response{}, &Failure{Class: ClassValidation, Action: req.Action, Detail: "key required"}
		}
		if err := col.Put(ctx, key, []byte(val)); err != nil {
			return Response{}, err
		}
		return Response{Output: map[string]any{"stored": true}}, nil
	case "r2.get":
		raw, ok, err := col.Get(ctx, key)
		if err != nil {
			return Response{}, err
		}
		if !ok {
			return Response{}, &Failure{Class: ClassAbsent, Action: "r2.get", Detail: "object not found"}
		}
		return Response{Output: map[string]any{"found": true, "value": string(raw)}}, nil
	case "r2.delete":
		if err := col.Delete(ctx, key); err != nil {
			return Response{}, err
		}
		return Response{Output: map[string]any{"deleted": true}}, nil
	case "r2.list":
		entries, _, err := col.List(ctx, "", "", 0)
		if err != nil {
			return Response{}, err
		}
		objs := make([]any, 0, len(entries))
		for _, e := range entries {
			objs = append(objs, map[string]any{"key": e.Key})
		}
		return Response{Output: map[string]any{"objects": objs}}, nil
	default:
		return Response{}, &Failure{Class: ClassUnsupported, Action: req.Action, Detail: "r2"}
	}
}

func (n *NativeKV) queueSend(ctx context.Context, req Request) (Response, error) {
	q, _ := req.Args["queue"].(string)
	if q == "" {
		return Response{}, &Failure{Class: ClassValidation, Action: req.Action, Detail: "queue required"}
	}
	col := n.Store.Scope(req.Ref.Account, req.Ref.Region).Collection("cfq:" + q)
	body, _ := req.Args["body"].(string)
	id := fmt.Sprintf("%d", n.Clock.Now().UnixNano())
	if err := col.Put(ctx, id, []byte(body)); err != nil {
		return Response{}, err
	}
	return Response{Output: map[string]any{"sent": true}}, nil
}
