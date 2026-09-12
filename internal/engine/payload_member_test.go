package engine_test

import (
	"testing"
)

// A payload member whose shape is a structure or a union IS the request body:
// the codec decodes that body's members into the input, so the member itself
// is never present in it. Vercel is the first service whose document marks
// such a member required -- the env create's body is a union of free-form
// documents and the KV command's is an array -- and requiring the member
// would fail every one of those calls before a rule ran, so the model check
// skips it for exactly the non-scalar case.
func TestAStructuredPayloadMemberIsTheBodyNotAnInput(t *testing.T) {
	p := served(t, "vercel.api")
	invoke(t, p, "CreateProject", map[string]any{"name": "app"})
	// No `body` member: the request carries the body's members, as the codec
	// would have decoded them. A rejected-as-missing body is this test failing.
	out := invoke(t, p, "CreateProjectEnv", map[string]any{"idOrName": "app", "key": "FOO", "value": "bar"})
	created, _ := out["created"].(map[string]any)
	if created["key"] != "FOO" {
		t.Fatalf("env create answered %#v", out)
	}

	kv := served(t, "vercel.kv")
	got := invoke(t, kv, "Command", map[string]any{"_redis": []any{"SET", "k", "v"}})
	if got["result"] != "OK" {
		t.Fatalf("kv command answered %#v", got)
	}
}
