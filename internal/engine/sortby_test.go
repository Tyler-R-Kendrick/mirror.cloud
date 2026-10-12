package engine

import (
	"reflect"
	"testing"
	"time"

	"cel.dev/cel-go/cel"
)

// evalCEL compiles and runs one expression against the runtime functions, with
// no bundle behind it, so a helper is tested as the helper it is.
func evalCEL(t *testing.T, src string, vars map[string]any) any {
	t.Helper()
	names := make([]string, 0, len(vars))
	for k := range vars {
		names = append(names, k)
	}
	env, err := runtimeEnv(names)
	if err != nil {
		t.Fatal(err)
	}
	ast, iss := env.Compile(src)
	if iss.Err() != nil {
		t.Fatalf("%s: %v", src, iss.Err())
	}
	prg, err := env.Program(ast, cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		t.Fatal(err)
	}
	act := map[string]any{
		"input": map[string]any{}, "identity": map[string]any{}, "endpoint": "",
		"headers": map[string]string{}, "has_http": false, "now_ms": int64(0),
	}
	act["now"] = timeZero()
	for k, v := range vars {
		act[k] = v
	}
	out, _, err := prg.Eval(act)
	if err != nil {
		t.Fatalf("%s: %v", src, err)
	}
	return fromCEL(out)
}

func TestSha1hexIsTheDigestAFileIsNamedBy(t *testing.T) {
	// The well-known SHA-1 of "hello", which is what `vercel` names an
	// uploaded file by (x-vercel-digest) and a deployment refers to it by.
	got := evalCEL(t, `sha1hex('hello')`, nil)
	if got != "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d" {
		t.Fatalf("sha1hex('hello') = %v", got)
	}
}

func TestSortByOrdersRecordsByOneMember(t *testing.T) {
	rows := []any{
		map[string]any{"id": "b", "n": float64(2)},
		map[string]any{"id": "a", "n": float64(10)},
		map[string]any{"id": "c"},
		map[string]any{"id": "d", "n": float64(2)},
	}
	ids := func(v any) []string {
		var out []string
		for _, e := range v.([]any) {
			out = append(out, e.(map[string]any)["id"].(string))
		}
		return out
	}
	// Numbers sort numerically (10 after 2, not before it as text would), ties
	// keep their listed order, and a record without the member goes last
	// whichever way the sort runs.
	if got := ids(evalCEL(t, `sortBy(rows, 'n', false)`, map[string]any{"rows": rows})); !reflect.DeepEqual(got, []string{"b", "d", "a", "c"}) {
		t.Errorf("ascending = %v", got)
	}
	if got := ids(evalCEL(t, `sortBy(rows, 'n', true)`, map[string]any{"rows": rows})); !reflect.DeepEqual(got, []string{"a", "b", "d", "c"}) {
		t.Errorf("descending = %v", got)
	}
	if got := ids(evalCEL(t, `sortBy([], 'n', true)`, nil)); len(got) != 0 {
		t.Errorf("empty = %v", got)
	}
}

func timeZero() time.Time { return time.Unix(0, 0).UTC() }
