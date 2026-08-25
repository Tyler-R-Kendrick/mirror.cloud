package states

import "testing"

func FuzzJSONPath(f *testing.F) {
	for _, path := range []string{"$", "$.items.length()", "$..items.length()", "$.items.length(1)", "$[?(@.price < 10)]"} {
		f.Add(path)
	}
	data := map[string]any{"items": []any{map[string]any{"price": 5.0}}}
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 1024 {
			t.Skip()
		}
		if _, found := jsonPathLookup(data, path); found && path != "" && path[0] == '$' && !validJSONPath(path, false) {
			t.Fatalf("lookup accepted invalid path %q", path)
		}
	})
}
