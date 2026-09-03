package expr

import "testing"

func FuzzEvalBool(f *testing.F) {
	f.Add("attribute_exists(a)", "a")
	f.Add("n > :n AND begins_with(s, :s)", "x")
	f.Add("NOT (a = :x OR b IN (:x))", "")
	f.Fuzz(func(t *testing.T, expression, key string) {
		item := map[string]any{}
		if key != "" {
			item[key] = map[string]any{"S": "v"}
		}
		_, _ = EvalBool(expression, item, nil, map[string]any{":n": map[string]any{"N": "1"}, ":s": map[string]any{"S": "v"}, ":x": map[string]any{"S": "v"}})
	})
}

func FuzzApplyUpdate(f *testing.F) {
	f.Add("SET a = :x")
	f.Add("REMOVE a")
	f.Add("SET a = if_not_exists(a, :x)")
	f.Add("SET a = :x, b = :x")
	f.Fuzz(func(t *testing.T, expression string) {
		item := map[string]any{"a": map[string]any{"N": "1"}}
		_, _ = ApplyUpdate(expression, item, nil, map[string]any{":x": map[string]any{"N": "2"}})
	})
}
