package expr

import "testing"

func TestEvalBool(t *testing.T) {
	item := map[string]any{"n": map[string]any{"N": "5"}, "s": map[string]any{"S": "abc"}}
	vals := map[string]any{":n": map[string]any{"N": "3"}, ":s": map[string]any{"S": "ab"}}
	ok, err := EvalBool("n > :n AND begins_with(s, :s)", item, nil, vals)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected true")
	}
	ok, err = EvalBool("attribute_not_exists(missing)", item, nil, nil)
	if err != nil || !ok {
		t.Fatalf("attr %v %v", ok, err)
	}
}

func TestANDNotOR(t *testing.T) {
	item := map[string]any{"n": map[string]any{"N": "5"}}
	ok, err := EvalBool("attribute_exists(n) AND attribute_exists(missing)", item, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("AND must fail when the second operand is false")
	}
}

func TestEquals(t *testing.T) {
	item := map[string]any{"n": map[string]any{"N": "5"}}
	vals := map[string]any{":n": map[string]any{"N": "5"}, ":m": map[string]any{"N": "7"}}
	ok, err := EvalBool("n = :n", item, nil, vals)
	if err != nil || !ok {
		t.Fatalf("equal %v %v", ok, err)
	}
	ok, err = EvalBool("n = :m", item, nil, vals)
	if err != nil || ok {
		t.Fatalf("not equal %v %v", ok, err)
	}
}

func TestApplyUpdateSET(t *testing.T) {
	item := map[string]any{"a": map[string]any{"N": "1"}}
	vals := map[string]any{":x": map[string]any{"N": "2"}}
	if err := ApplyUpdate("SET a = :x", item, nil, vals); err != nil {
		t.Fatal(err)
	}
	if item["a"].(map[string]any)["N"] != "2" && fmtN(item["a"]) != "2" {
		t.Fatalf("%v", item)
	}
}

func fmtN(v any) string {
	m, _ := v.(map[string]any)
	s, _ := m["N"].(string)
	return s
}
