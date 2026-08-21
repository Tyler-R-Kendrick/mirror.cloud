package expr

import (
	"encoding/json"
	"strings"
	"testing"
)

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
	if _, err := ApplyUpdate("SET a = :x", item, nil, vals); err != nil {
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

func TestQueryOperatorsAndUpdate(t *testing.T) {
	item := map[string]any{"id": map[string]any{"S": "abc"}, "n": map[string]any{"N": "5"}, "tags": map[string]any{"SS": []any{"a"}}}
	vals := map[string]any{
		":lo": map[string]any{"N": "1"}, ":hi": map[string]any{"N": "9"}, ":p": map[string]any{"S": "ab"},
		":one": map[string]any{"N": "1"}, ":t": map[string]any{"SS": []any{"b"}}, ":x": map[string]any{"S": "abc"},
	}
	must := func(e string, want bool) {
		t.Helper()
		ok, err := EvalBool(e, item, nil, vals)
		if err != nil || ok != want {
			t.Fatalf("%s -> %v %v want %v", e, ok, err, want)
		}
	}
	must("n BETWEEN :lo AND :hi", true)
	must("begins_with(id, :p)", true)
	must("(id = :x) OR (id = :p)", true)
	must("NOT (n < :lo)", true)
	must("n > :lo", true)
	must("n < :hi", true)
	must("id IN (:x, :p)", true)
	must("attribute_type(id, S)", true)
	if _, err := ApplyUpdate("ADD n :one", item, nil, vals); err != nil {
		t.Fatal(err)
	}
	if fmtN(item["n"]) != "6" {
		t.Fatalf("ADD %v", item["n"])
	}
	if _, err := ApplyUpdate("ADD tags :t", item, nil, vals); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyUpdate("DELETE tags :t", item, nil, vals); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyUpdate("SET n = n + :one", item, nil, vals); err != nil {
		t.Fatal(err)
	}
	proj := Project("id, n", item, nil)
	if proj["id"] == nil || proj["tags"] != nil {
		t.Fatalf("project %v", proj)
	}
	nested := map[string]any{"id": map[string]any{"S": "nest"}, "doc": map[string]any{"M": map[string]any{"k": map[string]any{"S": "v"}, "hide": map[string]any{"S": "no"}}}}
	doc := nested["doc"].(map[string]any)
	inner, ok := doc["M"].(map[string]any)
	if !ok {
		t.Fatalf("M type %T", doc["M"])
	}
	if inner["k"] == nil {
		t.Fatalf("no k in %v", inner)
	}
	top := Project("doc", nested, nil)
	if top["doc"] == nil {
		t.Fatalf("project doc top %v", top)
	}
	np := Project("doc.k", nested, nil)
	raw, _ := json.Marshal(np)
	if !strings.Contains(string(raw), `"v"`) || strings.Contains(string(raw), "hide") {
		t.Fatalf("nested project %s top=%s (doc keys %v inner %v)", raw, mustJSON(top), keysOf(doc), keysOf(inner))
	}
}

func keysOf(m map[string]any) []string {
	var k []string
	for x := range m {
		k = append(k, x)
	}
	return k
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
