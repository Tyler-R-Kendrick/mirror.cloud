package vercel

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// oasDoc is a document read for the one question the conformance test asks of
// it: does this response body satisfy the schema the document declares for this
// operation and status?
//
// It is a recursive checker over the subset of OpenAPI 3.0 the vendored Vercel
// document actually uses (a survey of components.schemas finds type, properties,
// required, enum, nullable, items, additionalProperties, oneOf, anyOf, allOf,
// pattern, min/max and two formats) and nothing else. A JSON Schema library
// would be a new module in go.mod for a document this plain, and the repository
// keeps its dependency list short on purpose.
//
// Two choices are deliberate leniencies, and both are stated here so a reader
// does not mistake them for oversights:
//
//   - oneOf holds when at least one variant matches, not exactly one. The
//     document's deployment response is a oneOf of a reduced view and the full
//     view, and a full body satisfies both, which is a flaw in how the
//     document expresses the union and not a flaw in the body.
//   - format is checked only for date-time and uri, the two the document uses.
type oasDoc struct {
	name string
	root map[string]any
}

func loadOAS(t *testing.T, file string) *oasDoc {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "specs", "vercel", file))
	if err != nil {
		t.Fatal(err)
	}
	d := &oasDoc{name: file}
	if err := json.Unmarshal(raw, &d.root); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	return d
}

// deref follows a $ref chain. A value that is not an object is returned as an
// empty schema, which accepts anything.
func (d *oasDoc) deref(v any) map[string]any {
	for i := 0; i < 64; i++ {
		m, ok := v.(map[string]any)
		if !ok {
			return map[string]any{}
		}
		ref, ok := m["$ref"].(string)
		if !ok {
			return m
		}
		cur := any(d.root)
		for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
			part = strings.NewReplacer("~1", "/", "~0", "~").Replace(part)
			next, ok := cur.(map[string]any)
			if !ok {
				return map[string]any{}
			}
			cur = next[part]
		}
		v = cur
	}
	return map[string]any{}
}

// operation finds the operation object for a method and path template.
func (d *oasDoc) operation(method, path string) (map[string]any, bool) {
	paths, _ := d.root["paths"].(map[string]any)
	item, _ := paths[path].(map[string]any)
	op, ok := item[strings.ToLower(method)].(map[string]any)
	return op, ok
}

// responseSchema answers the schema the document declares for a status and
// whether the document declares that status (or a default) at all.
func (d *oasDoc) responseSchema(op map[string]any, status int) (schema any, declared bool) {
	responses, _ := op["responses"].(map[string]any)
	resp, ok := responses[fmt.Sprint(status)]
	if !ok {
		resp, ok = responses["default"]
	}
	if !ok {
		return nil, false
	}
	content, _ := d.deref(resp)["content"].(map[string]any)
	for _, media := range []string{"application/json"} {
		if m, ok := content[media].(map[string]any); ok {
			return m["schema"], true
		}
	}
	for name, m := range content {
		if strings.Contains(name, "json") {
			if mm, ok := m.(map[string]any); ok {
				return mm["schema"], true
			}
		}
	}
	return nil, true
}

// validate returns every way inst fails schema, as "path: reason" lines. An
// empty list means it conforms.
func (d *oasDoc) validate(schema any, inst any) []string {
	var out []string
	d.check(schema, inst, "$", 0, &out)
	return out
}

func (d *oasDoc) check(schema any, inst any, path string, depth int, out *[]string) {
	if depth > 200 {
		*out = append(*out, path+": schema nesting too deep to follow")
		return
	}
	s := d.deref(schema)
	if len(s) == 0 {
		return
	}
	if inst == nil {
		if nullable, _ := s["nullable"].(bool); nullable {
			return
		}
		if enum, ok := s["enum"].([]any); ok {
			for _, e := range enum {
				if e == nil {
					return
				}
			}
		}
	}
	if allOf, ok := s["allOf"].([]any); ok {
		for _, sub := range allOf {
			d.check(sub, inst, path, depth+1, out)
		}
	}
	for _, key := range []string{"oneOf", "anyOf"} {
		variants, ok := s[key].([]any)
		if !ok || len(variants) == 0 {
			continue
		}
		var best []string
		matched := false
		for _, sub := range variants {
			var errs []string
			d.check(sub, inst, path, depth+1, &errs)
			if len(errs) == 0 {
				matched = true
				break
			}
			if best == nil || len(errs) < len(best) {
				best = errs
			}
		}
		if !matched {
			*out = append(*out, fmt.Sprintf("%s: matches none of the %s variants; closest failure: %s", path, key, strings.Join(best, "; ")))
		}
	}
	if enum, ok := s["enum"].([]any); ok {
		found := false
		for _, e := range enum {
			if jsonEqual(e, inst) {
				found = true
				break
			}
		}
		if !found {
			*out = append(*out, fmt.Sprintf("%s: %s is not one of %s", path, show(inst), show(enum)))
			return
		}
	}

	typ, _ := s["type"].(string)
	if typ == "" {
		switch {
		case s["properties"] != nil || s["required"] != nil || s["additionalProperties"] != nil:
			typ = "object"
		case s["items"] != nil:
			typ = "array"
		}
	}
	switch typ {
	case "object":
		obj, ok := inst.(map[string]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: want an object, got %s", path, kind(inst)))
			return
		}
		props, _ := s["properties"].(map[string]any)
		if req, ok := s["required"].([]any); ok {
			for _, r := range req {
				name, _ := r.(string)
				v, present := obj[name]
				if !present {
					*out = append(*out, fmt.Sprintf("%s: required member %q is missing", path, name))
					continue
				}
				if v == nil {
					if p, ok := props[name]; ok && !d.acceptsNull(p) {
						*out = append(*out, fmt.Sprintf("%s.%s: is null but the document does not make it nullable", path, name))
					}
				}
			}
		}
		names := make([]string, 0, len(obj))
		for k := range obj {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			if p, ok := props[k]; ok {
				if obj[k] == nil && d.acceptsNull(p) {
					continue
				}
				d.check(p, obj[k], path+"."+k, depth+1, out)
				continue
			}
			switch ap := s["additionalProperties"].(type) {
			case bool:
				if !ap {
					*out = append(*out, fmt.Sprintf("%s: member %q is not declared and the document forbids additional members", path, k))
				}
			case map[string]any:
				d.check(ap, obj[k], path+"."+k, depth+1, out)
			}
		}
		if n, ok := num(s["minProperties"]); ok && float64(len(obj)) < n {
			*out = append(*out, fmt.Sprintf("%s: %d members, want at least %v", path, len(obj), n))
		}
	case "array":
		arr, ok := inst.([]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: want an array, got %s", path, kind(inst)))
			return
		}
		if n, ok := num(s["minItems"]); ok && float64(len(arr)) < n {
			*out = append(*out, fmt.Sprintf("%s: %d items, want at least %v", path, len(arr), n))
		}
		if n, ok := num(s["maxItems"]); ok && float64(len(arr)) > n {
			*out = append(*out, fmt.Sprintf("%s: %d items, want at most %v", path, len(arr), n))
		}
		if items, ok := s["items"]; ok {
			for i, e := range arr {
				d.check(items, e, fmt.Sprintf("%s[%d]", path, i), depth+1, out)
			}
		}
	case "string":
		str, ok := inst.(string)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: want a string, got %s", path, kind(inst)))
			return
		}
		if n, ok := num(s["minLength"]); ok && float64(utf8.RuneCountInString(str)) < n {
			*out = append(*out, fmt.Sprintf("%s: %q is shorter than %v", path, str, n))
		}
		if n, ok := num(s["maxLength"]); ok && float64(utf8.RuneCountInString(str)) > n {
			*out = append(*out, fmt.Sprintf("%s: %q is longer than %v", path, str, n))
		}
		if p, ok := s["pattern"].(string); ok {
			// ECMA patterns the Go engine cannot compile are skipped rather
			// than guessed at.
			if re, err := regexp.Compile(p); err == nil && !re.MatchString(str) {
				*out = append(*out, fmt.Sprintf("%s: %q does not match %s", path, str, p))
			}
		}
		switch s["format"] {
		case "date-time":
			if _, err := time.Parse(time.RFC3339, str); err != nil {
				*out = append(*out, fmt.Sprintf("%s: %q is not an RFC 3339 date-time", path, str))
			}
		case "uri":
			if u, err := url.Parse(str); err != nil || u.Scheme == "" {
				*out = append(*out, fmt.Sprintf("%s: %q is not a URI", path, str))
			}
		}
	case "number", "integer":
		f, ok := inst.(float64)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: want a %s, got %s", path, typ, kind(inst)))
			return
		}
		if typ == "integer" && f != math.Trunc(f) {
			*out = append(*out, fmt.Sprintf("%s: %v is not an integer", path, f))
		}
		if n, ok := num(s["minimum"]); ok {
			excl, _ := s["exclusiveMinimum"].(bool)
			if f < n || (excl && f == n) {
				*out = append(*out, fmt.Sprintf("%s: %v is below the minimum %v", path, f, n))
			}
		}
		if n, ok := num(s["maximum"]); ok && f > n {
			*out = append(*out, fmt.Sprintf("%s: %v is above the maximum %v", path, f, n))
		}
	case "boolean":
		if _, ok := inst.(bool); !ok {
			*out = append(*out, fmt.Sprintf("%s: want a boolean, got %s", path, kind(inst)))
		}
	}
}

// acceptsNull answers whether a schema lets null stand for its value.
func (d *oasDoc) acceptsNull(schema any) bool {
	s := d.deref(schema)
	if n, _ := s["nullable"].(bool); n {
		return true
	}
	if enum, ok := s["enum"].([]any); ok {
		for _, e := range enum {
			if e == nil {
				return true
			}
		}
	}
	for _, key := range []string{"oneOf", "anyOf"} {
		if variants, ok := s[key].([]any); ok {
			for _, v := range variants {
				if d.acceptsNull(v) {
					return true
				}
			}
		}
	}
	return false
}

func num(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

func kind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "a boolean"
	case float64:
		return "a number"
	case string:
		return "a string"
	case []any:
		return "an array"
	case map[string]any:
		return "an object"
	}
	return fmt.Sprintf("%T", v)
}

func show(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	if len(raw) > 120 {
		return string(raw[:117]) + "..."
	}
	return string(raw)
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// TestOASCheckerRejectsWhatItShould pins the checker on small documents, so a
// conformance pass means something: a checker that accepted everything would
// make the whole suite green.
func TestOASCheckerRejectsWhatItShould(t *testing.T) {
	d := &oasDoc{root: map[string]any{"components": map[string]any{"schemas": map[string]any{
		"Thing": map[string]any{
			"type":                 "object",
			"required":             []any{"id", "state"},
			"additionalProperties": false,
			"properties": map[string]any{
				"id":    map[string]any{"type": "string", "pattern": "^thing_"},
				"state": map[string]any{"type": "string", "enum": []any{"A", "B"}},
				"n":     map[string]any{"type": "integer", "minimum": float64(1)},
				"note":  map[string]any{"type": "string", "nullable": true},
				"at":    map[string]any{"type": "string", "format": "date-time"},
				"tags":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			},
		},
	}}}}
	ref := map[string]any{"$ref": "#/components/schemas/Thing"}
	good := map[string]any{"id": "thing_1", "state": "A", "n": float64(2), "note": nil, "at": "2026-01-01T00:00:00Z", "tags": []any{"x"}}
	if errs := d.validate(ref, good); len(errs) != 0 {
		t.Fatalf("a conforming body was rejected: %v", errs)
	}
	for name, mutate := range map[string]func(map[string]any){
		"missing required":   func(m map[string]any) { delete(m, "state") },
		"bad enum":           func(m map[string]any) { m["state"] = "C" },
		"bad pattern":        func(m map[string]any) { m["id"] = "other" },
		"non-integer":        func(m map[string]any) { m["n"] = 1.5 },
		"below minimum":      func(m map[string]any) { m["n"] = float64(0) },
		"null not nullable":  func(m map[string]any) { m["id"] = nil },
		"wrong type":         func(m map[string]any) { m["tags"] = "x" },
		"bad item":           func(m map[string]any) { m["tags"] = []any{float64(1)} },
		"bad date":           func(m map[string]any) { m["at"] = "yesterday" },
		"undeclared member":  func(m map[string]any) { m["extra"] = true },
		"string for a count": func(m map[string]any) { m["n"] = "2" },
	} {
		body := map[string]any{}
		for k, v := range good {
			body[k] = v
		}
		mutate(body)
		if errs := d.validate(ref, body); len(errs) == 0 {
			t.Errorf("%s: the checker accepted %s", name, show(body))
		}
	}
}
