package spi

import (
	"encoding/json"
	"strconv"
	"strings"
)

// The value helpers below read the loosely typed members a request or a
// stored record carries. Packs share them here instead of each keeping its
// own copy.

// First answers the first of keys whose member in m is a non-empty string, or
// "" when none is.
func First(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// Str answers v when it is a string, and "" otherwise.
func Str(v any) string {
	s, _ := v.(string)
	return s
}

// Slice answers v when it is a list, and nil otherwise.
func Slice(v any) []any {
	s, _ := v.([]any)
	return s
}

// Object answers v when it is a map, and an empty map otherwise, never nil.
func Object(v any) map[string]any {
	if m, ok := v.(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

// Clone answers a shallow copy of m, never nil.
func Clone(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// LastSlash answers what follows the last "/" in s, or s when it has none.
func LastSlash(s string) string { return s[strings.LastIndex(s, "/")+1:] }

// JSONText answers v's text: a string as it is, anything else as JSON.
func JSONText(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	raw, _ := json.Marshal(v)
	return string(raw)
}

// Int answers v as an int when it is an int or a float64 (truncated), and 0
// otherwise.
func Int(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return 0
}

// JSONObject answers raw decoded as a JSON object, and nil when it is none: a
// stored record read back.
func JSONObject(raw []byte) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

// Integer answers v as an int: an int, int64 or float64 (truncated), or a
// string's decimal digits; 0 for anything else.
func Integer(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

// Members is the list the query protocol spells name.member.1, name.member.2 and on, in that order.
func Members(in map[string]any, name string) []any {
	by := map[int]any{}
	for k, v := range in {
		if n, ok := strings.CutPrefix(k, name+".member."); ok {
			if i, err := strconv.Atoi(n); err == nil {
				by[i] = v
			}
		}
	}
	values := make([]any, 0, len(by))
	for i := 1; i <= len(by); i++ {
		if value, ok := by[i]; ok {
			values = append(values, value)
		}
	}
	return values
}
