package spi

import (
	"context"
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

// Members is the list the query protocol spells name.member.1, name.member.2 and on, in that order, to
// the first index missing: a gap ends the list, as the query decoder's (awsquery's listAt) does.
func Members(in map[string]any, name string) []any {
	values := []any{}
	for i := 1; ; i++ {
		v, ok := in[name+".member."+strconv.Itoa(i)]
		if !ok {
			return values
		}
		values = append(values, v)
	}
}

// Decode answers raw's JSON as a T: the read every stored record's reader makes, its zero value when raw
// holds no T, as each reader that dropped the error left it.
func Decode[T any](raw []byte) T {
	var v T
	_ = json.Unmarshal(raw, &v)
	return v
}

// Entries answers c's List: its entries with the given key prefix, in key order, after after, at most limit
// (none for limit <= 0); none when the list fails, as each caller that dropped the error read it.
func Entries(ctx context.Context, c Collection, prefix, after string, limit int) []KV {
	kvs, _, _ := c.List(ctx, prefix, after, limit)
	return kvs
}

// GetJSON reads the JSON object c holds under key: whether c holds it, and the error of the read or of the
// decode. A value that is not an object is the decode's error, and not held; a JSON null is no object, held,
// and no error.
func GetJSON(ctx context.Context, c Collection, key string) (map[string]any, bool, error) {
	raw, ok, err := c.Get(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	var rec map[string]any
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

// ReadJSON decodes into v the JSON c holds under key, when c holds any, and answers whether it does: v
// keeps what it held for what the JSON does not set (a map is merged into), and a decode error is dropped,
// as each reader that kept a default and overlaid the record on it read it.
func ReadJSON(ctx context.Context, c Collection, key string, v any) bool {
	raw, ok, _ := c.Get(ctx, key)
	if ok {
		_ = json.Unmarshal(raw, v)
	}
	return ok
}

// Fold answers m's value under key, or else under a key equal to it under Unicode case folding: a
// column a query names in any case. With several such keys, which one answers is unspecified.
func Fold(m map[string]string, key string) string {
	if v, ok := m[key]; ok {
		return v
	}
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

// PutJSON stores v in c under key as its JSON: the encode and the write every
// record store makes, and the error of either.
func PutJSON(ctx context.Context, c Collection, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Put(ctx, key, raw)
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
