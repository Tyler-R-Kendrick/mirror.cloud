package prim

import (
	"fmt"
	"strings"
)

// This is the query half of the OpenSearch data plane, ported verbatim from
// the pack it replaces (including the first-pair-wins iteration over term and
// match maps, which is nondeterministic for multi-key queries on both sides).
// Substring matching is case-insensitive everywhere except the fallthrough,
// which is the pack's exact asymmetry, not a transcription slip.
func init() {
	Register(Func{
		Name:    "opensearch.match",
		Version: 1,
		Call: func(args []any) (any, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("opensearch.match takes query and source, got %d arguments", len(args))
			}
			return matchQuery(args[0], args[1]), nil
		},
	})
}

func matchQuery(q, src any) bool {
	if q == nil {
		return true
	}
	qm, ok := q.(map[string]any)
	if !ok {
		return strings.Contains(fmt.Sprint(src), fmt.Sprint(q))
	}
	if _, ok := qm["match_all"]; ok || len(qm) == 0 {
		return true
	}
	if term, ok := qm["term"].(map[string]any); ok {
		for k, v := range term {
			return fmt.Sprint(field(src, k)) == fmt.Sprint(v)
		}
	}
	if m, ok := qm["match"].(map[string]any); ok {
		for k, v := range m {
			return strings.Contains(strings.ToLower(fmt.Sprint(field(src, k))), strings.ToLower(fmt.Sprint(v)))
		}
	}
	if qs, ok := qm["query_string"].(map[string]any); ok {
		return strings.Contains(strings.ToLower(fmt.Sprint(src)), strings.ToLower(fmt.Sprint(qs["query"])))
	}
	return strings.Contains(fmt.Sprint(src), fmt.Sprint(q))
}

func field(src any, key string) any {
	m, ok := src.(map[string]any)
	if !ok {
		return nil
	}
	return m[key]
}
