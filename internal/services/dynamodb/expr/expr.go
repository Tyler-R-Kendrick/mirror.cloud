// Package expr is a DynamoDB expression lexer/parser/evaluator.
package expr

import (
	"fmt"
	"strconv"
	"strings"
)

// EvalBool evaluates a condition/filter expression against item.
func EvalBool(expr string, item, names, values map[string]any) (bool, error) {
	expr = substNames(expr, names)
	p := parse(expr)
	v, err := eval(p, item, values)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	return b, nil
}

// ApplyUpdate applies an UpdateExpression to item in place.
func ApplyUpdate(expr string, item, names, values map[string]any) error {
	expr = substNames(expr, names)
	parts := splitTop(expr)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "SET "):
			for _, assign := range strings.Split(strings.TrimPrefix(part, "SET "), ",") {
				kv := strings.SplitN(assign, "=", 2)
				if len(kv) != 2 {
					continue
				}
				name := strings.TrimSpace(kv[0])
				val, err := eval(parse(strings.TrimSpace(kv[1])), item, values)
				if err != nil {
					return err
				}
				item[name] = toAttr(val)
			}
		case strings.HasPrefix(part, "REMOVE "):
			for _, name := range strings.Split(strings.TrimPrefix(part, "REMOVE "), ",") {
				delete(item, strings.TrimSpace(name))
			}
		case strings.HasPrefix(part, "ADD "):
			// documented numeric/set add; store as-is for v1
		case strings.HasPrefix(part, "DELETE "):
		}
	}
	return nil
}

func substNames(s string, names map[string]any) string {
	for k, v := range names {
		if str, ok := v.(string); ok {
			s = strings.ReplaceAll(s, k, str)
		}
	}
	return s
}

type node struct {
	op    string
	val   string
	kids  []node
	isVal bool
}

func parse(s string) node {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		return parse(s[1 : len(s)-1])
	}
	for _, op := range []string{" OR ", " AND "} {
		if i := strings.Index(s, op); i >= 0 {
			return node{op: strings.TrimSpace(op), kids: []node{parse(s[:i]), parse(s[i+len(op):])}}
		}
	}
	if strings.HasPrefix(s, "NOT ") {
		return node{op: "NOT", kids: []node{parse(strings.TrimPrefix(s, "NOT "))}}
	}
	for _, op := range []string{"<>", "<=", ">=", "=", "<", ">"} {
		if i := strings.Index(s, op); i >= 0 {
			return node{op: op, kids: []node{parse(s[:i]), parse(s[i+len(op):])}}
		}
	}
	if strings.Contains(s, " BETWEEN ") {
		parts := strings.SplitN(s, " BETWEEN ", 2)
		and := strings.SplitN(parts[1], " AND ", 2)
		if len(and) == 2 {
			return node{op: "BETWEEN", kids: []node{parse(parts[0]), parse(and[0]), parse(and[1])}}
		}
	}
	if i := strings.Index(s, "("); i > 0 && strings.HasSuffix(s, ")") {
		fn := s[:i]
		arg := s[i+1 : len(s)-1]
		var kids []node
		for _, a := range splitArgs(arg) {
			kids = append(kids, parse(a))
		}
		return node{op: fn, kids: kids}
	}
	return node{val: strings.TrimSpace(s), isVal: true}
}

func eval(n node, item, values map[string]any) (any, error) {
	if n.isVal {
		if strings.HasPrefix(n.val, ":") {
			return unwrap(values[n.val]), nil
		}
		if v, ok := item[n.val]; ok {
			return unwrap(v), nil
		}
		if n.val == "true" {
			return true, nil
		}
		if n.val == "false" {
			return false, nil
		}
		if f, err := strconvParse(n.val); err == nil {
			return f, nil
		}
		return n.val, nil
	}
	kid := func(i int) node {
		if i >= 0 && i < len(n.kids) {
			return n.kids[i]
		}
		return node{isVal: true}
	}
	switch n.op {
	case "AND":
		if len(n.kids) < 2 {
			return false, fmt.Errorf("AND needs two operands")
		}
		a, err := eval(kid(0), item, values)
		if err != nil {
			return nil, err
		}
		b, err := eval(kid(1), item, values)
		if err != nil {
			return nil, err
		}
		ab, _ := a.(bool)
		bb, _ := b.(bool)
		return ab && bb, nil
	case "OR":
		if len(n.kids) < 2 {
			return false, fmt.Errorf("OR needs two operands")
		}
		a, _ := eval(kid(0), item, values)
		b, _ := eval(kid(1), item, values)
		ab, _ := a.(bool)
		bb, _ := b.(bool)
		return ab || bb, nil
	case "NOT":
		if len(n.kids) < 1 {
			return false, fmt.Errorf("NOT needs one operand")
		}
		a, _ := eval(kid(0), item, values)
		ab, _ := a.(bool)
		return !ab, nil
	case "=", "<>", "<", ">", "<=", ">=":
		a, _ := eval(kid(0), item, values)
		b, _ := eval(kid(1), item, values)
		cmp := compare(a, b)
		switch n.op {
		case "=":
			return cmp == 0, nil
		case "<>":
			return cmp != 0, nil
		case "<":
			return cmp < 0, nil
		case ">":
			return cmp > 0, nil
		case "<=":
			return cmp <= 0, nil
		case ">=":
			return cmp >= 0, nil
		}
	case "BETWEEN":
		v, _ := eval(kid(0), item, values)
		lo, _ := eval(kid(1), item, values)
		hi, _ := eval(kid(2), item, values)
		return compare(v, lo) >= 0 && compare(v, hi) <= 0, nil
	case "attribute_exists":
		name, _ := eval(kid(0), item, values)
		_, ok := item[fmt.Sprint(name)]
		return ok, nil
	case "attribute_not_exists":
		name, _ := eval(kid(0), item, values)
		_, ok := item[fmt.Sprint(name)]
		return !ok, nil
	case "begins_with":
		a, _ := eval(kid(0), item, values)
		b, _ := eval(kid(1), item, values)
		return strings.HasPrefix(fmt.Sprint(a), fmt.Sprint(b)), nil
	case "contains":
		a, _ := eval(kid(0), item, values)
		b, _ := eval(kid(1), item, values)
		return strings.Contains(fmt.Sprint(a), fmt.Sprint(b)), nil
	case "size":
		a, _ := eval(kid(0), item, values)
		return float64(len(fmt.Sprint(a))), nil
	case "if_not_exists":
		a, _ := eval(kid(0), item, values)
		if a == nil || a == "" {
			return eval(kid(1), item, values)
		}
		return a, nil
	case "list_append":
		return eval(kid(1), item, values)
	}
	return nil, fmt.Errorf("unsupported expr op %s", n.op)
}

func unwrap(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	if s, ok := m["S"].(string); ok {
		return s
	}
	if n, ok := m["N"].(string); ok {
		f, _ := strconvParse(n)
		return f
	}
	if b, ok := m["BOOL"].(bool); ok {
		return b
	}
	return v
}

func toAttr(v any) any {
	switch t := v.(type) {
	case string:
		return map[string]any{"S": t}
	case float64:
		return map[string]any{"N": strconv.FormatFloat(t, 'f', -1, 64)}
	case bool:
		return map[string]any{"BOOL": t}
	default:
		return v
	}
}

func compare(a, b any) int {
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		return strings.Compare(as, bs)
	}
	af, _ := toFloat(a)
	bf, _ := toFloat(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case string:
		f, err := strconvParse(t)
		return f, err == nil
	}
	return 0, false
}

func strconvParse(s string) (float64, error) { return strconv.ParseFloat(s, 64) }

func splitTop(s string) []string {
	var out []string
	depth := 0
	last := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && i+1 < len(s) {
			rest := s[i:]
			if strings.HasPrefix(rest, " SET ") || strings.HasPrefix(rest, " REMOVE ") || strings.HasPrefix(rest, " ADD ") || strings.HasPrefix(rest, " DELETE ") {
				out = append(out, s[last:i])
				last = i + 1
			}
		}
	}
	out = append(out, s[last:])
	if len(out) == 1 {
		return []string{s}
	}
	return out
}

func splitArgs(s string) []string {
	var out []string
	depth := 0
	last := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[last:i]))
				last = i + 1
			}
		}
	}
	out = append(out, strings.TrimSpace(s[last:]))
	return out
}
