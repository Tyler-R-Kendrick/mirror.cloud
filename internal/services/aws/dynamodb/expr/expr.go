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

// ApplyUpdate applies an UpdateExpression to item in place and returns touched names.
func ApplyUpdate(expr string, item, names, values map[string]any) ([]string, error) {
	expr = substNames(expr, names)
	parts := splitTop(expr)
	var touched []string
	touch := func(n string) { touched = append(touched, n) }
	for _, part := range parts {
		part = strings.TrimSpace(part)
		switch {
		case strings.HasPrefix(part, "SET "):
			for _, assign := range splitArgs(strings.TrimPrefix(part, "SET ")) {
				kv := strings.SplitN(assign, "=", 2)
				if len(kv) != 2 {
					continue
				}
				name := strings.TrimSpace(kv[0])
				val, err := eval(parse(strings.TrimSpace(kv[1])), item, values)
				if err != nil {
					return nil, err
				}
				item[name] = toAttr(val)
				touch(name)
			}
		case strings.HasPrefix(part, "REMOVE "):
			for _, name := range splitArgs(strings.TrimPrefix(part, "REMOVE ")) {
				name = strings.TrimSpace(name)
				delete(item, name)
				touch(name)
			}
		case strings.HasPrefix(part, "ADD "):
			for _, pair := range splitArgs(strings.TrimPrefix(part, "ADD ")) {
				fs := strings.Fields(strings.TrimSpace(pair))
				if len(fs) < 2 {
					continue
				}
				name, rhs := fs[0], strings.Join(fs[1:], " ")
				delta, err := eval(parse(rhs), item, values)
				if err != nil {
					return nil, err
				}
				item[name] = addAttr(item[name], delta)
				touch(name)
			}
		case strings.HasPrefix(part, "DELETE "):
			for _, pair := range splitArgs(strings.TrimPrefix(part, "DELETE ")) {
				fs := strings.Fields(strings.TrimSpace(pair))
				if len(fs) < 2 {
					continue
				}
				name, rhs := fs[0], strings.Join(fs[1:], " ")
				delta, err := eval(parse(rhs), item, values)
				if err != nil {
					return nil, err
				}
				item[name] = deleteFromSet(item[name], delta)
				touch(name)
			}
		}
	}
	return touched, nil
}

// Project keeps only paths listed in a ProjectionExpression.
func Project(expr string, item, names map[string]any) map[string]any {
	if strings.TrimSpace(expr) == "" {
		return item
	}
	expr = substNames(expr, names)
	out := map[string]any{}
	for _, part := range splitArgs(expr) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if v, ok := walkPath(item, part); ok {
			setPath(out, part, v)
		}
	}
	return out
}

func substNames(s string, names map[string]any) string {
	for k, v := range names {
		if str, ok := v.(string); ok {
			s = strings.ReplaceAll(s, k, str)
		}
	}
	return s
}

func indexTop(s, op string) int {
	depth := 0
	for i := 0; i+len(op) <= len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && s[i:i+len(op)] == op {
			return i
		}
	}
	return -1
}

func unwrapParens(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return s, false
	}
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return s, false
			}
		}
	}
	if depth != 0 {
		return s, false
	}
	return strings.TrimSpace(s[1 : len(s)-1]), true
}

type node struct {
	op    string
	val   string
	kids  []node
	isVal bool
}

func parse(s string) node {
	s = strings.TrimSpace(s)
	for {
		inner, ok := unwrapParens(s)
		if !ok {
			break
		}
		s = inner
	}
	if i := indexTop(s, " BETWEEN "); i >= 0 {
		left, rest := s[:i], s[i+len(" BETWEEN "):]
		if j := indexTop(rest, " AND "); j >= 0 {
			return node{op: "BETWEEN", kids: []node{parse(left), parse(rest[:j]), parse(rest[j+len(" AND "):])}}
		}
	}
	for _, op := range []string{" OR ", " AND "} {
		if i := indexTop(s, op); i >= 0 {
			return node{op: strings.TrimSpace(op), kids: []node{parse(s[:i]), parse(s[i+len(op):])}}
		}
	}
	if strings.HasPrefix(s, "NOT ") {
		return node{op: "NOT", kids: []node{parse(strings.TrimPrefix(s, "NOT "))}}
	}
	if i := indexTop(s, " IN "); i >= 0 {
		inner := strings.TrimSpace(s[i+4:])
		inner = strings.TrimPrefix(inner, "(")
		inner = strings.TrimSuffix(inner, ")")
		kids := []node{parse(s[:i])}
		for _, a := range splitArgs(inner) {
			kids = append(kids, parse(a))
		}
		return node{op: "IN", kids: kids}
	}
	for _, op := range []string{"<>", "<=", ">=", "=", "<", ">"} {
		if i := indexTop(s, op); i >= 0 {
			return node{op: op, kids: []node{parse(s[:i]), parse(s[i+len(op):])}}
		}
	}
	for _, op := range []string{" + ", " - "} {
		if i := indexTop(s, op); i >= 0 {
			return node{op: strings.TrimSpace(op), kids: []node{parse(s[:i]), parse(s[i+len(op):])}}
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
		_, ok := item[kid(0).val]
		return ok, nil
	case "attribute_not_exists":
		_, ok := item[kid(0).val]
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
	case "IN":
		v, _ := eval(kid(0), item, values)
		for i := 1; i < len(n.kids); i++ {
			w, _ := eval(kid(i), item, values)
			if compare(v, w) == 0 {
				return true, nil
			}
		}
		return false, nil
	case "attribute_type":
		name, _ := eval(kid(0), item, values)
		want, _ := eval(kid(1), item, values)
		raw, ok := item[fmt.Sprint(name)]
		if !ok {
			raw = item[n.kids[0].val]
		}
		return attrType(raw) == fmt.Sprint(want), nil
	case "+":
		a, _ := eval(kid(0), item, values)
		b, _ := eval(kid(1), item, values)
		af, _ := toFloat(a)
		bf, _ := toFloat(b)
		return af + bf, nil
	case "-":
		a, _ := eval(kid(0), item, values)
		b, _ := eval(kid(1), item, values)
		af, _ := toFloat(a)
		bf, _ := toFloat(b)
		return af - bf, nil
	case "if_not_exists":
		name := n.kids[0].val
		if _, ok := item[name]; !ok {
			return eval(kid(1), item, values)
		}
		return eval(kid(0), item, values)
	case "list_append":
		a, _ := eval(kid(0), item, values)
		b, _ := eval(kid(1), item, values)
		return append(asList(a), asList(b)...), nil
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
	case []any:
		var l []any
		for _, x := range t {
			l = append(l, toAttr(x))
		}
		return map[string]any{"L": l}
	default:
		return v
	}
}

func asList(v any) []any {
	if m, ok := v.(map[string]any); ok {
		if l, ok := m["L"].([]any); ok {
			return append([]any{}, l...)
		}
		if ss, ok := m["SS"].([]any); ok {
			return append([]any{}, ss...)
		}
	}
	if l, ok := v.([]any); ok {
		return append([]any{}, l...)
	}
	if v == nil {
		return nil
	}
	return []any{v}
}

func attrType(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	for _, k := range []string{"S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"} {
		if _, ok := m[k]; ok {
			return k
		}
	}
	return ""
}

func addAttr(cur, delta any) any {
	if dm, ok := delta.(map[string]any); ok {
		if ss, ok := dm["SS"].([]any); ok {
			exist := map[string]bool{}
			out := []any{}
			if cm, ok := cur.(map[string]any); ok {
				for _, x := range asList(cm) {
					s := fmt.Sprint(unwrap(x))
					if !exist[s] {
						exist[s] = true
						out = append(out, x)
					}
				}
			}
			for _, x := range ss {
				s := fmt.Sprint(unwrap(x))
				if !exist[s] {
					exist[s] = true
					out = append(out, x)
				}
			}
			return map[string]any{"SS": out}
		}
	}
	cf, _ := toFloat(unwrap(cur))
	df, _ := toFloat(unwrap(delta))
	return map[string]any{"N": strconv.FormatFloat(cf+df, 'f', -1, 64)}
}

func deleteFromSet(cur, delta any) any {
	remove := map[string]bool{}
	for _, x := range asList(delta) {
		remove[fmt.Sprint(unwrap(x))] = true
	}
	var out []any
	for _, x := range asList(cur) {
		if !remove[fmt.Sprint(unwrap(x))] {
			out = append(out, x)
		}
	}
	return map[string]any{"SS": out}
}

func walkPath(item map[string]any, path string) (any, bool) {
	cur := any(item)
	for _, seg := range strings.Split(path, ".") {
		name, idx := seg, -1
		if i := strings.IndexByte(seg, '['); i >= 0 {
			name = seg[:i]
			fmt.Sscanf(seg[i:], "[%d]", &idx)
		}
		m, isMap := cur.(map[string]any)
		if !isMap {
			return nil, false
		}
		v, ok := m[name]
		if !ok {
			if inner, iok := m["M"].(map[string]any); iok {
				v, ok = inner[name]
			}
		}
		if !ok {
			return nil, false
		}
		if idx >= 0 {
			l := asList(v)
			if idx < 0 || idx >= len(l) {
				return nil, false
			}
			v = l[idx]
		}
		cur = v
	}
	return cur, true
}

func setPath(out map[string]any, path string, v any) {
	out[path] = v
	if !strings.ContainsAny(path, ".[") {
		return
	}
	out[strings.Split(path, ".")[0]] = v
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
