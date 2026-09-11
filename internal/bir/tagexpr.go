package bir

import (
	"fmt"
	"strings"
)

// Tag-expression support implements the Azure tag-condition expression subset used by
// x-ms-if-tags and the FilterBlobs `where` parameter, pinned to Azurite's
// grammar: comparisons (=, <>, >, >=, <, <=) over bare identifiers or
// double-quoted keys against single-quoted string literals, combined with
// and/or/not and parentheses. Azurite's own QueryInterpreter is a full SQL
// WHERE parser; everything the pinned tests exercise is this subset.
//
// ponytail: Azurite accepts arbitrary SQL WHERE constructs beyond this subset
// (BETWEEN, IN, numeric coercion); nothing in the pinned corpus uses them and
// they would land here as an invalid-expression 400. Upgrade path: extend
// TagExprParser.

// TagExprParser is a recursive-descent parser for the tag-condition grammar.
type TagExprParser struct {
	s   string
	pos int
}

// ParseTagExpr parses a tag-condition expression, returning an evaluator over
// (tags, container) or an error when the expression is malformed. A nil error
// with a nil evaluator never happens.
func ParseTagExpr(s string) (func(tags map[string]any, container string) bool, error) {
	p := &TagExprParser{s: s}
	e, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	p.ws()
	if p.pos != len(p.s) {
		return nil, fmt.Errorf("trailing input at %d", p.pos)
	}
	return e, nil
}

func (p *TagExprParser) ws() {
	for p.pos < len(p.s) && (p.s[p.pos] == ' ' || p.s[p.pos] == '\t') {
		p.pos++
	}
}

// keyword matches a case-insensitive AND/OR/NOT at a word boundary.
func (p *TagExprParser) keyword(kw string) bool {
	p.ws()
	rest := p.s[p.pos:]
	if len(rest) < len(kw) || !strings.EqualFold(rest[:len(kw)], kw) {
		return false
	}
	end := p.pos + len(kw)
	if end < len(p.s) && isTagIdentByte(p.s[end]) {
		return false
	}
	p.pos = end
	return true
}

func isTagIdentByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

func (p *TagExprParser) parseOr() (func(map[string]any, string) bool, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.keyword("or") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l, r := left, right
		left = func(t map[string]any, c string) bool { return l(t, c) || r(t, c) }
	}
	return left, nil
}

func (p *TagExprParser) parseAnd() (func(map[string]any, string) bool, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.keyword("and") {
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		l, r := left, right
		left = func(t map[string]any, c string) bool { return l(t, c) && r(t, c) }
	}
	return left, nil
}

func (p *TagExprParser) parseUnary() (func(map[string]any, string) bool, error) {
	if p.keyword("not") {
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return func(t map[string]any, c string) bool { return !inner(t, c) }, nil
	}
	p.ws()
	if p.pos < len(p.s) && p.s[p.pos] == '(' {
		p.pos++
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		p.ws()
		if p.pos >= len(p.s) || p.s[p.pos] != ')' {
			return nil, fmt.Errorf("missing closing parenthesis")
		}
		p.pos++
		return inner, nil
	}
	return p.parseComparison()
}

// tagOps are matched longest-first so <> and <= win over <.
var tagOps = []string{"<>", ">=", "<=", "=", ">", "<"}

func (p *TagExprParser) parseComparison() (func(map[string]any, string) bool, error) {
	p.ws()
	key, isContainer, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	p.ws()
	var op string
	for _, cand := range tagOps {
		if strings.HasPrefix(p.s[p.pos:], cand) {
			op = cand
			p.pos += len(cand)
			break
		}
	}
	if op == "" {
		return nil, fmt.Errorf("missing comparison operator at %d", p.pos)
	}
	p.ws()
	if p.pos >= len(p.s) || p.s[p.pos] != '\'' {
		return nil, fmt.Errorf("tag comparison value must be a quoted string at %d", p.pos)
	}
	p.pos++
	var lit strings.Builder
	for {
		if p.pos >= len(p.s) {
			return nil, fmt.Errorf("unterminated string literal")
		}
		c := p.s[p.pos]
		if c == '\'' {
			if p.pos+1 < len(p.s) && p.s[p.pos+1] == '\'' {
				lit.WriteByte('\'')
				p.pos += 2
				continue
			}
			p.pos++
			break
		}
		lit.WriteByte(c)
		p.pos++
	}
	value := lit.String()
	return func(tags map[string]any, container string) bool {
		have := container
		if !isContainer {
			v, ok := tags[key]
			if !ok || v == nil {
				// SQL NULL semantics: a missing tag matches nothing, not even <>.
				return false
			}
			have = fmt.Sprint(v)
		}
		switch op {
		case "=":
			return have == value
		case "<>":
			return have != value
		case ">":
			return have > value
		case ">=":
			return have >= value
		case "<":
			return have < value
		case "<=":
			return have <= value
		}
		return false
	}, nil
}

// parseOperand reads a bare identifier, a double-quoted key, or @container.
func (p *TagExprParser) parseOperand() (string, bool, error) {
	if p.pos < len(p.s) && p.s[p.pos] == '"' {
		p.pos++
		start := p.pos
		for p.pos < len(p.s) && p.s[p.pos] != '"' {
			p.pos++
		}
		if p.pos >= len(p.s) {
			return "", false, fmt.Errorf("unterminated quoted tag name")
		}
		key := p.s[start:p.pos]
		p.pos++
		return key, false, nil
	}
	if strings.HasPrefix(p.s[p.pos:], "@") {
		rest := p.s[p.pos+1:]
		if !strings.HasPrefix(rest, "container") || len(rest) > len("container") && isTagIdentByte(rest[len("container")]) {
			return "", false, fmt.Errorf("unsupported parameter at %d", p.pos)
		}
		p.pos += len("@container")
		return "", true, nil
	}
	start := p.pos
	for p.pos < len(p.s) && isTagIdentByte(p.s[p.pos]) {
		p.pos++
	}
	if p.pos == start {
		return "", false, fmt.Errorf("expected a tag name at %d", p.pos)
	}
	return p.s[start:p.pos], false, nil
}

// TagExprKeys lists the tag keys an expression references, excluding
// @container. FilterBlobs answers only those tags, like Azurite.
func TagExprKeys(s string) []string {
	seen := map[string]bool{}
	var out []string
	// Direct scan rather than a parse: collect quoted keys and bare words an
	// operator follows. Works even on expressions too long to matter.
	pos := 0
	for pos < len(s) {
		c := s[pos]
		switch {
		case c == '\'':
			pos++
			for pos < len(s) && s[pos] != '\'' {
				pos++
			}
			pos++
		case c == '"':
			pos++
			start := pos
			for pos < len(s) && s[pos] != '"' {
				pos++
			}
			key := s[start:pos]
			pos++
			if !seen[key] {
				seen[key] = true
				out = append(out, key)
			}
		case c == '@':
			// @container is not a tag key.
			pos++
			for pos < len(s) && isTagIdentByte(s[pos]) {
				pos++
			}
		case isTagIdentByte(c) && (pos == 0 || !isTagIdentByte(s[pos-1])):
			start := pos
			for pos < len(s) && isTagIdentByte(s[pos]) {
				pos++
			}
			word := s[start:pos]
			if strings.EqualFold(word, "and") || strings.EqualFold(word, "or") || strings.EqualFold(word, "not") {
				continue
			}
			// A bare word is a tag key only when an operator follows it.
			rest := strings.TrimLeft(s[pos:], " \t")
			for _, op := range tagOps {
				if strings.HasPrefix(rest, op) {
					if !seen[word] {
						seen[word] = true
						out = append(out, word)
					}
					break
				}
			}
		default:
			pos++
		}
	}
	return out
}

