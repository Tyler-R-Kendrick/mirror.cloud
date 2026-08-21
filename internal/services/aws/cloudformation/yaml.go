package cloudformation

import (
	"fmt"
	"strconv"
	"strings"
)

// parseYAML is a CFN-shaped YAML subset: maps, lists, scalars, !Ref/!GetAtt/!Sub.
// ponytail: not a full YAML 1.1 parser; anchors/merge keys/complex tags fail loud.
func parseYAML(src string) (map[string]any, error) {
	lines := yamlLines(src)
	v, _, err := parseYAMLBlock(lines, 0, 0)
	if err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("yaml root must be a map")
	}
	return m, nil
}

type yline struct {
	n      int
	indent int
	text   string
}

func yamlLines(src string) []yline {
	var out []yline
	for i, raw := range strings.Split(strings.ReplaceAll(src, "\r\n", "\n"), "\n") {
		s := strings.TrimRight(raw, " \t")
		if strings.TrimSpace(s) == "" || strings.HasPrefix(strings.TrimSpace(s), "#") {
			continue
		}
		ind := 0
		for _, c := range s {
			if c == ' ' {
				ind++
				continue
			}
			if c == '\t' {
				ind += 2
				continue
			}
			break
		}
		out = append(out, yline{n: i + 1, indent: ind, text: strings.TrimSpace(s)})
	}
	return out
}

func parseYAMLBlock(lines []yline, i, indent int) (any, int, error) {
	if i >= len(lines) {
		return map[string]any{}, i, nil
	}
	if strings.HasPrefix(lines[i].text, "- ") || lines[i].text == "-" {
		return parseYAMLList(lines, i, indent)
	}
	return parseYAMLMap(lines, i, indent)
}

func parseYAMLMap(lines []yline, i, indent int) (map[string]any, int, error) {
	m := map[string]any{}
	for i < len(lines) {
		ln := lines[i]
		if ln.indent < indent {
			break
		}
		if ln.indent > indent {
			return nil, i, fmt.Errorf("yaml line %d: unexpected indent", ln.n)
		}
		key, rest, ok := splitKey(ln.text)
		if !ok {
			return nil, i, fmt.Errorf("yaml line %d: expected key:", ln.n)
		}
		if rest != "" {
			m[key] = yamlScalar(rest)
			i++
			continue
		}
		if i+1 >= len(lines) || lines[i+1].indent <= indent {
			m[key] = map[string]any{}
			i++
			continue
		}
		child, ni, err := parseYAMLBlock(lines, i+1, lines[i+1].indent)
		if err != nil {
			return nil, ni, err
		}
		m[key] = child
		i = ni
	}
	return m, i, nil
}

func parseYAMLList(lines []yline, i, indent int) ([]any, int, error) {
	var out []any
	for i < len(lines) {
		ln := lines[i]
		if ln.indent < indent {
			break
		}
		if ln.indent > indent {
			return nil, i, fmt.Errorf("yaml line %d: unexpected indent", ln.n)
		}
		if !strings.HasPrefix(ln.text, "-") {
			break
		}
		rest := strings.TrimSpace(strings.TrimPrefix(ln.text, "-"))
		if rest == "" {
			if i+1 < len(lines) && lines[i+1].indent > indent {
				child, ni, err := parseYAMLBlock(lines, i+1, lines[i+1].indent)
				if err != nil {
					return nil, ni, err
				}
				out = append(out, child)
				i = ni
				continue
			}
			out = append(out, nil)
			i++
			continue
		}
		if key, val, ok := splitKey(rest); ok {
			item := map[string]any{}
			if val != "" {
				item[key] = yamlScalar(val)
				i++
			} else if i+1 < len(lines) && lines[i+1].indent > indent {
				child, ni, err := parseYAMLBlock(lines, i+1, lines[i+1].indent)
				if err != nil {
					return nil, ni, err
				}
				item[key] = child
				i = ni
			} else {
				item[key] = map[string]any{}
				i++
			}
			for i < len(lines) && lines[i].indent > indent && !strings.HasPrefix(lines[i].text, "-") {
				k2, r2, ok2 := splitKey(lines[i].text)
				if !ok2 {
					return nil, i, fmt.Errorf("yaml line %d: expected key", lines[i].n)
				}
				if r2 != "" {
					item[k2] = yamlScalar(r2)
					i++
					continue
				}
				if i+1 < len(lines) && lines[i+1].indent > lines[i].indent {
					child, ni, err := parseYAMLBlock(lines, i+1, lines[i+1].indent)
					if err != nil {
						return nil, ni, err
					}
					item[k2] = child
					i = ni
					continue
				}
				item[k2] = map[string]any{}
				i++
			}
			out = append(out, item)
			continue
		}
		out = append(out, yamlScalar(rest))
		i++
	}
	return out, i, nil
}

func splitKey(s string) (key, rest string, ok bool) {
	if strings.HasPrefix(s, "-") {
		return "", "", false
	}
	i := strings.IndexByte(s, ':')
	if i <= 0 {
		return "", "", false
	}
	key = strings.TrimSpace(s[:i])
	if key == "" {
		return "", "", false
	}
	rest = strings.TrimSpace(s[i+1:])
	if strings.HasPrefix(rest, "#") {
		rest = ""
	}
	return key, rest, true
}

func yamlScalar(s string) any {
	if i := strings.Index(s, " #"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	switch {
	case strings.HasPrefix(s, "!Ref "):
		return map[string]any{"Ref": strings.TrimSpace(s[len("!Ref "):])}
	case strings.HasPrefix(s, "!GetAtt "):
		att := strings.TrimSpace(s[len("!GetAtt "):])
		att = strings.Trim(att, "[] ")
		if j := strings.IndexByte(att, '.'); j >= 0 {
			return map[string]any{"Fn::GetAtt": []any{att[:j], att[j+1:]}}
		}
		if j := strings.Index(att, ", "); j >= 0 {
			return map[string]any{"Fn::GetAtt": []any{strings.TrimSpace(att[:j]), strings.TrimSpace(att[j+2:])}}
		}
		return map[string]any{"Fn::GetAtt": att}
	case strings.HasPrefix(s, "!Sub "):
		return map[string]any{"Fn::Sub": unquote(strings.TrimSpace(s[len("!Sub "):]))}
	case s == "true" || s == "True":
		return true
	case s == "false" || s == "False":
		return false
	case s == "null" || s == "~" || s == "Null":
		return nil
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && strings.Contains(s, ".") {
		return f
	}
	return unquote(s)
}

func unquote(s string) string {
	if len(s) >= 2 {
		if s[0] == '"' && s[len(s)-1] == '"' {
			inner := s[1 : len(s)-1]
			inner = strings.ReplaceAll(inner, `\"`, `"`)
			return inner
		}
		if s[0] == '\'' && s[len(s)-1] == '\'' {
			return s[1 : len(s)-1]
		}
	}
	return s
}
