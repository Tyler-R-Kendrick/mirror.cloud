package cloudformation

import (
	"cmp"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// parseYAML reads a YAML template into the shape a JSON template has. The
// short-form intrinsics become their long forms: !Ref x is {"Ref": x},
// !GetAtt A.B is {"Fn::GetAtt": [A, B]}, and any other !Name is {"Fn::Name": ...}.
func parseYAML(src string) (map[string]any, error) {
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(src), &root); err != nil {
		return nil, err
	}
	v, err := yamlValue(&root)
	if m, ok := v.(map[string]any); ok || err != nil {
		return m, err
	}
	return nil, fmt.Errorf("yaml root must be a map")
}

// CloudFormation "doesn't support ... Aliases[,] Hash merges" (User Guide, Template formats), so an alias
// or a merge key is refused, and no node is read twice: a template's size bounds its walk (templateBody),
// and yaml.v3 its depth, 10,000 levels. The sentence names no anchor: one no alias refers to is read as if
// it were not there.
func yamlValue(n *yaml.Node) (any, error) {
	if n.Kind == yaml.AliasNode || n.Tag == "!!merge" {
		return nil, fmt.Errorf("line %d: YAML aliases and merge keys are not supported", n.Line)
	} else if tag := n.ShortTag(); n.Style&yaml.TaggedStyle != 0 && strings.Contains(" !!binary !!omap !!pairs !!set !!timestamp ", " "+tag+" ") {
		return nil, fmt.Errorf("line %d: the YAML %s tag is not supported", n.Line, tag) // "The binary, omap, pairs, set, and timestamp tags"
	}
	tag := ""
	if strings.HasPrefix(n.Tag, "!") && !strings.HasPrefix(n.Tag, "!!") {
		tag, n.Tag = n.Tag[1:], ""
	}
	var v any
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return yamlValue(n.Content[0])
	case yaml.MappingNode:
		m := map[string]any{}
		for i := 0; i+1 < len(n.Content); i += 2 {
			_, err := yamlValue(n.Content[i]) // a key can be a merge key
			item, itemErr := yamlValue(n.Content[i+1])
			if err = cmp.Or(err, itemErr); err != nil {
				return nil, err
			}
			m[n.Content[i].Value] = item
		}
		v = m
	case yaml.SequenceNode:
		out := []any{}
		for _, c := range n.Content {
			item, err := yamlValue(c)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		v = out
	default:
		if quoted := yaml.DoubleQuotedStyle | yaml.SingleQuotedStyle | yaml.LiteralStyle | yaml.FoldedStyle; n.Style&quoted == 0 && n.Tag == "" || n.Style&(quoted|yaml.TaggedStyle) == 0 {
			v = yaml11(n.Value) // a plain scalar, untagged or under an intrinsic's tag
		} else if err := n.Decode(&v); err != nil {
			return nil, err
		}
	}
	if tag == "" {
		return v, nil
	}
	if tag == "Ref" || tag == "Condition" {
		return map[string]any{tag: v}, nil
	}
	if s, ok := v.(string); ok && tag == "GetAtt" {
		if logical, attr, found := strings.Cut(s, "."); found {
			v = []any{logical, attr}
		}
	}
	return map[string]any{"Fn::" + tag: v}, nil
}

// The YAML 1.1 type repository's plain scalars (yaml.org/type/bool.html, int.html, float.html): CloudFormation
// "supports the YAML Version 1.1 specification" (User Guide, Template formats), where yaml.v3 reads 1.2. Its
// .inf and .nan are left as text, which a template's JSON form could not hold.
var (
	yaml11Bool  = regexp.MustCompile(`^(y|Y|yes|Yes|YES|n|N|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$`)
	yaml11Int   = regexp.MustCompile(`^[-+]?(0b[01_]+|0[0-7_]+|0|[1-9][0-9_]*|0x[0-9a-fA-F_]+|[1-9][0-9_]*(:[0-5]?[0-9])+)$`)
	yaml11Float = regexp.MustCompile(`^[-+]?(([0-9][0-9_]*)?\.[0-9.]*([eE][-+][0-9]+)?|[0-9][0-9_]*(:[0-5]?[0-9])+\.[0-9_]*)$`)
)

// yaml11 is a plain scalar as YAML 1.1 reads it: null, a bool, an int (binary, octal, decimal, hexadecimal
// or sexagesimal) or a float, else the string it is.
func yaml11(text string) any {
	plain, sexagesimal := strings.ReplaceAll(text, "_", ""), 0.0
	for _, part := range strings.Split(strings.TrimLeft(plain, "+-"), ":") {
		n, _ := strconv.ParseFloat(part, 64)
		sexagesimal = sexagesimal*60 + n
	}
	if strings.HasPrefix(text, "-") {
		sexagesimal = -sexagesimal
	}
	n, intErr := strconv.ParseInt(plain, 0, 64) // base 0 reads 0b, 0x and a leading 0's octal, as 1.1 does
	f, floatErr := strconv.ParseFloat(plain, 64)
	switch isInt, isFloat := yaml11Int.MatchString(text), yaml11Float.MatchString(text); {
	case text == "" || strings.Contains(" ~ null Null NULL ", " "+text+" "):
		return nil
	case yaml11Bool.MatchString(text):
		return strings.ContainsAny(text[:1], "yYtT") || strings.EqualFold(text, "on")
	case strings.Contains(text, ":") && (isInt || isFloat):
		return map[bool]any{true: int(sexagesimal), false: sexagesimal}[isInt]
	case isInt && intErr == nil:
		return int(n)
	case isFloat && floatErr == nil:
		return f
	}
	return text
}
