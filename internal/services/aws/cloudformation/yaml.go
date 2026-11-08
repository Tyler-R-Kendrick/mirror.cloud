package cloudformation

import (
	"cmp"
	"fmt"
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
		if err := n.Decode(&v); err != nil {
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
