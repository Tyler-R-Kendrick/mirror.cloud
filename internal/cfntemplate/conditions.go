package cfntemplate

import (
	"maps"
	"strconv"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// truth is a condition's value when the parameters settle it, and whether they do: Fn::Equals of two
// literals or Refs to parameters, and Fn::Not, Fn::And, Fn::Or and Condition over such conditions (Template
// Reference, condition functions). Anything else, a pseudo parameter included, settles nothing (a cycle of
// conditions parseTemplate refuses). memo holds each named condition once evaluated, so each is evaluated once.
func truth(c any, conds, mappings map[string]any, params map[string]string, memo map[string][2]bool) (bool, bool) {
	m, text := asMap(c), [2]string{}
	if name, ok := m["Condition"]; ok {
		if r, seen := memo[Str(name)]; seen { // or being evaluated: a condition that names itself
			return r[0], r[1]
		}
		memo[Str(name)] = [2]bool{}
		yes, settled := truth(conds[Str(name)], conds, mappings, params, memo)
		memo[Str(name)] = [2]bool{yes, settled}
		return yes, settled
	} else if args := spi.Slice(m["Fn::Equals"]); len(args) == 2 {
		for i, x := range args {
			v, ok := Coerce(x, "String")
			if r, isRef := asMap(x)["Ref"]; isRef {
				v, ok = params[Str(r)]
			} else if f, isMapped := asMap(x)["Fn::FindInMap"]; isMapped {
				v, _, ok = findInMap(f, params, mappings)
			}
			if text[i] = Str(v); !ok {
				return false, false
			}
		}
		return text[0] == text[1], true
	}
	for op, a := range m {
		args, trues := spi.Slice(a), 0
		for _, x := range args {
			yes, ok := truth(x, conds, mappings, params, memo)
			if trues += map[bool]int{true: 1}[yes]; !ok {
				return false, false
			}
		}
		if rule, ok := map[string][2]bool{"Fn::Not": {trues == 0, len(args) == 1}, "Fn::And": {trues == len(args), len(args) > 0},
			"Fn::Or": {trues > 0, len(args) > 0}}[op]; ok {
			return rule[0], rule[1]
		}
	}
	return false, false
}

// conditions are params, with each template condition they settle as "Condition::<name>", "true" or
// "false": every condition evaluated once (truth).
func conditions(tpl map[string]any, params map[string]string) map[string]string {
	refs, memo := maps.Clone(params), map[string][2]bool{}
	for name := range asMap(tpl["Conditions"]) {
		if yes, settled := truth(map[string]any{"Condition": name}, asMap(tpl["Conditions"]), asMap(tpl["Mappings"]), params, memo); settled {
			refs["Condition::"+name] = strconv.FormatBool(yes)
		}
	}
	return refs
}
