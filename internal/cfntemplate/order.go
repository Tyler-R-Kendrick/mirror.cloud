package cfntemplate

import (
	"cmp"
	"maps"
	"slices"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// order is the order a template's resources are made in: each after those its properties name, by Ref,
// Fn::GetAtt or Fn::Sub ("Resource B is created before resource A", Template Reference, DependsOn
// attribute), and those its DependsOn names, else by logical id. One walk of each resource's properties finds the names, and Kahn's
// algorithm orders them, the earliest logical id first of those ready. A cycle orders none, and is
// CloudFormation's error. A Fn::If whose condition refs settles names only what its branch taken does.
func (p *Reader) order(tpl map[string]any, refs map[string]string) ([]string, error) {
	resources, waiting, users := asMap(tpl["Resources"]), map[string]int{}, map[string][]string{}
	var ready, order, undeclared []string
	// A Ref, a Fn::GetAtt, a Fn::Sub variable or a DependsOn names a parameter, a pseudo parameter or a
	// resource the template declares ("can be template parameter names, resource logical IDs, resource
	// attributes"; a Fn::GetAtt's and a DependsOn's, a resource), or it is "Unresolved": refused before
	// anything is made, in any block and either branch of a Fn::If, as the service refuses it.
	unresolved := func(n string) string {
		v, _ := strings.CutPrefix(n, "${")
		if l, attr := strings.CutPrefix(v, "."); l != "" && resources[l] == nil && (attr || asMap(tpl["Parameters"])[l] == nil && !strings.HasPrefix(l, "AWS::")) {
			return v
		}
		return ""
	}
	for logical, r := range resources {
		p.Walks++
		named := map[string]bool{}
		names(asMap(r)["Properties"], named, refs) // the order prunes what a settled Fn::If does not take
		deps := asMap(r)["DependsOn"]              // "a string or a list of strings" (Template Reference, DependsOn attribute), as parseTemplate checks
		if d, ok := deps.(string); ok {
			deps = []any{d}
		}
		for _, d := range spi.Slice(deps) { // "Resource B is created before resource A"; a resource the stack makes, nothing else,
			// unless this one is left out too; a template read alone settles no condition here, as ValidateTemplate evaluates none
			if d := Str(d); resources[d] != nil && (off(r, refs) || !off(resources[d], refs) || refs["AWS::StackName"] == "") {
				named[d] = true
			} else {
				undeclared = append(undeclared, d)
			}
		}
		for n := range named {
			if _, ok := resources[n]; ok {
				waiting[logical], users[n] = waiting[logical]+1, append(users[n], logical)
			}
		}
		if waiting[logical] == 0 {
			ready = append(ready, logical)
		}
	}
	block := "Resources"
	for _, b := range []string{"Resources", "Outputs", "Conditions"} {
		if all := map[string]bool{}; len(undeclared) == 0 {
			names(tpl[b], all, nil) // every branch of a Fn::If
			for n := range all {
				if v := unresolved(n); v != "" {
					block, undeclared = b, append(undeclared, v)
				}
			}
		}
	}
	if i := slices.IndexFunc(undeclared, func(u string) bool { return strings.HasPrefix(u, ".") && asMap(tpl["Parameters"])[u[1:]] != nil }); i >= 0 {
		return nil, invalid("Template error: instance of Fn::GetAtt references undefined resource " + undeclared[i][1:])
	}
	for i, u := range undeclared {
		undeclared[i] = strings.TrimPrefix(u, ".")
	}
	if len(undeclared) > 0 {
		return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(slices.Compact(slices.Sorted(slices.Values(undeclared))), ", ") + "] in the " + block + " block of the template")
	}
	for slices.Sort(ready); len(ready) > 0; ready = ready[1:] {
		order = append(order, ready[0])
		for _, u := range users[ready[0]] {
			if waiting[u]--; waiting[u] == 0 {
				i, _ := slices.BinarySearch(ready[1:], u)
				ready = slices.Insert(ready, i+1, u)
			}
		}
	}
	if len(order) < len(resources) { // the cycle is those that lead back to themselves, not those after it
		cycle := cycles(resources, users, &p.Walks)
		return nil, invalid("Circular dependency between resources: [" + strings.Join(cycle, ", ") + "]")
	}
	return order, nil
}

// cycles are the nodes (resources, or conditions) in a cycle of users' references, sorted: each strongly
// connected set of more than one, and each that names itself, found in one pass (Tarjan's algorithm), each
// visit counted in walks.
func cycles(nodes map[string]any, users map[string][]string, walks *int) []string {
	index, low, on := map[string]int{}, map[string]int{}, map[string]bool{}
	var stack, cycle []string
	var visit func(string)
	visit = func(v string) {
		*walks++
		n := len(index) + 1
		index[v], low[v], on[v], stack = n, n, true, append(stack, v)
		for _, w := range users[v] {
			if index[w] == 0 {
				visit(w)
				low[v] = min(low[v], low[w])
			} else if on[w] {
				low[v] = min(low[v], index[w])
			}
		}
		if low[v] == index[v] { // v roots a strongly connected set: what the stack holds from v up
			i := len(stack) - 1
			for stack[i] != v {
				i--
			}
			for _, w := range stack[i:] {
				on[w] = false
			}
			if len(stack[i:]) > 1 || slices.Contains(users[v], v) {
				cycle = append(cycle, stack[i:]...)
			}
			stack = stack[:i]
		}
	}
	for _, l := range slices.Sorted(maps.Keys(nodes)) {
		if index[l] == 0 {
			visit(l)
		}
	}
	slices.Sort(cycle)
	return cycle
}

// names adds to named every name v refers to: a Ref's, a Fn::GetAtt's resource, and each Name of a
// ${Name} or ${Name.Attribute} in a Fn::Sub string, but for a variable its map defines, whose value is
// walked instead, and a ${!Literal}, which names nothing.
func names(v any, named map[string]bool, refs map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			switch k {
			case "Ref":
				named[Str(x)] = true
			case "Fn::GetAtt":
				logical, _, _ := strings.Cut(Str(x), ".")
				logical = cmp.Or(Str(append(spi.Slice(x), nil)[0]), logical)
				named[logical], named["."+logical] = true, true // ".": a resource's alone
			case "Fn::Sub":
				parts := append(spi.Slice(x), x, nil) // "text", or ["text", {variables}]
				for _, m := range subVariable.FindAllStringSubmatch(Str(parts[0]), -1) {
					if n, _, _ := strings.Cut(m[1], "."); asMap(parts[1])[m[1]] == nil && !strings.HasPrefix(n, "!") {
						named[n] = true
					}
				}
				names(parts[1], named, refs)
			case "Fn::If": // [condition, then, else]
				branches := append(spi.Slice(x), nil, nil, nil)
				if yes, settled := refs["Condition::"+Str(branches[0])]; settled {
					x = branches[map[bool]int{true: 1, false: 2}[yes == "true"]]
				}
				names(x, named, refs)
			default:
				names(x, named, refs)
			}
		}
	case []any:
		for _, x := range t {
			names(x, named, refs)
		}
	}
}

// referred is the names v refers to (names), sorted.
func referred(v any, refs map[string]string) []string {
	named := map[string]bool{}
	names(v, named, refs)
	return slices.Sorted(maps.Keys(named))
}

// off is whether the condition of a resource or an output v leaves it out: one settled false (a stack's are
// all settled).
func off(v any, refs map[string]string) bool {
	c := Str(asMap(v)["Condition"])
	return c != "" && refs["Condition::"+c] == "false" // one not settled, a template read alone's, is checked: both its Fn::If branches
}

// skipped notes in p.skip each resource the stack does not make: "CloudFormation creates the resource only
// when the condition evaluates to true", and "any resources that depend on that resource aren't created
// either" (Template Reference, Condition attribute).
func (p *Reader) skipped(order []string, refs map[string]string) {
	resources := asMap(p.tpl["Resources"])
	p.skip = map[string]bool{}
	for _, l := range order {
		p.skip[l] = off(resources[l], refs) || slices.ContainsFunc(referred(asMap(resources[l])["Properties"], refs), func(n string) bool { return p.skip[n] })
	}
}
