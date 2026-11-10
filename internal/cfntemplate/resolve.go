package cfntemplate

import (
	"cmp"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// mark is what resolve answers that is no value a template holds: its own type, so no string a template
// gives, whatever characters it has, is one (holds). resolve has three outcomes, a value, unselected (a
// Fn::Select of no element of a list it knows) or unknown: what a Ref to or a Fn::GetAtt of a resource not
// made yet answers, and a string built on one. noValue is what a Ref to AWS::NoValue answers, which resolve
// drops from a map or a list; inValue a string built on it, which sound refuses, as it does unselected.
type mark string

const unknown, unselected, noValue, inValue mark = "unknown", "Fn::Select", "AWS::NoValue", "AWS::NoValue in a value"

// holds is whether v is the mark m or holds it.
func holds(v any, m mark) bool {
	switch t := v.(type) {
	case map[string]any:
		return slices.ContainsFunc(slices.Collect(maps.Values(t)), func(x any) bool { return holds(x, m) })
	case []any:
		return slices.ContainsFunc(t, func(x any) bool { return holds(x, m) })
	}
	return v == m
}

// combine is the mark a value built on vs is, and whether it is one: an error (AWS::NoValue in it, inValue,
// or unselected) before unknown, and unknown before a value. Every intrinsic that builds a value from
// others answers this when it is one.
func combine(vs ...any) (any, bool) {
	for _, m := range []mark{inValue, noValue, unselected, unknown} {
		if holds(vs, m) {
			return map[bool]mark{true: inValue, false: m}[m == noValue], true
		}
	}
	return nil, false
}

// refused is the error for a value holding an error mark: a Fn::Select of no element (the wording is a
// reading), or AWS::NoValue as part of a value. sound refuses them before anything is made, encode as it is.
func refused(v any) error {
	if holds(v, inValue) || holds(v, noValue) {
		return invalid("Template error: AWS::NoValue removes a property or a list element; it cannot be part of a value")
	} else if holds(v, unselected) {
		return invalid("Template error: Fn::Select cannot select nonexistent value")
	}
	return nil
}

// Encode is the error for a resolved value going to a provider, into the stack record or into an answer
// that holds any mark: an error mark (refused), or an id not yet known (the wording is a reading).
func Encode(v any) error {
	if err := refused(v); err != nil || !holds(v, unknown) {
		return err
	}
	return invalid("Template error: a value is not known when it is used")
}

func (p *Reader) Resolve(v any, refs map[string]string, stack string) any {
	switch t := v.(type) {
	case map[string]any:
		if r, ok := t["Ref"]; ok {
			v, _ := p.ref(Str(r), refs, stack)
			if Str(r) == "AWS::NoValue" {
				return noValue
			} else if p.unmade(Str(r), refs) {
				return unknown
			} else if Str(r) == "AWS::NotificationARNs" {
				arns := spi.Decode[[]any]([]byte(v))
				return arns
			} else if typ := Str(asMap(asMap(p.tpl["Parameters"])[Str(r)])["Type"]); typ == "CommaDelimitedList" || strings.HasPrefix(typ, "List<") {
				items := []any{} // "a Ref would result in ["test","dev","prod"]": "each member string is space trimmed"
				for _, item := range strings.Split(v, ",") {
					items = append(items, strings.TrimSpace(item))
				}
				return items
			}
			return v
		}
		if g, ok := t["Fn::GetAtt"]; ok {
			return getAtt(g, refs)
		}
		if j, ok := t["Fn::Join"]; ok {
			return p.fnJoin(j, refs, stack)
		}
		if s, ok := t["Fn::Sub"]; ok {
			return p.fnSub(s, refs, stack)
		}
		if args, ok := t["Fn::Select"]; ok { // [index, listOfObjects]: "a single object from a list of objects by index"
			args := append(spi.Slice(args), nil, nil)
			index, list := p.Resolve(args[0], refs, stack), p.Resolve(args[1], refs, stack)
			if m, isMark := combine(index, list); isMark && (m != unknown || index == unknown || list == unknown) {
				return m // an error in either, or a list or index not known yet: checked as it is made
			} else if i, isInt := Coerce(index, "Integer"); isInt && i.(int) >= 0 && i.(int) < len(spi.Slice(list)) {
				return spi.Slice(list)[i.(int)]
			}
			return unselected // "doesn't check ... if the index is out of bounds": the stack's error (selected)
		}
		if args, ok := t["Fn::Split"]; ok { // [delimiter, source string]: "A list of string values"
			args, parts := append(spi.Slice(args), nil, nil), []any{}
			if m, isMark := combine(p.Resolve(args[1], refs, stack)); isMark {
				return m
			}
			for _, part := range strings.Split(Str(p.Resolve(args[1], refs, stack)), Str(args[0])) {
				parts = append(parts, part)
			}
			return parts
		}
		if region, ok := t["Fn::GetAZs"]; ok { // "" is the stack's Region; three zones, a reading
			v := p.Resolve(region, refs, stack)
			if m, isMark := combine(v); isMark {
				return m
			}
			r := cmp.Or(Str(v), p.Region)
			return []any{r + "a", r + "b", r + "c"}
		}
		if f, ok := t["Fn::FindInMap"]; ok {
			if v, _, found := findInMap(f, refs, asMap(p.tpl["Mappings"])); found {
				return v
			}
		}
		branches := append(spi.Slice(t["Fn::If"]), nil, nil, nil) // the branch its condition takes (conditions)
		if yes, settled := refs["Condition::"+Str(branches[0])]; settled && t["Fn::If"] != nil {
			return p.Resolve(branches[map[bool]int{true: 1, false: 2}[yes == "true"]], refs, stack)
		}
		out := map[string]any{}
		for k, val := range t {
			if r := p.Resolve(val, refs, stack); r != noValue { // AWS::NoValue "Removes the corresponding resource property"
				out[k] = r
			}
		}
		return out
	case []any:
		out := []any{}
		for _, val := range t {
			if r := p.Resolve(val, refs, stack); r != noValue {
				out = append(out, r)
			}
		}
		return out
	default:
		return v
	}
}

// Props are the resolved Properties of tpl's resource logical, a stack's stored template, which its
// Fn::FindInMaps and Refs read (its Mappings and its parameters' types), not the template the operation was given.
func (p *Reader) Props(tpl map[string]any, logical string, refs map[string]string, stack string) map[string]any {
	defer func(given map[string]any) { p.tpl = given }(p.tpl)
	p.tpl = tpl
	return asMap(p.Resolve(asMap(asMap(tpl["Resources"])[logical])["Properties"], refs, stack))
}

// ref is what a Ref to name answers, a pseudo parameter's value, a parameter's or a resource's, and
// whether name is any of them; the name itself when it is none.
func (p *Reader) ref(name string, refs map[string]string, stack string) (string, bool) {
	if v, ok := p.pseudo(stack, refs["AWS::StackId"])[name]; ok {
		return v, true
	}
	if v, ok := refs[name]; ok { // an empty parameter is a value: "" is not its name
		return v, true
	}
	return name, false
}

// getAtt is a Fn::GetAtt's value: the attribute the resource's type has (attribute), or else its Ref's.
func getAtt(g any, refs map[string]string) any {
	logical, attr, _ := strings.Cut(Str(g), ".") // "A.B", or [A, B]
	if t, ok := g.([]any); ok {
		t = append(slices.Clip(t), nil, nil)
		logical, attr = Str(t[0]), Str(t[1])
	}
	if _, made := refs[logical]; !made { // a resource not made yet
		return unknown
	}
	return cmp.Or(refs[logical+"."+attr], refs[logical])
}

// unmade is whether name is a resource of the template read whose id refs does not hold yet: what reads it
// is unknown until it is made (unknown).
func (p *Reader) unmade(name string, refs map[string]string) bool {
	_, resource := asMap(p.tpl["Resources"])[name]
	_, made := refs[name]
	return resource && !made
}

// sound is the error for a template whose values cannot be made, found before anything is reserved or
// made: AWS::NoValue "Removes the corresponding resource property" (User Guide, pseudo parameters), or a list
// element, so it is never a value, and as part of one, a Fn::Join's separator, a Fn::Sub's string or
// variable, an output's value, it is refused.
func (p *Reader) sound(tpl map[string]any, refs map[string]string, stack string) error {
	var values []any
	for l, r := range asMap(tpl["Resources"]) { // all its properties, when AWS::NoValue is the whole attribute (a reading)
		if v := p.Resolve(asMap(r)["Properties"], refs, stack); v != noValue && !p.skip[l] { // what the stack makes (skipped)
			values = append(values, v)
		}
	}
	for _, o := range asMap(tpl["Outputs"]) {
		if !off(o, refs) {
			values = append(values, p.Resolve(asMap(o)["Value"], refs, stack))
		}
	}
	if err := refused(values); err != nil && (stack != "" || !holds(values, unselected) || holds(values, noValue) || holds(values, inValue)) {
		return err // a template read alone takes a Fn::Select of no element (round 12)
	}
	var err error // without a DefaultValue, "Fn::FindInMap raises an error when a key is not found"
	each([]any{tpl["Resources"], tpl["Outputs"], tpl["Conditions"]}, func(x map[string]any) {
		f, mapped := x["Fn::FindInMap"]
		args, literal := append(spi.Slice(f), nil), 0 // literal counts the keys given as literals
		for _, a := range args {
			_, ok := Coerce(a, "String")
			literal += map[bool]int{true: 1}[ok]
		}
		_, key, found := findInMap(f, refs, asMap(tpl["Mappings"]))
		switch _, ok := Coerce(args[0], "String"); {
		case !mapped || err != nil || found:
		case len(args) != 4: // "Fn::FindInMap: [ MapName, TopLevelKey, SecondLevelKey ]" (Template Reference); the wordings are readings
			err = invalid("Template error: every Fn::FindInMap object requires three parameters, the map name, map key and the attribute for return value")
		case ok && asMap(tpl["Mappings"])[Str(args[0])] == nil:
			err = invalid("Template error: Mapping named '" + Str(args[0]) + "' is not present in the 'Mappings' section of template.")
		case stack == "" && literal < 3: // a template read alone checks only what literals name
		case key != "":
			err = invalid("Template error: Unable to get mapping for " + key)
		case stack != "": // a stack is made with every key settled
			err = invalid("Template error: the Fn::FindInMap " + spi.JSONText(f) + " cannot be evaluated")
		}
	})
	return err
}

// findInMap is the value a Fn::FindInMap names, "the value corresponding to keys in a two-level map that's
// declared in the Mappings section", the template's mappings (Template Reference, Fn::FindInMap):
// its three keys literals, or a Ref or another Fn::FindInMap, the functions the page allows there, that refs
// settles. key is the keys, "map::top::second", "" while one is unsettled; found, whether the map holds it.
func findInMap(args any, refs map[string]string, mappings map[string]any) (v any, key string, found bool) {
	var keys []string
	for _, a := range spi.Slice(args) {
		k, ok := Coerce(a, "String")
		if r, isRef := asMap(a)["Ref"]; isRef {
			k, ok = refs[Str(r)]
		} else if f, isMapped := asMap(a)["Fn::FindInMap"]; isMapped {
			k, _, ok = findInMap(f, refs, mappings)
		} else if asMap(a) != nil {
			ok = false
		}
		if keys = append(keys, Str(k)); !ok {
			return nil, "", false
		}
	}
	if len(keys) != 3 { // a DefaultValue needs the AWS::LanguageExtensions transform, which the engine does not run
		return nil, "", false
	}
	v, found = asMap(asMap(mappings[keys[0]])[keys[1]])[keys[2]]
	return v, strings.Join(keys, "::"), found
}

func (p *Reader) fnJoin(j any, refs map[string]string, stack string) any {
	arr, parts := append(spi.Slice(j), nil, nil), []string(nil) // [separator, [values]]
	sep, list := p.Resolve(arr[0], refs, stack), p.Resolve(arr[1], refs, stack)
	if m, isMark := combine(sep, list); isMark { // a list, from which AWS::NoValue removes its element
		return m
	}
	for _, x := range spi.Slice(list) {
		parts = append(parts, Str(x))
	}
	return strings.Join(parts, Str(sep))
}

// fnSub is the Fn::Sub string with each ${...} substituted: ${!Literal} is the literal ${Literal}, a
// variable its map defines is the variable's value, ${Name.Attribute} is Fn::GetAtt's and ${Name} a Ref's
// (Template Reference, Fn::Sub). A name that is none of them is left as written.
func (p *Reader) fnSub(s any, refs map[string]string, stack string) any {
	parts, held := append(spi.Slice(s), s, nil), []any{} // "text", or ["text", {variables}]; held, what each ${} is
	out := subVariable.ReplaceAllStringFunc(Str(parts[0]), func(m string) string {
		name, v := m[2:len(m)-1], any(nil)
		if literal, ok := strings.CutPrefix(name, "!"); ok {
			return "${" + literal + "}"
		} else if x, ok := asMap(parts[1])[name]; ok {
			v = p.Resolve(x, refs, stack)
		} else if logical, attr, ok := strings.Cut(name, "."); ok {
			v = getAtt([]any{logical, attr}, refs)
		} else if _, ok := p.ref(name, refs, stack); ok || p.unmade(name, refs) || name == "AWS::NoValue" { // a list as its text, as Fn::Join's
			v = p.Resolve(map[string]any{"Ref": name}, refs, stack)
		} else {
			return m
		}
		held = append(held, v)
		return Text(v)
	})
	if m, isMark := combine(held...); isMark { // a string built on a mark is one
		return m
	}
	return out
}

// subVariable is a ${...} in a Fn::Sub string, and subName the name one may hold.
var subVariable, subName = regexp.MustCompile(`\$\{([^}]*)\}`), regexp.MustCompile(`^[A-Za-z0-9_.:]+$`)
