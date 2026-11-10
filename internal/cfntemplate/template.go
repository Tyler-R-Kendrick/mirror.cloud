package cfntemplate

import (
	"cmp"
	"encoding/json"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Parse reads a template body, JSON or YAML, and checks what can be checked of it alone, before any
// parameter has a value: its Resources (required, 500 at most), each DeletionPolicy and UpdateReplacePolicy,
// the shape of its Mappings, its conditions (declared, no resource named, no cycle), its Fn::Sub and
// Fn::Split forms, and that every Ref, Fn::GetAtt and DependsOn names something the template declares.
// It answers the template, or the service's ValidationError for the first thing wrong.
func Parse(body string) (map[string]any, error) {
	m, err := decode(body)
	if err != nil {
		return nil, err
	} else if len(asMap(m["Resources"])) == 0 { // "The Resources section is required in every CloudFormation template"
		return nil, invalid("Template format error: At least one Resources member must be defined.")
	} else if n := len(asMap(m["Resources"])); n > 500 { // "The maximum number of resources that you can declare ... 500 resources" (quotas)
		return nil, invalid("Template format error: Number of resources, " + strconv.Itoa(n) + ", is greater than maximum allowed, 500")
	}
	// DeletionPolicy's options and UpdateReplacePolicy's (Template Reference): DeletionPolicy's Snapshot is
	// for the types it lists, none of which the engine makes; UpdateReplacePolicy's "reverts to ... Delete".
	for logical, r := range asMap(m["Resources"]) {
		for attr, options := range map[string]string{"DeletionPolicy": "Delete Retain RetainExceptOnCreate", "UpdateReplacePolicy": "Delete Retain Snapshot"} {
			if v, set := asMap(r)[attr]; set && !slices.Contains(strings.Fields(options), Str(v)) {
				return nil, invalid("Template format error: " + logical + "'s " + attr + " must be one of [" + strings.ReplaceAll(options, " ", ", ") + "], not " + spi.JSONText(v))
			}
		}
	}
	if err := mapped(m["Mappings"]); err != nil {
		return nil, err
	}
	conds, users := asMap(m["Conditions"]), map[string][]string{} // conditions "based on ... Other conditions" (User Guide)
	for name, c := range conds {
		each(c, func(x map[string]any) {
			if n, ok := x["Condition"]; ok {
				users[Str(n)] = append(users[Str(n)], name)
			}
		})
	}
	var named, missing []string // "you can't reference resource logical IDs or their attributes in conditions" (User Guide)
	each(conds, func(x map[string]any) {
		target, _, _ := strings.Cut(cmp.Or(Str(x["Ref"]), Str(append(spi.Slice(x["Fn::GetAtt"]), x["Fn::GetAtt"])[0])), ".")
		if _, resource := asMap(m["Resources"])[target]; resource {
			named = append(named, target)
		}
		if c, ok := x["Condition"].(string); ok && conds[c] == nil {
			missing = append(missing, c)
		}
	})
	var refused *spi.Fault // the first of these refusals, each a reading (the Template Reference gives no wording)
	each([]any{m["Resources"], m["Outputs"]}, func(x map[string]any) {
		if c := Str(append(spi.Slice(x["Fn::If"]), nil)[0]); x["Fn::If"] != nil && conds[c] == nil {
			missing = append(missing, c)
		}
		for _, v := range subVariable.FindAllStringSubmatch(Str(append(spi.Slice(x["Fn::Sub"]), x["Fn::Sub"])[0]), -1) {
			if !strings.HasPrefix(v[1], "!") && !subName.MatchString(v[1]) { // the Template Reference says nothing of a space in ${}
				refused = cmp.Or(refused, invalid("Template error: variable names in Fn::Sub syntax must contain only alphanumeric characters, underscores, periods, and colons"))
			}
		}
		if x["Fn::Split"] != nil && Str(append(spi.Slice(x["Fn::Split"]), nil)[0]) == "" { // "For the Fn::Split delimiter, you can't use any functions. You must specify a string value"
			refused = cmp.Or(refused, invalid("Template error: Fn::Split's delimiter must be a string of one or more characters"))
		}
	})
	if refused != nil {
		return nil, refused
	}
	for _, v := range slices.Concat(slices.Collect(maps.Values(asMap(m["Resources"]))), slices.Collect(maps.Values(asMap(m["Outputs"])))) {
		if c, ok := asMap(v)["Condition"].(string); ok && conds[c] == nil { // a resource's or output's Condition attribute
			missing = append(missing, c)
		}
	}
	if len(named) > 0 {
		return nil, invalid("Template format error: Unresolved dependencies [" + strings.Join(slices.Compact(slices.Sorted(slices.Values(named))), ", ") + "]. Cannot reference resources in the Conditions block of the template")
	} else if len(missing) > 0 {
		return nil, invalid("Template error: unresolved condition dependency " + strings.Join(slices.Compact(slices.Sorted(slices.Values(missing))), ", "))
	}
	if cycle := cycles(conds, users, new(int)); len(cycle) > 0 { // none of them can be evaluated
		return nil, invalid("Template format error: Circular dependency between conditions: [" + strings.Join(cycle, ", ") + "]")
	}
	// A Ref and a Fn::Sub variable, which "can be template parameter names, resource logical IDs, resource
	// attributes", name a resource, parameter or pseudo parameter (AWS::...); a Fn::GetAtt and a DependsOn,
	// a resource.
	undeclared := map[string]bool{}
	for _, r := range asMap(m["Resources"]) {
		named := map[string]bool{}
		names(asMap(r)["Properties"], named, nil)
		dep := asMap(r)["DependsOn"] // "a string or a list of strings" (Template Reference, DependsOn attribute)
		if l, ok := dep.([]any); dep != nil && reflect.TypeOf(dep).Kind() != reflect.String && (!ok || slices.ContainsFunc(l, func(d any) bool { _, s := d.(string); return !s })) {
			return nil, invalid("Template format error: DependsOn must be a string or a list of strings") // the wording is a reading
		}
		for _, d := range slices.DeleteFunc(dependsOn(r), func(d string) bool { return asMap(m["Resources"])[d] != nil }) {
			undeclared[d] = true // a DependsOn names a resource, nothing else
		}
		for n := range named {
			if l, resource := strings.CutPrefix(n, "."); resource && asMap(m["Resources"])[l] == nil && asMap(m["Parameters"])[l] != nil {
				return nil, invalid("Template error: instance of Fn::GetAtt references undefined resource " + l)
			} else if asMap(m["Resources"])[l] == nil && (resource || asMap(m["Parameters"])[n] == nil && !strings.HasPrefix(n, "AWS::")) {
				undeclared[l] = true
			}
		}
	}
	if len(undeclared) > 0 {
		return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(slices.Sorted(maps.Keys(undeclared)), ", ") + "] in the Resources block of the template")
	}
	return m, nil
}

// dependsOn is the resources a resource's DependsOn names: it "can take a single string or list of
// strings" (Template Reference, DependsOn attribute).
func dependsOn(r any) []string {
	names := []string{Str(asMap(r)["DependsOn"])}
	for _, d := range spi.Slice(asMap(r)["DependsOn"]) {
		names = append(names, Str(d))
	}
	return slices.DeleteFunc(names, func(n string) bool { return n == "" })
}

// mapped is the error for a Mappings section that is not one: "Within the mapping, each map is a key followed
// by another mapping", and its values "can be of type String or List" (User Guide, Mappings syntax). Each is
// refused in the service's words, "Every Mappings member <name> must be a map" as a user's report quotes it
// and the rest readings, the first in sorted order.
func mapped(section any) error {
	mappings, isMap := section.(map[string]any)
	if section != nil && !isMap {
		return invalid("Template format error: Every Mappings member must be a map")
	}
	for _, name := range slices.Sorted(maps.Keys(mappings)) {
		if mapping, isMap := mappings[name].(map[string]any); !isMap {
			return invalid("Template format error: Every Mappings member " + name + " must be a map")
		} else if len(mapping) == 0 {
			return invalid("Template format error: Mappings member " + name + " must contain at least one key")
		}
		for _, key := range slices.Sorted(maps.Keys(asMap(mappings[name]))) {
			attrs, isMap := asMap(mappings[name])[key].(map[string]any)
			if !isMap {
				return invalid("Template format error: Every Mappings member " + key + " must be a map")
			}
			for _, v := range attrs {
				if _, scalar := Coerce(v, "String"); !scalar && reflect.TypeOf(v) != reflect.TypeOf([]any{}) {
					return invalid("Template format error: Every Mappings attribute must be a String or a List.")
				}
			}
		}
	}
	return nil
}

// decode is a template body as JSON, or YAML, before any check of what it holds.
func decode(body string) (m map[string]any, err error) {
	if body = strings.TrimSpace(body); body == "" {
		err = invalid("empty TemplateBody")
	} else if body[0] == '{' {
		if jsonErr := json.Unmarshal([]byte(body), &m); jsonErr != nil {
			err = invalid(jsonErr.Error())
		}
	} else if m, err = parseYAML(body); err != nil {
		err = invalid("TemplateBody YAML: " + err.Error())
	}
	return m, err
}

// Stored is a stack's stored template as it holds it: the stack was made from it, so it is read, not
// checked again by checks added since.
func Stored(body string) map[string]any {
	m, _ := decode(body)
	return m
}

// each calls fn with every map v holds, v itself included.
func each(v any, fn func(map[string]any)) {
	if m, ok := v.(map[string]any); ok {
		fn(m)
		for _, x := range m {
			each(x, fn)
		}
	}
	for _, x := range spi.Slice(v) {
		each(x, fn)
	}
}

// Parameters are the request's parameter values, the Parameters list the query protocol's decoder builds
// (awsquery's unflatten), and each Default of a parameter it does not give; or the error for a parameter
// the template does not declare, or one given both a value and UsePreviousValue: "If you specify true, do
// not specify a parameter value" (API reference, Parameter). The wordings are readings.
func Parameters(in, tpl map[string]any) (map[string]string, error) {
	out, undeclared := map[string]string{}, []string{}
	for _, p := range spi.Slice(in["Parameters"]) {
		k, v, valued := Str(asMap(p)["ParameterKey"]), asMap(p)["ParameterValue"], asMap(p)["ParameterValue"] != nil
		if Str(v) != "" && IsTrue(asMap(p)["UsePreviousValue"]) { // "non empty", as the service words it
			return nil, invalid("Invalid input for parameter key " + k + ". Cannot specify usePreviousValue as true and non empty value for a parameter")
		} else if asMap(tpl["Parameters"])[k] == nil {
			undeclared = append(undeclared, k)
		} else if valued {
			out[k] = Str(v)
		}
	}
	for k, v := range asMap(tpl["Parameters"]) {
		if _, given := out[k]; !given && asMap(v)["Default"] != nil {
			out[k] = spi.JSONText(asMap(v)["Default"]) // a parameter "becomes a string": Default 16 is "16"
		}
	}
	if len(undeclared) > 0 {
		return nil, invalid("Parameters: [" + strings.Join(undeclared, ", ") + "] do not exist in the template")
	}
	return out, nil
}

// made is the resources of order a stack makes from tpl, and the error for a template no stack can be made
// from: a parameter or condition without a value, or an output naming a resource the stack does not make.
// The outputs tpl answers are left in it.
func made(tpl map[string]any, order []string, refs map[string]string) ([]string, error) {
	missing := func(section, prefix string) []string { // what a section declares that refs holds no value of
		return slices.DeleteFunc(slices.Sorted(maps.Keys(asMap(tpl[section]))), func(k string) bool { _, ok := refs[prefix+k]; return ok })
	}
	if unvalued := missing("Parameters", ""); len(unvalued) > 0 { // for parameters without default values (User Guide)
		return nil, invalid("Parameters: [" + strings.Join(unvalued, ", ") + "] must have values")
	} else if unsettled := missing("Conditions", "Condition::"); len(unsettled) > 0 { // a stack is made only with every condition settled
		return nil, invalid("Template error: the condition " + unsettled[0] + " cannot be evaluated") // a function the engine does not evaluate
	}
	// "CloudFormation creates the resource only when the condition evaluates to true", and "any resources that
	// depend on that resource aren't created either" (Template Reference, Condition attribute): those skip.
	resources, outs := asMap(tpl["Resources"]), asMap(tpl["Outputs"])
	off := func(v any) bool { c := Str(asMap(v)["Condition"]); return c != "" && refs["Condition::"+c] != "true" }
	skip := map[string]bool{}
	for _, l := range order {
		skip[l] = off(resources[l]) || slices.ContainsFunc(append(referred(asMap(resources[l])["Properties"], refs), dependsOn(resources[l])...), func(n string) bool { return skip[n] })
	}
	maps.DeleteFunc(outs, func(_ string, o any) bool { return off(o) }) // "CloudFormation creates the output only when the associated condition evaluates to true"
	for _, o := range outs {
		if gone := slices.DeleteFunc(referred(asMap(o)["Value"], refs), func(n string) bool { return !skip[n] }); len(gone) > 0 {
			return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(gone, ", ") + "] in the Outputs block of the template")
		}
	}
	return slices.DeleteFunc(order, func(l string) bool { return skip[l] }), nil
}

// Conditioned is refs, what a Ref reads, with the pseudo parameters of the stack named stack, of id id, but
// AWS::NoValue, tpl's Mappings (findInMap) and the conditions of tpl that they settle.
func (p *Reader) Conditioned(tpl map[string]any, refs map[string]string, stack, id string) map[string]string {
	known := p.pseudo(stack, id)
	maps.Copy(known, refs) // a stack's record keeps the values its own operation read: its NotificationARNs, not this request's
	known = conditions(tpl, known)
	delete(known, "AWS::NoValue") // ref answers it: no record holds the sentinel
	return known
}

// pseudo are the pseudo parameters (User Guide, pseudo parameters) of the stack named stack, of id id, each
// as a Ref answers it: the stack's two only when it has them.
func (p *Reader) pseudo(stack, id string) map[string]string {
	var arns []any
	if p.NotificationARNs != nil {
		arns = p.NotificationARNs()
	}
	known := map[string]string{"AWS::AccountId": p.Account, "AWS::Region": p.Region, "AWS::StackName": stack,
		"AWS::StackId": id, "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com",
		"AWS::NotificationARNs": spi.JSONText(append([]any{}, arns...))} // a list, as JSON: [] for none
	maps.DeleteFunc(known, func(_, v string) bool { return v == "" })
	return known
}
