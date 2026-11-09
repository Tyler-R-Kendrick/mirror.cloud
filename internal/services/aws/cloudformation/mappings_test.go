package cloudformation

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// The Mappings section is read from the parsed template once per operation
// and kept out of the stack's stored Refs. Before, it was the JSON text of
// a Ref named "Fn::FindInMap", decoded again at every Fn::FindInMap, so a
// template of about a megabyte of mappings and thousands of Fn::FindInMaps
// took minutes, and every stack stored its mappings among its Refs. Each
// operation here is given 30 s, a hang timer, not a measure.
func TestCloudFormationReadsMappingsOnce(t *testing.T) {
	h := newStacks(t)
	mapping := map[string]any{}
	for i := range 30_000 {
		mapping[fmt.Sprintf("k%d", i)] = map[string]any{"a": "v"}
	}
	var finds []any
	for i := range 10_000 {
		finds = append(finds, map[string]any{"Fn::FindInMap": []any{"M", fmt.Sprintf("k%d", i), "a"}})
	}
	body, _ := json.Marshal(map[string]any{"Mappings": map[string]any{"M": mapping},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
		"Outputs":   map[string]any{"O": map[string]any{"Value": map[string]any{"Fn::Join": []any{"", finds}}}}})
	if len(body) > 1_000_000 {
		t.Fatalf("the template is %d bytes, more than a TemplateURL's 1,000,000", len(body))
	}
	url := h.upload("mappings.json", string(body))
	for _, op := range []string{"ValidateTemplate", "GetTemplateSummary", "CreateStack"} {
		done := make(chan error, 1)
		go func() { _, err := h.invoke(op, map[string]any{"StackName": "maps", "TemplateURL": url}); done <- err }()
		if err := within(t, done, op+" of "+fmt.Sprint(len(body))+" bytes of mappings"); err != nil {
			t.Fatalf("%s answered %v", op, err)
		}
	}
	if got := h.outputs("maps")["O"]; got != strings.Repeat("v", 10_000) {
		t.Errorf("the output is %d bytes, want 10000 v", len(str(got)))
	}
	if refs := asMap(h.record("maps")["Refs"]); refs["Fn::FindInMap"] != nil {
		t.Errorf("the stack stored its Mappings among its Refs: %d bytes", len(str(refs["Fn::FindInMap"])))
	}
}

// A Mappings section is a map of mappings, each a map of top-level keys,
// each a map of attributes whose values "can be of type String or List"
// (User Guide, Mappings syntax). Each other shape is refused, by
// ValidateTemplate and CreateStack alike, and nothing is made. Before, any
// shape was taken, and a Fn::FindInMap into it found nothing or a map.
func TestCloudFormationValidatesTheMappingsSection(t *testing.T) {
	h := newStacks(t)
	for i, c := range []struct {
		mappings any
		want     string
	}{
		{[]any{"M"}, "Every Mappings member must be a map"},
		{map[string]any{"M": "text"}, "Every Mappings member M must be a map"},
		{map[string]any{"M": map[string]any{"us-east-1": []any{"a"}}}, "Every Mappings member us-east-1 must be a map"},
		{map[string]any{"M": map[string]any{}}, "Mappings member M must contain at least one key"},
		{map[string]any{"M": map[string]any{"k": map[string]any{"a": map[string]any{"b": "c"}}}}, "Every Mappings attribute must be a String or a List."},
		{map[string]any{"M": map[string]any{"k": map[string]any{"a": nil}}}, "Every Mappings attribute must be a String or a List."},
	} {
		body, _ := json.Marshal(map[string]any{"Mappings": c.mappings, "Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
		for _, op := range []string{"ValidateTemplate", "CreateStack"} {
			name := fmt.Sprintf("mapped-%d", i)
			if _, err := h.invoke(op, map[string]any{"StackName": name, "TemplateBody": string(body)}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Template format error: "+c.want) {
				t.Errorf("%s of Mappings %v answered %v, want ValidationError: %s", op, c.mappings, err, c.want)
			}
			if record, left := h.stored(name); left {
				t.Errorf("the refused Mappings %v left %v", c.mappings, record)
			}
		}
	}
	body, _ := json.Marshal(map[string]any{"Mappings": map[string]any{"M": map[string]any{"k": map[string]any{"s": "v", "n": 1, "l": []any{"a", "b"}}}},
		"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}})
	if _, err := h.invoke("ValidateTemplate", map[string]any{"TemplateBody": string(body)}); err != nil {
		t.Errorf("ValidateTemplate of String, number and List attributes answered %v", err)
	}
}

// ValidateTemplate and GetTemplateSummary read a template alone: they check a
// Fn::FindInMap's shape, its three parameters and that the map it names
// exists, and that the value two literal keys name exists, but evaluate no
// key a Ref gives, a parameter's Default included, since a stack may give
// the parameter another value. Before, a Default without a value in the map
// refused the template, and a Fn::FindInMap of two parameters was taken.
func TestCloudFormationValidatesAFindInMapByItsShape(t *testing.T) {
	h := newStacks(t)
	template := func(find ...any) string {
		body, _ := json.Marshal(map[string]any{"Parameters": map[string]any{"Env": map[string]any{"Type": "String", "Default": "absent"}},
			"Mappings":  map[string]any{"M": map[string]any{"k": map[string]any{"a": "v"}}},
			"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}},
			"Outputs":   map[string]any{"O": map[string]any{"Value": map[string]any{"Fn::FindInMap": find}}}})
		return string(body)
	}
	env := map[string]any{"Ref": "Env"}
	for _, c := range []struct {
		find []any
		want string // "" when a template read alone is taken
	}{
		{[]any{"M", env, "a"}, ""},
		{[]any{"M", "k", env}, ""},
		{[]any{"M", "k", "a"}, ""},
		{[]any{"M", "k", "b"}, "Unable to get mapping for M::k::b"},
		{[]any{"Nope", env, "a"}, "Mapping named 'Nope' is not present in the 'Mappings' section of template."},
		{[]any{"M", "k"}, "every Fn::FindInMap object requires three parameters"},
		{[]any{"M", "k", "a", "d"}, "every Fn::FindInMap object requires three parameters"},
	} {
		for _, op := range []string{"ValidateTemplate", "GetTemplateSummary"} {
			_, err := h.invoke(op, map[string]any{"TemplateBody": template(c.find...)})
			if c.want == "" && err != nil || c.want != "" && (code(err) != "ValidationError" || !strings.Contains(err.Error(), c.want)) {
				t.Errorf("%s of Fn::FindInMap %v answered %v, want %q", op, c.find, err, c.want)
			}
		}
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "absent", "TemplateBody": template("M", env, "a")}); code(err) != "ValidationError" || !strings.Contains(err.Error(), "Unable to get mapping for M::absent::a") {
		t.Errorf("CreateStack with the Default answered %v, want ValidationError: Unable to get mapping for M::absent::a", err)
	}
}
