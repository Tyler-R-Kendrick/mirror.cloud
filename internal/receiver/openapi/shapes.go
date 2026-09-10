package openapi

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// shaper builds the shape graph, naming every anonymous schema it meets.
//
// OpenAPI nests object and array schemas inline without naming them, and
// `model.Member` refers to a shape by ID, so each nesting level needs an ID
// that is stable across runs: the dotted path from the named schema or the
// operation it sits in, which is unique by construction and reads as what it is.
type shaper struct {
	shapes map[string]model.Shape
	// missing collects references that were swallowed rather than carried into
	// the model, so the check at the end can still see them. A composed schema
	// and a request body are the two places a `$ref` is dereferenced rather
	// than copied: the members are merged and the reference does not survive,
	// so one naming a schema the document does not define would otherwise
	// produce an empty shape and no complaint.
	missing []string
	// composites are allOf structures whose branches are references. They
	// cannot be merged as they are met, because a branch may name a component
	// defined later, so they are recorded and merged once every component
	// exists.
	composites []composite
}

type composite struct {
	id   string
	refs []string
}

// define registers a named component schema under its own name.
func (s *shaper) define(name string, sch schema) {
	s.put(name, sch, name)
}

// request synthesizes the input shape: every parameter under the placement the
// document gives it, plus the members of a JSON request body.
//
// The body's members are merged in rather than referenced, because the codec
// flattens a JSON body into the same map the parameters land in -- a client
// sends `{"name":"x"}` and `?teamId=1` and the operation sees both at the top
// level. A body that is not an object (a raw string, an array) is carried as a
// single `body` member instead, since it has no members to merge.
func (s *shaper) request(op string, params []parameter, o operation) string {
	id := op + "Request"
	members := map[string]model.Member{}
	for _, p := range params {
		if p.Name == "" {
			continue
		}
		// A placement the model cannot express -- a cookie -- is dropped rather
		// than carried with an empty one. A member no codec will ever fill is
		// worse than an absent member: it is a required-member check that can
		// only ever fail, on something no client is able to send.
		at := location(p.In)
		if at == "" {
			continue
		}
		member := model.Member{
			Shape:    s.resolve(id+"."+p.Name, deref(p.Schema)),
			Required: p.Required || p.In == "path",
			Binding:  model.MemberBinding{Location: at, Name: p.Name},
			Doc:      p.Description,
		}
		// A path label is named by the URI pattern, and the pattern uses the
		// document's own spelling, so the member has to carry that spelling as
		// its name rather than only as its wire name.
		members[p.Name] = member
	}
	if o.RequestBody != nil {
		if body, ok := jsonSchema(o.RequestBody.Content); ok {
			s.mergeBody(id, body, members, o.RequestBody.Required)
		}
	}
	s.shapes[id] = model.Shape{ID: id, Kind: model.KindStructure, Members: members}
	return id
}

// mergeBody folds a request body into the request shape's members.
func (s *shaper) mergeBody(id string, body schema, into map[string]model.Member, required bool) {
	resolved, ok := s.object(body)
	if !ok {
		// Not an object: a raw payload. It gets one member, marked as the
		// payload so the codec knows the whole body is this member.
		into["body"] = model.Member{
			Shape:    s.resolve(id+".body", body),
			Required: required,
			Binding:  model.MemberBinding{Location: "payload", Name: "body"},
		}
		return
	}
	for _, name := range sortedKeys(resolved.Members) {
		if _, taken := into[name]; taken {
			// A parameter and a body member of the same name: the parameter
			// wins, because it is the one with a placement on the wire.
			continue
		}
		into[name] = resolved.Members[name]
	}
}

// object answers the structure a schema denotes, following one `$ref` and one
// level of composition, or reports that the schema is not an object at all.
func (s *shaper) object(sch schema) (model.Shape, bool) {
	if sch.Ref != "" {
		name := strings.TrimPrefix(sch.Ref, componentSchemas)
		shape, ok := s.shapes[name]
		if !ok {
			s.missing = append(s.missing, "body -> "+sch.Ref)
			return model.Shape{}, false
		}
		return shape, shape.Kind == model.KindStructure
	}
	if len(sch.Properties) == 0 && len(sch.AllOf) == 0 {
		return model.Shape{}, false
	}
	id := s.resolve("body", sch)
	shape := s.shapes[id]
	return shape, shape.Kind == model.KindStructure
}

// response is the shape of the operation's success body. An operation that
// answers with no body gets an empty structure rather than nothing, so a bundle
// projecting an empty answer still has a shape to be checked against -- the
// same choice the Discovery receiver makes.
func (s *shaper) response(op string, o operation) string {
	id := op + "Response"
	code := successCode(o)
	for _, key := range []string{fmt.Sprint(code), "default"} {
		r, ok := o.Responses[key]
		if !ok {
			continue
		}
		body, ok := jsonSchema(r.Content)
		if !ok {
			break
		}
		if body.Ref != "" {
			// Carried, not dereferenced: the reference is the answer, and
			// `resolves` checks it like any other.
			name := strings.TrimPrefix(body.Ref, componentSchemas)
			if _, defined := s.shapes[name]; !defined {
				s.missing = append(s.missing, "operation "+op+" response -> "+body.Ref)
			}
			return name
		}
		return s.resolve(id, body)
	}
	s.shapes[id] = model.Shape{ID: id, Kind: model.KindStructure, Members: map[string]model.Member{}}
	return id
}

// jsonSchema picks the JSON media type out of a content map. A document that
// offers several representations of one body describes the same members in each,
// and JSON is the one the codec speaks.
func jsonSchema(content map[string]mediaType) (schema, bool) {
	for _, ct := range sortedKeys(content) {
		if !strings.Contains(ct, "json") {
			continue
		}
		if m := content[ct]; m.Schema != nil {
			return *m.Schema, true
		}
	}
	// No JSON representation, but a body all the same -- a binary upload, a
	// form. The first one in name order keeps this deterministic.
	for _, ct := range sortedKeys(content) {
		if m := content[ct]; m.Schema != nil {
			return *m.Schema, true
		}
	}
	return schema{}, false
}

// resolve answers the shape ID for a schema, defining it under path when the
// schema is anonymous. A `$ref` is carried through without being looked up --
// it has to be, since a schema may refer to one defined after it -- and
// `resolves` is what refuses a reference that never arrives.
func (s *shaper) resolve(path string, sch schema) string {
	if sch.Ref != "" {
		return strings.TrimPrefix(sch.Ref, componentSchemas)
	}
	s.put(path, sch, path)
	return path
}

// put defines one shape under id. name is the dotted path used to name the
// shapes nested inside it.
func (s *shaper) put(id string, sch schema, name string) {
	switch {
	case len(sch.AllOf) > 0:
		s.putComposed(id, sch, name)
	case len(sch.OneOf) > 0 || len(sch.AnyOf) > 0:
		s.putUnion(id, sch, name)
	case len(sch.Properties) > 0 || sch.typeName() == "object":
		s.putObject(id, sch, name)
	case sch.typeName() == "array":
		item := schema{}
		if sch.Items != nil {
			item = *sch.Items
		}
		s.shapes[id] = model.Shape{
			ID: id, Kind: model.KindList,
			Member:      s.resolve(name+".member", item),
			Constraints: constraints(sch),
			Doc:         sch.Description,
		}
	default:
		s.shapes[id] = model.Shape{
			ID: id, Kind: primitive(sch), EnumValues: enumValues(sch),
			Constraints: constraints(sch), Doc: sch.Description,
		}
	}
}

// putObject defines a structure, or a map when the document describes free-form
// keys with `additionalProperties`. A schema with both is a structure whose
// declared properties are the ones a bundle can name.
func (s *shaper) putObject(id string, sch schema, name string) {
	if len(sch.Properties) == 0 {
		if value, ok := additional(sch); ok {
			s.shapes[id] = model.Shape{
				ID: id, Kind: model.KindMap,
				Key:    s.stringShape(),
				Member: s.resolve(name+".value", value),
				Doc:    sch.Description,
			}
			return
		}
	}
	required := map[string]bool{}
	for _, r := range sch.Required {
		required[r] = true
	}
	members := map[string]model.Member{}
	for _, prop := range sortedKeys(sch.Properties) {
		p := sch.Properties[prop]
		members[prop] = model.Member{
			Shape:    s.resolve(name+"."+prop, p),
			Required: required[prop],
			Doc:      p.Description,
		}
	}
	s.shapes[id] = model.Shape{ID: id, Kind: model.KindStructure, Members: members, Doc: sch.Description}
}

// putComposed defines an allOf as one structure carrying every branch's
// members. Branches that are references are recorded and merged once every
// component exists, because a branch may name a component defined later.
func (s *shaper) putComposed(id string, sch schema, name string) {
	required := map[string]bool{}
	for _, r := range sch.Required {
		required[r] = true
	}
	members := map[string]model.Member{}
	var refs []string
	for i, branch := range sch.AllOf {
		if branch.Ref != "" {
			refs = append(refs, strings.TrimPrefix(branch.Ref, componentSchemas))
			continue
		}
		for _, r := range branch.Required {
			required[r] = true
		}
		for _, prop := range sortedKeys(branch.Properties) {
			p := branch.Properties[prop]
			members[prop] = model.Member{
				Shape:    s.resolve(fmt.Sprintf("%s.allOf%d.%s", name, i, prop), p),
				Required: required[prop],
				Doc:      p.Description,
			}
		}
	}
	for _, prop := range sortedKeys(sch.Properties) {
		p := sch.Properties[prop]
		members[prop] = model.Member{
			Shape:    s.resolve(name+"."+prop, p),
			Required: required[prop],
			Doc:      p.Description,
		}
	}
	s.shapes[id] = model.Shape{ID: id, Kind: model.KindStructure, Members: members, Doc: sch.Description}
	if len(refs) > 0 {
		s.composites = append(s.composites, composite{id: id, refs: refs})
	}
}

// putUnion defines a oneOf or anyOf as a union of its branches. A branch that
// is a reference keeps the referenced name, which is what makes the union
// readable; an inline branch is named by its position.
func (s *shaper) putUnion(id string, sch schema, name string) {
	branches := append(append([]schema{}, sch.OneOf...), sch.AnyOf...)
	members := map[string]model.Member{}
	for i, branch := range branches {
		member := s.resolve(fmt.Sprintf("%s.oneOf%d", name, i), branch)
		key := member
		if branch.Ref == "" {
			key = fmt.Sprintf("option%d", i)
		}
		members[key] = model.Member{Shape: member}
	}
	s.shapes[id] = model.Shape{ID: id, Kind: model.KindUnion, Members: members, Doc: sch.Description}
}

// mergeComposites folds each recorded allOf branch into the structure that
// composed it, now that every component is defined.
//
// It runs to a fixed point because a composition may name a component that is
// itself a composition, and it is bounded: a document whose compositions form a
// cycle would otherwise spin here rather than produce a model.
func (s *shaper) mergeComposites() {
	const rounds = 16
	for round := 0; round < rounds; round++ {
		changed := false
		for _, c := range s.composites {
			into, ok := s.shapes[c.id]
			if !ok {
				continue
			}
			for _, ref := range c.refs {
				from, ok := s.shapes[ref]
				if !ok {
					if round == 0 {
						s.missing = append(s.missing, c.id+" (allOf) -> "+componentSchemas+ref)
					}
					continue
				}
				for _, name := range sortedKeys(from.Members) {
					if _, taken := into.Members[name]; taken {
						continue
					}
					into.Members[name] = from.Members[name]
					changed = true
				}
			}
			s.shapes[c.id] = into
		}
		if !changed {
			return
		}
	}
}

// stringShape is the shape a map key takes. Every OpenAPI map is keyed by a
// string, so one shared definition is enough and it is defined lazily so a
// document with no maps does not carry it.
func (s *shaper) stringShape() string {
	const id = "openapi.String"
	if _, ok := s.shapes[id]; !ok {
		s.shapes[id] = model.Shape{ID: id, Kind: model.KindString}
	}
	return id
}

// additional reads `additionalProperties`, which is either a boolean or a
// schema. Only a schema describes a map worth modelling; `true` says a document
// permits unknown keys and says nothing about their shape.
func additional(sch schema) (schema, bool) {
	if len(sch.AdditionalProperties) == 0 {
		return schema{}, false
	}
	var flag bool
	if json.Unmarshal(sch.AdditionalProperties, &flag) == nil {
		return schema{}, false
	}
	var value schema
	if json.Unmarshal(sch.AdditionalProperties, &value) != nil {
		return schema{}, false
	}
	return value, true
}

func deref(p *schema) schema {
	if p == nil {
		return schema{}
	}
	return *p
}

// location maps an OpenAPI parameter `in` onto the model's placement vocabulary.
// A cookie parameter has no placement in the model, and carrying it as a header
// would be a wrong answer rather than a missing one.
func location(in string) string {
	switch in {
	case "path":
		return "label"
	case "query":
		return "query"
	case "header":
		return "header"
	default:
		return ""
	}
}

func primitive(sch schema) model.ShapeKind {
	switch sch.typeName() {
	case "integer":
		if sch.Format == "int64" {
			return model.KindLong
		}
		return model.KindInteger
	case "number":
		if sch.Format == "float" {
			return model.KindFloat
		}
		return model.KindDouble
	case "boolean":
		return model.KindBoolean
	case "string":
		switch sch.Format {
		case "date-time", "date":
			return model.KindTimestamp
		case "byte", "binary":
			return model.KindBlob
		}
		if len(sch.Enum) > 0 {
			return model.KindEnum
		}
		return model.KindString
	case "":
		// No type at all: the document permits anything here.
		return model.KindDocument
	}
	return model.KindString
}

func enumValues(sch schema) []string {
	if len(sch.Enum) == 0 {
		return nil
	}
	out := make([]string, 0, len(sch.Enum))
	for _, v := range sch.Enum {
		if v == nil {
			continue
		}
		out = append(out, fmt.Sprint(v))
	}
	return out
}

func constraints(sch schema) model.Constraints {
	return model.Constraints{
		MinLength:   sch.MinLength,
		MaxLength:   sch.MaxLength,
		MinValue:    sch.Minimum,
		MaxValue:    sch.Maximum,
		Pattern:     sch.Pattern,
		UniqueItems: sch.UniqueItems,
	}
}

// resolves refuses a model whose references do not. A `$ref` is carried through
// without being looked up, so a document naming a schema it does not define
// would otherwise produce members pointing at shapes that are not there, in a
// model that looks populated -- which is the exact defect the Discovery
// receiver shipped with and which this check is copied from.
func resolves(svc *model.Service, swallowed []string) error {
	missing := append([]string(nil), swallowed...)
	check := func(from, ref string) {
		if ref == "" {
			return
		}
		if _, ok := svc.Shapes[ref]; !ok {
			missing = append(missing, from+" -> "+ref)
		}
	}
	for _, op := range svc.Operations {
		check("operation "+op.Name+" input", op.Input)
		check("operation "+op.Name+" output", op.Output)
	}
	for _, id := range sortedKeys(svc.Shapes) {
		sh := svc.Shapes[id]
		for _, name := range sortedKeys(sh.Members) {
			check(id+"."+name, sh.Members[name].Shape)
		}
		check(id+" (member)", sh.Member)
		if sh.Kind == model.KindMap {
			check(id+" (key)", sh.Key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	if len(missing) > 12 {
		missing = append(missing[:12], fmt.Sprintf("and %d more", len(missing)-12))
	}
	return fmt.Errorf("openapi: %s: %d unresolved reference(s): %s",
		svc.ID, len(missing), strings.Join(missing, "; "))
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
