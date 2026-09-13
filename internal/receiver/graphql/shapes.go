package graphql

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// shaper turns GraphQL types into model shapes, defining each named type once
// and following references lazily. A schema is a cyclic graph -- Project has
// services, a Service has its project -- so `active` guards the recursion: a
// type already being defined is referenced by name and not walked again.
type shaper struct {
	types  map[string]*gqlType
	shapes map[string]model.Shape
	active map[string]bool
}

// resolve answers the shape ID a type reference denotes, and whether the
// reference is non-null. NON_NULL is not a type in this model, it is a property
// of the member that carries it, so it is unwrapped here and reported back.
func (s *shaper) resolve(ref typeRef) (string, bool) {
	if ref.Kind == "NON_NULL" {
		if ref.OfType == nil {
			return "", true
		}
		id, _ := s.resolve(*ref.OfType)
		return id, true
	}
	if ref.Kind == "LIST" {
		if ref.OfType == nil {
			return "", false
		}
		member, _ := s.resolve(*ref.OfType)
		id := member + ".list"
		if _, done := s.shapes[id]; !done {
			s.shapes[id] = model.Shape{ID: id, Kind: model.KindList, Member: member}
		}
		return id, false
	}
	if ref.Name == "" {
		return "", false
	}
	s.define(ref.Name)
	return ref.Name, false
}

// define materializes one named type, once.
func (s *shaper) define(name string) {
	if _, done := s.shapes[name]; done {
		return
	}
	if s.active[name] {
		return // a cycle: the reference by name is enough
	}
	t := s.types[name]
	if t == nil {
		// A reference to a type the document does not define. Recording it as
		// an opaque document keeps the shape graph closed -- a dangling
		// reference is the one thing every consumer of this model may assume
		// cannot happen -- while saying honestly that nothing is known of it.
		s.shapes[name] = model.Shape{ID: name, Kind: model.KindDocument}
		return
	}
	s.active[name] = true
	defer delete(s.active, name)
	// Reserve the ID before walking members, so a type that reaches itself
	// finds something rather than recursing.
	s.shapes[name] = model.Shape{ID: name, Kind: model.KindStructure, Doc: t.Description}

	switch t.Kind {
	case "OBJECT", "INTERFACE":
		s.shapes[name] = model.Shape{
			ID: name, Kind: model.KindStructure,
			Members: s.membersFromFields(t.Fields),
			Doc:     t.Description,
		}
	case "INPUT_OBJECT":
		s.shapes[name] = model.Shape{
			ID: name, Kind: model.KindStructure,
			Members: s.membersFromInputs(t.InputFields),
			Doc:     t.Description,
		}
	case "UNION":
		// A union's possible types are its members, named for themselves. The
		// model has no notion of an unnamed alternative, and GraphQL gives the
		// alternatives names, so this loses nothing.
		members := map[string]model.Member{}
		for _, p := range t.PossibleTypes {
			id, _ := s.resolve(p)
			if id == "" {
				continue
			}
			members[id] = model.Member{Shape: id, Binding: model.MemberBinding{Name: id}}
		}
		s.shapes[name] = model.Shape{ID: name, Kind: model.KindUnion, Members: members, Doc: t.Description}
	case "ENUM":
		values := make([]string, 0, len(t.EnumValues))
		for _, v := range t.EnumValues {
			values = append(values, v.Name)
		}
		sort.Strings(values)
		s.shapes[name] = model.Shape{ID: name, Kind: model.KindEnum, EnumValues: values, Doc: t.Description}
	case "SCALAR":
		s.shapes[name] = model.Shape{ID: name, Kind: scalarKind(name), Doc: t.Description}
	default:
		s.shapes[name] = model.Shape{ID: name, Kind: model.KindDocument, Doc: t.Description}
	}
}

// membersFromFields maps an object's fields. A field's own arguments are not
// members of anything here: they belong to a selection, and a selection is a
// property of a request this model does not carry (C53).
func (s *shaper) membersFromFields(fields []gqlField) map[string]model.Member {
	out := map[string]model.Member{}
	for _, f := range fields {
		id, required := s.resolve(f.Type)
		if id == "" {
			continue
		}
		out[f.Name] = model.Member{
			Shape:    id,
			Required: required,
			Binding:  model.MemberBinding{Name: f.Name},
			Doc:      f.Description,
		}
	}
	return out
}

func (s *shaper) membersFromInputs(inputs []gqlInput) map[string]model.Member {
	out := map[string]model.Member{}
	for _, f := range inputs {
		id, required := s.resolve(f.Type)
		if id == "" {
			continue
		}
		m := model.Member{
			Shape:    id,
			Required: required,
			Binding:  model.MemberBinding{Name: f.Name},
			Doc:      f.Description,
		}
		// A default makes a non-null argument optional in practice: the server
		// supplies the value when the caller does not, so requiring it of the
		// caller would refuse a request the schema accepts.
		if f.DefaultValue != nil {
			m.Default = *f.DefaultValue
			m.Required = false
		}
		out[f.Name] = m
	}
	return out
}

// arguments synthesizes the input structure for one root field. The name is the
// field's, suffixed, because GraphQL gives an operation's arguments no type of
// their own -- they are a list on the field -- and every consumer of this model
// expects an operation's input to be one named shape.
func (s *shaper) arguments(f gqlField) string {
	id := f.Name + ".input"
	if _, done := s.shapes[id]; done {
		return id
	}
	s.shapes[id] = model.Shape{
		ID:      id,
		Kind:    model.KindStructure,
		Members: s.membersFromInputs(f.Args),
		Doc:     fmt.Sprintf("Arguments of the %s root field.", f.Name),
	}
	return id
}

// scalarKind maps GraphQL's five built-in scalars and refuses to guess at the
// rest. A custom scalar is serialized however its server likes -- DateTime is a
// string, JSON is an object, BigInt is either -- and the specification says
// nothing about which. KindDocument is the model's word for "any JSON", so an
// unknown scalar is recorded as one rather than asserted to be a string: the
// permissive answer is wrong about nothing, and a wrong guess here would reject
// a value the real service accepts.
func scalarKind(name string) model.ShapeKind {
	switch name {
	case "String", "ID":
		return model.KindString
	case "Int":
		return model.KindInteger
	case "Float":
		return model.KindFloat
	case "Boolean":
		return model.KindBoolean
	default:
		return model.KindDocument
	}
}

// serviceID derives the stable id from the document's path under specs/, the
// same rule the other receivers use: `<provider>/<service>.json` is
// `provider.service`.
func serviceID(p string) string {
	clean := strings.TrimSuffix(path.Base(p), path.Ext(p))
	dir := path.Base(path.Dir(p))
	if dir == "" || dir == "." || dir == "/" {
		return clean
	}
	return dir + "." + clean
}

func provider(id string) string {
	if i := strings.Index(id, "."); i >= 0 {
		return id[:i]
	}
	return id
}
