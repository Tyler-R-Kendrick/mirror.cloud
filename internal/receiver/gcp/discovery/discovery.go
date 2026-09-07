// Package discovery ingests Google API Discovery documents.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver"
)

// Receiver implements receiver.Receiver for Google Discovery JSON.
type Receiver struct{}

func (Receiver) Name() string { return "discovery" }

func (Receiver) Detect(_ string, head []byte) bool {
	s := string(head)
	return strings.Contains(s, `"discoveryVersion"`) || strings.Contains(s, `"resources"`)
}

type document struct {
	Name string `json:"name"`
	// Parameters apply to every method in the document -- `alt`, `fields`,
	// `key` and the rest -- so they are members of every request.
	Parameters map[string]schema   `json:"parameters"`
	Resources  map[string]resource `json:"resources"`
	Schemas    map[string]schema   `json:"schemas"`
}

type resource struct {
	Methods   map[string]method   `json:"methods"`
	Resources map[string]resource `json:"resources"`
}

type method struct {
	ID         string            `json:"id"`
	HTTPMethod string            `json:"httpMethod"`
	Path       string            `json:"path"`
	FlatPath   string            `json:"flatPath"`
	Parameters map[string]schema `json:"parameters"`
	Request    *reference        `json:"request"`
	Response   *reference        `json:"response"`
}

// reference is Discovery's `{"$ref": "Bucket"}`, which is how a method names
// the schema its body and its answer are.
type reference struct {
	Ref string `json:"$ref"`
}

type schema struct {
	Ref                  string            `json:"$ref"`
	Type                 string            `json:"type"`
	Format               string            `json:"format"`
	Description          string            `json:"description"`
	Required             bool              `json:"required"`
	Location             string            `json:"location"`
	Pattern              string            `json:"pattern"`
	Minimum              string            `json:"minimum"`
	Maximum              string            `json:"maximum"`
	Enum                 []string          `json:"enum"`
	Properties           map[string]schema `json:"properties"`
	Items                *schema           `json:"items"`
	AdditionalProperties *schema           `json:"additionalProperties"`
}

// Ingest parses one Discovery document.
//
// It used to record each method's name, verb and path and stop there, leaving
// every operation with no input and no output shape. The schemas were parsed
// into `Shapes` and nothing referred to them: a member's shape was set to the
// member's own name, so `Bucket.acl` pointed at a shape called `acl` that did
// not exist. So the model looked populated -- thirty-eight shapes for Cloud
// Storage -- while carrying no usable type for any of its eighty-seven
// operations, and a Behavior IR bundle, whose every output member is checked
// against the operation's output shape, could not be written for the only
// non-AWS service the emulator serves.
func (Receiver) Ingest(ctx context.Context, src model.SourceRef, data []byte) ([]model.Service, error) {
	_ = ctx
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	id := "gcp." + strings.ToLower(doc.Name)
	if doc.Name == "storage" {
		id = "gcp.storage"
	}
	sh := &shaper{shapes: map[string]model.Shape{}}
	// The named schemas first, so a $ref from a method or from another schema
	// resolves to a shape that is already there.
	for _, name := range sortedSchemas(doc.Schemas) {
		sh.define(name, doc.Schemas[name])
	}

	svc := model.Service{
		ID:             id,
		Namespace:      doc.Name,
		Protocol:       model.ProtoGCPRESTSON,
		EndpointPrefix: doc.Name,
		Source:         src,
	}
	var walk func(resource)
	walk = func(r resource) {
		for _, key := range sortedMethods(r.Methods) {
			m := r.Methods[key]
			if m.ID == "" {
				continue
			}
			svc.Operations = append(svc.Operations, model.Operation{
				Name:       m.ID,
				HTTP:       model.HTTPBinding{Method: m.HTTPMethod, URI: "/" + strings.TrimPrefix(m.Path, "/"), Code: 200},
				Input:      sh.request(m, doc.Parameters),
				Output:     sh.response(m),
				Confidence: model.ConfDeclared,
				Source:     src,
			})
		}
		for _, child := range sortedResources(r.Resources) {
			walk(r.Resources[child])
		}
	}
	for _, name := range sortedResources(doc.Resources) {
		walk(doc.Resources[name])
	}
	// Operations are emitted in document order within a resource and resources
	// in name order, so ingesting the same document twice produces the same
	// list. A map range would not.
	sort.SliceStable(svc.Operations, func(i, j int) bool {
		return svc.Operations[i].Name < svc.Operations[j].Name
	})
	svc.Shapes = sh.shapes
	if err := resolves(&svc, sh.missing); err != nil {
		return nil, err
	}
	return []model.Service{svc}, nil
}

// resolves refuses a model whose references do not. A `$ref` is copied through
// without being looked up -- it has to be, since a schema may refer to one
// defined after it -- so a document naming a schema it does not define would
// otherwise produce exactly the defect this receiver had: members pointing at
// shapes that are not there, in a model that looks populated.
//
// A test over one document proves it for that document. This proves it for
// whatever is ingested next.
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
	for _, id := range sortedShapes(svc.Shapes) {
		sh := svc.Shapes[id]
		for _, name := range sortedMembers(sh.Members) {
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
	return fmt.Errorf("discovery: %s: %d unresolved reference(s): %s",
		svc.ID, len(missing), strings.Join(missing, "; "))
}

// shaper builds the shape graph, giving a name to each anonymous schema it
// meets. Discovery nests object and array schemas inline without naming them,
// and `model.Member` refers to a shape by ID, so every nesting level needs an
// ID that is stable across runs: the dotted path from the named schema it sits
// in, which is unique by construction and reads as what it is.
type shaper struct {
	shapes map[string]model.Shape
	// missing collects references that were swallowed rather than carried
	// into the model, so the check at the end can still see them. A request
	// body is the one place a `$ref` is dereferenced rather than copied: the
	// members are merged and the reference itself does not survive, so a body
	// naming a schema the document does not define would have produced an
	// empty request shape and no complaint.
	missing []string
}

// define registers a named Discovery schema under its own name.
func (s *shaper) define(name string, sch schema) {
	s.put(name, sch)
}

// request builds the shape for a method's request.
//
// A Discovery request is three things at once: the document's global
// parameters, the method's own path and query parameters, and -- when the
// method has a body -- that body's members. The codec presents all three to a
// service as one flat map, so the model describes them as one flat structure.
// Modelling the body as a single payload member would be the tidier reading of
// the specification and would describe something the runtime does not do,
// which is the disagreement this project keeps finding: a service validated
// against one description and served through another.
//
// A parameter and a body member of the same name is the one case where that
// flattening loses something, and the order here is the codec's: it unmarshals
// the body into the map and then writes every query parameter over it, so the
// parameter is what a service sees. The model says what the runtime does, for
// the same reason as above.
func (s *shaper) request(m method, global map[string]schema) string {
	id := m.ID + ".request"
	members := map[string]model.Member{}
	if m.Request != nil && m.Request.Ref != "" {
		body, ok := s.shapes[m.Request.Ref]
		if !ok {
			s.missing = append(s.missing, id+" (body) -> "+m.Request.Ref)
		}
		for name, member := range body.Members {
			members[name] = member
		}
	}
	for _, src := range []map[string]schema{global, m.Parameters} {
		for _, name := range sortedSchemas(src) {
			p := src[name]
			members[name] = model.Member{
				Shape:    s.resolve(id+"."+name, p),
				Required: p.Required,
				Binding:  model.MemberBinding{Location: location(p.Location), Name: name},
				Doc:      p.Description,
			}
		}
	}
	s.shapes[id] = model.Shape{ID: id, Kind: model.KindStructure, Members: members}
	return id
}

// response is the schema a method answers with, or an empty structure when it
// answers with nothing. An empty structure rather than no shape at all: a
// bundle that names an output member on a method that has none should be told
// that, and an operation with no output shape cannot be told anything.
func (s *shaper) response(m method) string {
	// A named response is carried through whether or not the schema exists,
	// so the check at the end sees it. Falling back to an empty structure
	// would turn a document defect into "this method answers nothing".
	if m.Response != nil && m.Response.Ref != "" {
		return m.Response.Ref
	}
	id := m.ID + ".response"
	s.shapes[id] = model.Shape{ID: id, Kind: model.KindStructure, Members: map[string]model.Member{}}
	return id
}

// resolve answers with the ID of the shape a schema describes, registering one
// under path when the schema is anonymous.
func (s *shaper) resolve(path string, sch schema) string {
	if sch.Ref != "" {
		return sch.Ref
	}
	if sch.Type == "object" || sch.Type == "array" || len(sch.Properties) > 0 || sch.Items != nil {
		s.put(path, sch)
		return path
	}
	// An enumerated scalar is named for where it sits rather than shared, so
	// the values it permits belong to that member and a loader error names
	// something a reader can find in the document.
	if len(sch.Enum) > 0 {
		s.shapes[path] = model.Shape{
			ID:          path,
			Kind:        model.KindEnum,
			EnumValues:  append([]string(nil), sch.Enum...),
			Constraints: constraints(sch),
			Doc:         sch.Description,
		}
		return path
	}
	if c := constraints(sch); c.Pattern != "" || c.MinValue != nil || c.MaxValue != nil {
		k := kind(sch.Type)
		if sch.Format != "" {
			k = formatted(k, sch.Format)
		}
		s.shapes[path] = model.Shape{ID: path, Kind: k, Constraints: c, Doc: sch.Description}
		return path
	}
	return s.primitive(sch)
}

// put registers one shape under an ID, recursing into whatever it contains.
func (s *shaper) put(id string, sch schema) {
	switch {
	case sch.AdditionalProperties != nil:
		s.shapes[id] = model.Shape{
			ID:     id,
			Kind:   model.KindMap,
			Key:    s.primitive(schema{Type: "string"}),
			Member: s.resolve(id+".value", *sch.AdditionalProperties),
			Doc:    sch.Description,
		}
	case sch.Type == "array":
		item := s.primitive(schema{})
		if sch.Items != nil {
			item = s.resolve(id+".item", *sch.Items)
		}
		s.shapes[id] = model.Shape{ID: id, Kind: model.KindList, Member: item, Doc: sch.Description}
	default:
		members := map[string]model.Member{}
		for _, name := range sortedSchemas(sch.Properties) {
			p := sch.Properties[name]
			members[name] = model.Member{
				Shape:   s.resolve(id+"."+name, p),
				Binding: model.MemberBinding{Name: name},
				Doc:     p.Description,
			}
		}
		s.shapes[id] = model.Shape{ID: id, Kind: model.KindStructure, Members: members, Doc: sch.Description}
	}
}

// primitive answers with a shape shared by every scalar of the same type and
// format. Discovery states a scalar inline at every site; naming each one for
// its site would put a thousand identical shapes in the model and say nothing
// the type does not.
//
// Constraints are the exception a shared shape cannot carry, so a scalar that
// states a pattern or a bound gets its own shape at its own site.
func (s *shaper) primitive(sch schema) string {
	k := kind(sch.Type)
	if sch.Format != "" {
		k = formatted(k, sch.Format)
	}
	id := "google.discovery#" + string(k)
	if sch.Format != "" {
		id += "." + sch.Format
	}
	if _, seen := s.shapes[id]; !seen {
		s.shapes[id] = model.Shape{ID: id, Kind: k}
	}
	return id
}

// constraints carries the bounds Discovery states, which the engine enforces
// on every request and which were being dropped.
func constraints(sch schema) model.Constraints {
	var c model.Constraints
	c.Pattern = sch.Pattern
	if v, err := strconv.ParseFloat(sch.Minimum, 64); err == nil && sch.Minimum != "" {
		c.MinValue = &v
	}
	if v, err := strconv.ParseFloat(sch.Maximum, 64); err == nil && sch.Maximum != "" {
		c.MaxValue = &v
	}
	return c
}

// location maps Discovery's parameter placement onto the model's.
func location(l string) string {
	switch l {
	case "path":
		return "label"
	case "query":
		return "query"
	default:
		return ""
	}
}

func formatted(k model.ShapeKind, format string) model.ShapeKind {
	switch format {
	case "int64", "uint64":
		return model.KindLong
	case "double", "float":
		return model.KindDouble
	case "byte":
		return model.KindBlob
	case "date-time", "date":
		return model.KindTimestamp
	}
	return k
}

func kind(t string) model.ShapeKind {
	switch t {
	case "object":
		return model.KindStructure
	case "array":
		return model.KindList
	case "integer":
		return model.KindInteger
	case "number":
		return model.KindDouble
	case "boolean":
		return model.KindBoolean
	case "any":
		return model.KindDocument
	default:
		return model.KindString
	}
}

// The three sorted* helpers exist so ingesting the same document twice yields
// the same model: a map range does not, and `make generate` is checked in CI
// to follow byte-for-byte from the pinned lock.

func sortedSchemas(m map[string]schema) []string { return sortedKeys(m) }

func sortedMethods(m map[string]method) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedShapes(m map[string]model.Shape) []string { return sortedKeys(m) }

func sortedMembers(m map[string]model.Member) []string { return sortedKeys(m) }

func sortedResources(m map[string]resource) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

var _ receiver.Receiver = Receiver{}
