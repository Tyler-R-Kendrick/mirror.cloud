// Package xmlenc writes response bodies as the XML a generated model
// describes. It is the one writer for every XML dialect served -- awsQuery and
// ec2Query responses, restXml bodies -- so an element name, a flattened list
// or an attribute is decided by the model once rather than transcribed per
// operation.
package xmlenc

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Encoder writes values as the XML the model describes: member names resolve
// to their xmlName, lists to their element name (flattened or wrapped), maps
// to entries, and a member the shape marks as an attribute goes on its
// parent's open tag. Members bound to a header, a status code or a label are
// not part of a body and are skipped.
type Encoder struct{ Svc *model.Service }

// Value writes the contents of one element: the members of a structure in
// the order the specification declares them (a key the shape does not know
// goes last, sorted), the entries of a map, or the text of a scalar.
func (e Encoder) Value(b *strings.Builder, shapeID string, v any) {
	shape, known := e.shape(shapeID)
	switch t := v.(type) {
	case map[string]any:
		if known && shape.Kind == model.KindMap {
			e.entries(b, shape, t)
			return
		}
		for _, k := range declaredOrder(shape, known, t) {
			if m, ok := shape.Members[k]; known && ok && (m.Binding.XMLAttribute || !inBody(m.Binding)) {
				continue
			}
			wire, child, flat := e.resolve(shape, known, k)
			e.Member(b, wire, child, flat, t[k])
		}
	case []any:
		// A list the shape did not describe -- an undeclared member, or a
		// producer answering something the model does not carry. The wrapper
		// the codec has always written is kept so nothing that worked before
		// this change stops working.
		for _, item := range t {
			b.WriteString("<member>")
			e.Value(b, "", item)
			b.WriteString("</member>")
		}
	case nil:
	default:
		b.WriteString(Escape(fmt.Sprint(t)))
	}
}

// Member writes one named member, wrapper included -- except for a flattened
// list or map, which has no wrapper: each element carries the member's own name.
func (e Encoder) Member(b *strings.Builder, wire, shapeID string, flat bool, v any) {
	shape, known := e.shape(shapeID)
	if known {
		switch shape.Kind {
		case model.KindList:
			if items, ok := v.([]any); ok {
				e.list(b, wire, shape, flat, items)
				return
			}
		case model.KindMap:
			if entries, ok := v.(map[string]any); ok {
				if flat {
					for _, k := range sortedKeys(entries) {
						open(b, wire)
						e.entry(b, shape, k, entries[k])
						closeTag(b, wire)
					}
					return
				}
			}
		}
	}
	b.WriteString("<" + wire + e.attributes(shape, known, v) + ">")
	e.Value(b, shapeID, v)
	closeTag(b, wire)
}

// attributes renders the members a structure declares as XML attributes, with
// the namespace declaration a prefixed one needs: S3's Grantee carries
// `xsi:type`, declared under http://www.w3.org/2001/XMLSchema-instance.
func (e Encoder) attributes(shape model.Shape, known bool, v any) string {
	t, ok := v.(map[string]any)
	if !known || !ok {
		return ""
	}
	var b strings.Builder
	declared := map[string]bool{}
	for _, name := range sortedMembers(shape) {
		m := shape.Members[name]
		if !m.Binding.XMLAttribute {
			continue
		}
		val, present := t[name]
		if !present {
			val, present = t[m.Binding.Name]
		}
		if !present {
			continue
		}
		if prefix, _, has := strings.Cut(m.Binding.Name, ":"); has && m.Binding.XMLNamespace != "" && !declared[prefix] {
			declared[prefix] = true
			fmt.Fprintf(&b, " xmlns:%s=%q", prefix, m.Binding.XMLNamespace)
		}
		fmt.Fprintf(&b, " %s=%q", m.Binding.Name, Escape(fmt.Sprint(val)))
	}
	return b.String()
}

// shape looks a shape up, and finds nothing for an Encoder with no model --
// a fault body is written with none.
func (e Encoder) shape(id string) (model.Shape, bool) {
	if e.Svc == nil {
		return model.Shape{}, false
	}
	s, ok := e.Svc.Shapes[id]
	return s, ok
}

// inBody reports whether a member travels in the body at all.
func inBody(b model.MemberBinding) bool {
	switch b.Location {
	case "header", "prefixHeaders", "statusCode", "label", "query", "queryParams":
		return false
	}
	return true
}

// list writes an indexed list. The element name comes from the list shape's own
// member -- `item` in ec2, `member` where the specification says nothing.
func (e Encoder) list(b *strings.Builder, wire string, shape model.Shape, flat bool, items []any) {
	if flat {
		for _, item := range items {
			e.Member(b, wire, shape.Member, false, item)
		}
		return
	}
	elem := shape.MemberBinding.Name
	if elem == "" {
		elem = "member"
	}
	open(b, wire)
	for _, item := range items {
		e.Member(b, elem, shape.Member, false, item)
	}
	closeTag(b, wire)
}

// entries writes a map's entries, each carrying its key and value explicitly.
func (e Encoder) entries(b *strings.Builder, shape model.Shape, m map[string]any) {
	for _, k := range sortedKeys(m) {
		b.WriteString("<entry>")
		e.entry(b, shape, k, m[k])
		b.WriteString("</entry>")
	}
}

func (e Encoder) entry(b *strings.Builder, shape model.Shape, k string, v any) {
	kn, vn := shape.KeyBinding.Name, shape.MemberBinding.Name
	if kn == "" {
		kn = "key"
	}
	if vn == "" {
		vn = "value"
	}
	open(b, kn)
	b.WriteString(Escape(k))
	closeTag(b, kn)
	e.Member(b, vn, shape.Member, false, v)
}

// resolve maps one key of a produced record onto the member it stands for,
// answering the element name to write, the shape to write it with, and whether
// the member is flattened.
func (e Encoder) resolve(shape model.Shape, known bool, key string) (wire, child string, flat bool) {
	if !known || (shape.Kind != model.KindStructure && shape.Kind != model.KindUnion) {
		return key, "", false
	}
	if m, ok := shape.Members[key]; ok {
		if m.Binding.Name != "" {
			return m.Binding.Name, m.Shape, m.Binding.XMLFlattened
		}
		return key, m.Shape, m.Binding.XMLFlattened
	}
	// Already a wire name. Sorted so two members sharing one wire name -- which
	// no served model has, but which a vendor could introduce -- resolve the
	// same way on every run rather than by map order.
	for _, n := range sortedMembers(shape) {
		if m := shape.Members[n]; m.Binding.Name == key {
			return key, m.Shape, m.Binding.XMLFlattened
		}
	}
	return key, "", false
}

func open(b *strings.Builder, name string)     { b.WriteString("<" + name + ">") }
func closeTag(b *strings.Builder, name string) { b.WriteString("</" + name + ">") }

// declaredOrder is a record's keys in the shape's declared member order, by
// member name or wire name, with keys the shape does not declare last and
// sorted.
func declaredOrder(shape model.Shape, known bool, m map[string]any) []string {
	keys := sortedKeys(m)
	if !known || len(shape.MemberOrder) == 0 {
		return keys
	}
	rank := map[string]int{}
	for i, name := range shape.MemberOrder {
		rank[name] = i
		if wire := shape.Members[name].Binding.Name; wire != "" {
			rank[wire] = i
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		ri, oki := rank[keys[i]]
		rj, okj := rank[keys[j]]
		if oki != okj {
			return oki
		}
		return oki && ri < rj
	})
	return keys
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedMembers(shape model.Shape) []string {
	names := make([]string, 0, len(shape.Members))
	for n := range shape.Members {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Escape makes s safe as element text or an attribute value.
func Escape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}
