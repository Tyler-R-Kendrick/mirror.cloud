// Package awsquery implements the AWS query protocol.
package awsquery

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec implements proto.Codec for awsQuery.
type Codec struct{}

func (Codec) Protocol() model.Protocol { return model.ProtoAWSQuery }

func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	action := r.Form.Get("Action")
	if action == "" {
		return nil, spi.NotImplemented(svc.ID, "unknown", "emulate")
	}
	if op := svc.OperationByName(action); op != nil {
		return op, nil
	}
	return nil, spi.NotImplemented(svc.ID, action, "emulate")
}

func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	in := map[string]any{}
	for k, vs := range r.Form {
		if k == "Action" || k == "Version" {
			continue
		}
		if len(vs) == 1 {
			in[k] = vs[0]
		} else {
			arr := make([]any, len(vs))
			for i, v := range vs {
				arr[i] = v
			}
			in[k] = arr
		}
	}
	unflatten(svc, op.Input, r.Form, in)
	return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
}

func (Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	status := resp.Status
	if status == 0 {
		status = 200
	}
	w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
	w.WriteHeader(status)
	ns := svc.XMLNamespace
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	fmt.Fprintf(&b, `<%sResponse xmlns="%s">`, op.Name, ns)
	e := enc{svc: svc}
	if svc.Protocol == model.ProtoEC2Query {
		e.value(&b, op.Output, resp.Output)
		fmt.Fprintf(&b, `<requestId>mirror</requestId></%sResponse>`, op.Name)
	} else {
		fmt.Fprintf(&b, `<%sResult>`, op.Name)
		e.value(&b, op.Output, resp.Output)
		fmt.Fprintf(&b, `</%sResult><ResponseMetadata><RequestId>mirror</RequestId></ResponseMetadata></%sResponse>`, op.Name, op.Name)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func (Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	status := f.HTTPStatus
	if status == 0 {
		status = 400
	}
	typ := "Sender"
	if f.Fault == "server" {
		typ = "Receiver"
		if f.HTTPStatus == 0 {
			status = 500
		}
	}
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
		status = 501
	}
	w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
	w.WriteHeader(status)
	_, err := fmt.Fprintf(w, `<ErrorResponse><Error><Type>%s</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>%s</RequestId></ErrorResponse>`, typ, f.Code, xmlEscape(f.Message), requestID)
	return err
}

// enc serializes a response by walking the declared output shape alongside the
// value the pack or the engine produced.
//
// Without the shape a response carries whatever map keys it was handed, which
// is only correct when the producer knew the wire names itself. The hand-written
// ec2 pack knew some of them -- it answers `vpcSet` because someone typed
// `vpcSet` -- and no bundle generated from the model can: the model calls that
// member `Vpcs`. Renaming here is what lets a bundle answer in declared names
// and still reach a real client.
//
// A producer that already answers in wire names must not be renamed twice, so a
// key that is not a declared member but *is* some member's wire name passes
// through unchanged, carrying that member's shape onward. Both kinds of producer
// therefore serialize identically, which is what the extraction equivalence gate
// compares.
//
// XML attributes are not honoured here. No awsQuery or ec2Query response shape
// in the served models declares one, and a member written as an element where an
// attribute was declared would be a silent wrong answer rather than an obvious
// one, so this records the gap rather than guessing at it.
type enc struct{ svc *model.Service }

// value writes the contents of one element: the members of a structure, the
// entries of a map, or the text of a scalar.
func (e enc) value(b *strings.Builder, shapeID string, v any) {
	shape, known := e.svc.Shapes[shapeID]
	switch t := v.(type) {
	case map[string]any:
		if known && shape.Kind == model.KindMap {
			e.entries(b, shape, t)
			return
		}
		for _, k := range sortedKeys(t) {
			wire, child, flat := e.resolve(shape, known, k)
			e.member(b, wire, child, flat, t[k])
		}
	case []any:
		// A list the shape did not describe -- an undeclared member, or a
		// producer answering something the model does not carry. The wrapper
		// the codec has always written is kept so nothing that worked before
		// this change stops working.
		for _, item := range t {
			b.WriteString("<member>")
			e.value(b, "", item)
			b.WriteString("</member>")
		}
	case nil:
	default:
		b.WriteString(xmlEscape(fmt.Sprint(t)))
	}
}

// member writes one named member, wrapper included -- except for a flattened
// list or map, which has no wrapper: each element carries the member's own name.
func (e enc) member(b *strings.Builder, wire, shapeID string, flat bool, v any) {
	shape, known := e.svc.Shapes[shapeID]
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
	open(b, wire)
	e.value(b, shapeID, v)
	closeTag(b, wire)
}

// list writes an indexed list. The element name comes from the list shape's own
// member -- `item` in ec2, `member` where the specification says nothing.
func (e enc) list(b *strings.Builder, wire string, shape model.Shape, flat bool, items []any) {
	if flat {
		for _, item := range items {
			e.member(b, wire, shape.Member, false, item)
		}
		return
	}
	elem := shape.MemberBinding.Name
	if elem == "" {
		elem = "member"
	}
	open(b, wire)
	for _, item := range items {
		e.member(b, elem, shape.Member, false, item)
	}
	closeTag(b, wire)
}

// entries writes a map's entries, each carrying its key and value explicitly.
func (e enc) entries(b *strings.Builder, shape model.Shape, m map[string]any) {
	for _, k := range sortedKeys(m) {
		b.WriteString("<entry>")
		e.entry(b, shape, k, m[k])
		b.WriteString("</entry>")
	}
}

func (e enc) entry(b *strings.Builder, shape model.Shape, k string, v any) {
	kn, vn := shape.KeyBinding.Name, shape.MemberBinding.Name
	if kn == "" {
		kn = "key"
	}
	if vn == "" {
		vn = "value"
	}
	open(b, kn)
	b.WriteString(xmlEscape(k))
	closeTag(b, kn)
	e.member(b, vn, shape.Member, false, v)
}

// resolve maps one key of a produced record onto the member it stands for,
// answering the element name to write, the shape to write it with, and whether
// the member is flattened.
func (e enc) resolve(shape model.Shape, known bool, key string) (wire, child string, flat bool) {
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

func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}

// FormEncode is exported for tests.
func FormEncode(v url.Values) string { return v.Encode() }

// unflatten rebuilds the structured input the model describes from the flat
// query parameters the wire carries.
//
// The AWS query protocol has no nesting: a list arrives as Name.member.1,
// Name.member.2, a map as Name.entry.1.key and Name.entry.1.value, a nested
// structure as Name.Field, and any of them may be "flattened", which drops the
// member/entry segment. Decoding without the shape therefore yields members
// literally named "Identities.member.1" and no member named "Identities" at
// all -- which is why the hand-written packs read those dotted keys directly,
// and why the engine, which enforces the model's required members, rejected
// every real request to an awsQuery service.
//
// The flat keys are left in place beside the structured ones. Six packs still
// read them, and they go when the last of those is extracted; until then a
// decoder that removed them would break services this change is not otherwise
// touching.
func unflatten(svc *model.Service, shapeID string, form url.Values, in map[string]any) {
	shape, ok := svc.Shapes[shapeID]
	if !ok || shape.Kind != model.KindStructure {
		return
	}
	for name, member := range shape.Members {
		if v, found := valueAt(svc, member, requestName(svc, name, member.Binding), form, 0); found {
			in[name] = v
		}
	}
}

// requestName is the form field one member arrives under.
//
// awsQuery asks for a member by its xmlName, and by its member name where the
// specification gives no xmlName. ec2Query names its request fields separately
// from its response elements: explicitly with ec2QueryName, and otherwise by
// capitalizing the xmlName -- `dryRun` on the way out is `DryRun` on the way in.
// Reading ec2 requests by the response name means never finding a structured
// member at all, which is why ec2's own pack reads the flat keys by hand.
func requestName(svc *model.Service, name string, b model.MemberBinding) string {
	if svc.Protocol == model.ProtoEC2Query {
		switch {
		case b.QueryName != "":
			return b.QueryName
		case b.Name != "":
			return strings.ToUpper(b.Name[:1]) + b.Name[1:]
		}
		return name
	}
	if b.Name != "" {
		return b.Name
	}
	return name
}

// flattened reports whether a list or map arrives without its member/entry
// segment. awsQuery flattens only where the specification says so; ec2Query
// flattens every list in a request, which no trait records because the protocol
// itself decides it.
func flattened(svc *model.Service, b model.MemberBinding) bool {
	return b.XMLFlattened || svc.Protocol == model.ProtoEC2Query
}

// maxDepth bounds the shape-graph descent. valueAt walks the model, not the
// request, so a shape that reaches itself would recurse until the stack ran
// out however small the request was -- a crash rather than a rejected input.
// No such shape exists today: the deepest awsQuery or ec2Query input in the
// generated models is 11 levels, at aws.autoscaling.PutScalingPolicy. But the
// models move under specs-refresh without any code change here, so the bound
// is what keeps a vendor's edit from turning into a panic. It is set to roughly
// three times the observed maximum: deep enough that no real shape reaches it,
// shallow enough to stop a cycle promptly.
const maxDepth = 32

// valueAt reads one member, whatever its shape, from prefix in form.
func valueAt(svc *model.Service, member model.Member, prefix string, form url.Values, depth int) (any, bool) {
	if depth > maxDepth {
		return nil, false
	}
	shape, ok := svc.Shapes[member.Shape]
	if !ok {
		if v := form.Get(prefix); v != "" || form.Has(prefix) {
			return v, true
		}
		return nil, false
	}
	switch shape.Kind {
	case model.KindList:
		return listAt(svc, shape, prefix, flattened(svc, member.Binding), form, depth)
	case model.KindMap:
		return mapAt(svc, shape, prefix, flattened(svc, member.Binding), form, depth)
	case model.KindStructure, model.KindUnion:
		out := map[string]any{}
		for name, field := range shape.Members {
			at := prefix + "." + requestName(svc, name, field.Binding)
			if v, found := valueAt(svc, field, at, form, depth+1); found {
				out[name] = v
			}
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	default:
		if form.Has(prefix) {
			return form.Get(prefix), true
		}
		return nil, false
	}
}

// listAt reads an indexed list. Indices are one-based and contiguous, which is
// what every AWS SDK emits; a gap ends the list rather than being skipped, so a
// truncated request does not silently become a shorter valid one.
func listAt(svc *model.Service, shape model.Shape, prefix string, flat bool, form url.Values, depth int) (any, bool) {
	at := prefix
	if !flat {
		at = prefix + ".member"
	}
	item := model.Member{Shape: shape.Member}
	var out []any
	for i := 1; ; i++ {
		v, found := valueAt(svc, item, fmt.Sprintf("%s.%d", at, i), form, depth+1)
		if !found {
			break
		}
		out = append(out, v)
	}
	if out == nil {
		return nil, false
	}
	return out, true
}

// mapAt reads an indexed map, whose entries carry an explicit key and value.
func mapAt(svc *model.Service, shape model.Shape, prefix string, flat bool, form url.Values, depth int) (any, bool) {
	at := prefix
	if !flat {
		at = prefix + ".entry"
	}
	key := model.Member{Shape: shape.Key}
	value := model.Member{Shape: shape.Member}
	out := map[string]any{}
	for i := 1; ; i++ {
		base := fmt.Sprintf("%s.%d", at, i)
		k, found := valueAt(svc, key, base+".key", form, depth+1)
		if !found {
			break
		}
		v, _ := valueAt(svc, value, base+".value", form, depth+1)
		out[fmt.Sprint(k)] = v
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}
