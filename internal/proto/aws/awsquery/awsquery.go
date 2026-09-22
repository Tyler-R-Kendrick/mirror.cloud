// Package awsquery implements the AWS query protocol.
package awsquery

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/aws/xmlenc"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec implements proto.Codec for awsQuery.
//
// JSON selects the protocol's JSON dialect for responses: the same document,
// transcoded (see json.go). It is a field rather than a sniff inside Encode
// because content negotiation reads the request, and Encode is handed only the
// response -- and because the edge is already where SQS's other dialect choice
// is made, between the query and JSON protocols.
type Codec struct{ JSON bool }

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

func (c Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	status := resp.Status
	if status == 0 {
		status = 200
	}
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	// A model with no xmlNamespace gets no attribute. `xmlns=""` is not an
	// absent namespace: it explicitly *undeclares* the default one, which is a
	// different document and a different thing for a strict parser to read.
	// Four services reach here without the trait, SQS among them -- its
	// current specification describes awsJson1_0 and no longer carries the
	// query dialect's namespace -- so every one of them was serving `xmlns=""`.
	if ns := svc.XMLNamespace; ns != "" {
		fmt.Fprintf(&b, `<%sResponse xmlns="%s">`, op.Name, ns)
	} else {
		fmt.Fprintf(&b, `<%sResponse>`, op.Name)
	}
	e := xmlenc.Encoder{Svc: svc}
	if svc.Protocol == model.ProtoEC2Query {
		e.Value(&b, op.Output, resp.Output)
		fmt.Fprintf(&b, `<requestId>mirror</requestId></%sResponse>`, op.Name)
	} else {
		// An operation that produced nothing gets a self-closing result
		// element, which is what AWS returns for an empty ReceiveMessage and
		// what a client matching on `<ReceiveMessageResult/>` is looking for.
		// The open/close pair says the same thing to an XML parser and a
		// different thing to everything else reading the bytes.
		var body strings.Builder
		e.Value(&body, op.Output, resp.Output)
		if body.Len() == 0 {
			fmt.Fprintf(&b, `<%sResult/>`, op.Name)
		} else {
			fmt.Fprintf(&b, `<%sResult>%s</%sResult>`, op.Name, body.String(), op.Name)
		}
		fmt.Fprintf(&b, `<ResponseMetadata><RequestId>mirror</RequestId></ResponseMetadata></%sResponse>`, op.Name)
	}
	// The JSON dialect is the same document, so it is transcoded from the one
	// just built rather than encoded a second time. See json.go. A document
	// that will not parse is a defect in the encoder above, not something to
	// hide from the client by silently serving XML to a request that asked for
	// JSON, so the failure is returned.
	if c.JSON {
		encoded, err := xmlToJSON(b.String())
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, err = w.Write(encoded)
		return err
	}
	w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
	w.WriteHeader(status)
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
	_, err := fmt.Fprintf(w, `<ErrorResponse><Error><Type>%s</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>%s</RequestId></ErrorResponse>`, typ, f.Code, xmlenc.Escape(f.Message), requestID)
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
