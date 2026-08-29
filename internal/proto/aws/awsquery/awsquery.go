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
	if svc.Protocol == model.ProtoEC2Query {
		writeXML(&b, resp.Output)
		fmt.Fprintf(&b, `<requestId>mirror</requestId></%sResponse>`, op.Name)
	} else {
		fmt.Fprintf(&b, `<%sResult>`, op.Name)
		writeXML(&b, resp.Output)
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

func writeXML(b *strings.Builder, v any) {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(b, "<%s>", k)
			writeXML(b, t[k])
			fmt.Fprintf(b, "</%s>", k)
		}
	case []any:
		for _, item := range t {
			b.WriteString("<member>")
			writeXML(b, item)
			b.WriteString("</member>")
		}
	case nil:
	default:
		b.WriteString(xmlEscape(fmt.Sprint(t)))
	}
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
		wire := name
		if member.Binding.Name != "" {
			wire = member.Binding.Name
		}
		if v, found := valueAt(svc, member, wire, form, 0); found {
			in[name] = v
		}
	}
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
		return listAt(svc, shape, prefix, member.Binding.XMLFlattened, form, depth)
	case model.KindMap:
		return mapAt(svc, shape, prefix, member.Binding.XMLFlattened, form, depth)
	case model.KindStructure, model.KindUnion:
		out := map[string]any{}
		for name, field := range shape.Members {
			wire := name
			if field.Binding.Name != "" {
				wire = field.Binding.Name
			}
			if v, found := valueAt(svc, field, prefix+"."+wire, form, depth+1); found {
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
