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
