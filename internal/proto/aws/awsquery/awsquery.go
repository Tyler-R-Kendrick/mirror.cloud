// Package awsquery implements the AWS query protocol.
package awsquery

import (
	"encoding/json"
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
type Codec struct{ json bool }

// NewJSON returns the SQS Query endpoint's JSON response variant.
func NewJSON() Codec { return Codec{json: true} }

func (Codec) Protocol() model.Protocol { return model.ProtoAWSQuery }

func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	action := r.Form.Get("Action")
	if action == "" {
		if svc.ID == "aws.sqs" {
			return nil, &spi.Fault{Code: "UnknownOperationException", Message: "The action or operation requested is not valid.", HTTPStatus: http.StatusNotFound, Fault: "client"}
		}
		return nil, spi.NotImplemented(svc.ID, "unknown", "emulate")
	}
	if svc.ID == "aws.sqs" && sqsQueueURLRequest(r) && (action == "CreateQueue" || action == "ListQueues") {
		return nil, sqsInvalidAction(action)
	}
	if op := svc.OperationByName(action); op != nil {
		return op, nil
	}
	if svc.ID == "aws.sqs" {
		return nil, sqsInvalidAction(action)
	}
	return nil, spi.NotImplemented(svc.ID, action, "emulate")
}

func sqsQueueURLRequest(r *http.Request) bool {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	return len(parts) >= 2 && len(parts[len(parts)-2]) == 12 && isDigits(parts[len(parts)-2])
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

func sqsInvalidAction(action string) *spi.Fault {
	return &spi.Fault{Code: "InvalidAction", Message: fmt.Sprintf("The action %s is not valid for this endpoint.", action), HTTPStatus: http.StatusBadRequest, Fault: "client"}
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

func (c Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	status := resp.Status
	if status == 0 {
		status = 200
	}
	if c.json && svc.ID == "aws.sqs" && op.Name == "GetQueueAttributes" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"GetQueueAttributesResponse": map[string]any{"GetQueueAttributesResult": map[string]any{"Attributes": sqsAttributes(resp.Output["Attributes"])}}})
	}
	if c.json && svc.ID == "aws.sqs" && op.Name == "ReceiveMessage" {
		result := map[string]any{}
		if messages, ok := resp.Output["Messages"].([]any); ok && len(messages) > 0 {
			result["Message"] = messages[0]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		return json.NewEncoder(w).Encode(map[string]any{"ReceiveMessageResponse": map[string]any{"ReceiveMessageResult": result}})
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
		if svc.ID == "aws.sqs" && op.Name == "GetQueueAttributes" {
			writeSQSAttributes(&b, resp.Output["Attributes"])
		} else {
			writeXML(&b, resp.Output)
		}
		fmt.Fprintf(&b, `</%sResult><ResponseMetadata><RequestId>mirror</RequestId></ResponseMetadata></%sResponse>`, op.Name, op.Name)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func sqsAttributes(value any) []any {
	attrs, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(attrs))
	for key := range attrs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{"Name": key, "Value": fmt.Sprint(attrs[key])})
	}
	return out
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
	if svc.ID == "aws.sqs" && f.Code == "UnknownOperationException" {
		w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
		w.WriteHeader(status)
		_, err := fmt.Fprintf(w, "<UnknownOperationException><Message>%s</Message><RequestId>%s</RequestId></UnknownOperationException>", xmlEscape(f.Message), requestID)
		return err
	}
	w.Header().Set("Content-Type", "text/xml; charset=UTF-8")
	w.WriteHeader(status)
	_, err := fmt.Fprintf(w, `<ErrorResponse><Error><Type>%s</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>%s</RequestId></ErrorResponse>`, typ, f.Code, xmlEscape(f.Message), requestID)
	return err
}

func writeSQSAttributes(b *strings.Builder, value any) {
	attrs, ok := value.(map[string]any)
	if !ok {
		writeXML(b, map[string]any{"Attributes": value})
		return
	}
	keys := make([]string, 0, len(attrs))
	for key := range attrs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	b.WriteString("<Attributes>")
	for _, key := range keys {
		fmt.Fprintf(b, "<Attribute><Name>%s</Name><Value>%s</Value></Attribute>", xmlEscape(key), xmlEscape(fmt.Sprint(attrs[key])))
	}
	b.WriteString("</Attributes>")
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
