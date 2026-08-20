// Package restxml implements the S3 restXml codec.
package restxml

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec implements proto.Codec for restXml.
type Codec struct{}

func (Codec) Protocol() model.Protocol { return model.ProtoRESTXML }

func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	// Behavior packs for S3 do their own routing via the edge using method+path.
	// Here we pick the first matching http method, then let packs interpret.
	name := r.Header.Get("X-Mirror-Operation")
	if name != "" {
		if op := svc.OperationByName(name); op != nil {
			return op, nil
		}
	}
	q := r.URL.Query()
	candidates := []string{}
	switch r.Method {
	case http.MethodPut:
		if strings.Contains(r.URL.Path, "/") && r.URL.Path != "/" {
			candidates = []string{"PutObject", "CreateBucket", "PutBucketVersioning", "PutBucketTagging", "PutBucketNotificationConfiguration", "PutObjectTagging", "UploadPart", "CopyObject"}
		}
	case http.MethodGet:
		candidates = []string{"GetObject", "ListObjectsV2", "ListObjects", "ListBuckets", "GetBucketLocation", "GetBucketVersioning", "GetBucketTagging", "ListObjectVersions", "ListParts", "ListMultipartUploads", "GetObjectTagging"}
	case http.MethodHead:
		candidates = []string{"HeadObject", "HeadBucket"}
	case http.MethodDelete:
		candidates = []string{"DeleteObject", "DeleteBucket", "AbortMultipartUpload"}
	case http.MethodPost:
		candidates = []string{"DeleteObjects", "CreateMultipartUpload", "CompleteMultipartUpload"}
	}
	_ = q
	for _, n := range candidates {
		if op := svc.OperationByName(n); op != nil {
			return op, nil
		}
	}
	if len(svc.Operations) > 0 {
		return &svc.Operations[0], nil
	}
	return nil, spi.NotImplemented(svc.ID, r.Method+" "+r.URL.Path, "emulate")
}

func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	in := map[string]any{}
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) > 0 && parts[0] != "" {
		in["Bucket"] = parts[0]
	}
	if len(parts) > 1 {
		in["Key"] = parts[1]
	}
	for k, vs := range r.URL.Query() {
		in[k] = vs[0]
	}
	req := &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}
	if r.Body != nil && (op.Name == "PutObject" || op.Name == "UploadPart" || op.Name == "CopyObject") {
		req.Body = r.Body
	} else if r.Body != nil {
		b, _ := io.ReadAll(r.Body)
		if len(b) > 0 {
			in["_body"] = string(b)
		}
	}
	return req, nil
}

func (Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	status := resp.Status
	if status == 0 {
		status = 200
	}
	for k, vs := range resp.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if resp.Stream != nil {
		w.WriteHeader(status)
		_, err := io.Copy(w, resp.Stream)
		_ = resp.Stream.Close()
		return err
	}
	if op.Name == "HeadObject" || op.Name == "HeadBucket" {
		w.WriteHeader(status)
		return nil
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if resp.Output == nil {
		return nil
	}
	type kv struct {
		XMLName xml.Name
		Value   string `xml:",chardata"`
	}
	// Simple XML object encoder.
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	root := op.Name + "Result"
	if op.Name == "ListBuckets" {
		root = "ListAllMyBucketsResult"
	}
	if op.Name == "ListObjectsV2" || op.Name == "ListObjects" {
		root = "ListBucketResult"
	}
	fmt.Fprintf(&b, "<%s>", root)
	write(resp.Output, &b)
	fmt.Fprintf(&b, "</%s>", root)
	_, err := io.WriteString(w, b.String())
	return err
}

func write(v any, b *strings.Builder) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			fmt.Fprintf(b, "<%s>", k)
			write(val, b)
			fmt.Fprintf(b, "</%s>", k)
		}
	case []any:
		for _, item := range t {
			b.WriteString("<member>")
			write(item, b)
			b.WriteString("</member>")
		}
	case nil:
	default:
		b.WriteString(xmlEscape(fmt.Sprint(t)))
	}
}

func (Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	status := f.HTTPStatus
	if status == 0 {
		status = 400
	}
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
		status = 501
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, err := fmt.Fprintf(w, `<Error><Code>%s</Code><Message>%s</Message><RequestId>%s</RequestId><HostId>mirror</HostId></Error>`, xmlEscape(f.Code), xmlEscape(f.Message), xmlEscape(requestID))
	return err
}

func xmlEscape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}
