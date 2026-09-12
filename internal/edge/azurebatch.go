package edge

import (
	"bufio"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
)

// This file fans an Azure Blob batch (SubmitBatch, POST ?comp=batch) out into
// the same dispatch pipeline every other request uses: each multipart part is
// a raw HTTP request that is served recursively and replayed into the
// multipart/mixed response. The engine is deliberately single-operation, so
// the fan-out lives at the edge, next to the SQS envelope special cases.
//
// A malformed envelope (missing or duplicate boundary) is not an outer fault:
// Azurite answers 202 with one failed sub-response carrying the error code,
// and the pinned tests read it out of the body. Only a missing Content-Type
// header faults the whole request.

// azureBatchBoundary extracts the multipart boundary, tolerant of the forms
// the pinned tests use: case-insensitive parameter name, whitespace before
// the value, quoted values containing '='. The second return is the Azurite
// error code for the malformed forms ("" when the boundary is usable).
func azureBatchBoundary(ct string) (string, string) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(ct)), "multipart/mixed") {
		return "", "InvalidHeaderValue"
	}
	var boundaries []string
	for _, seg := range strings.Split(ct, ";")[1:] {
		k, v, ok := strings.Cut(strings.TrimSpace(seg), "=")
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(k), "boundary") {
			boundaries = append(boundaries, strings.Trim(strings.TrimSpace(v), `"`))
		}
	}
	if len(boundaries) > 1 {
		return "", "InvalidInput"
	}
	if len(boundaries) == 0 || boundaries[0] == "" {
		return "", "InvalidHeaderValue"
	}
	return boundaries[0], ""
}

type azureBatchPart struct {
	id      string
	status  int
	headers http.Header
	body    []byte
}

// azureBatchFaultPart builds the single failed sub-response Azurite returns
// when the envelope itself is malformed.
func azureBatchFaultPart(code, message string) azureBatchPart {
	return azureBatchPart{
		id:      "0",
		status:  http.StatusBadRequest,
		headers: http.Header{"x-ms-error-code": {code}, "Content-Type": {"application/xml"}},
		body:    []byte(`<Error><Code>` + code + `</Code><Message>` + message + `</Message></Error>`),
	}
}

// serveAzureBatch handles POST ?comp=batch for azure.blobs.
func (s *Server) serveAzureBatch(w http.ResponseWriter, r *http.Request, rid string) {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		w.Header().Set("x-ms-error-code", "InvalidHeaderValue")
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `<Error><Code>InvalidHeaderValue</Code><Message>The value for one of the HTTP headers is not in the correct format.</Message></Error>`)
		return
	}
	boundary, errCode := azureBatchBoundary(ct)
	if errCode != "" {
		writeAzureBatchResponse(w, rid, []azureBatchPart{azureBatchFaultPart(errCode, "The value for one of the HTTP headers is not in the correct format.")})
		return
	}
	// Container-scoped blob batches name their container in the path; the
	// service-level and table batches are unscoped.
	scope := strings.Trim(r.URL.Path, "/")
	if scope == "$batch" {
		scope = ""
	}
	parts := s.serveAzureBatchParts(r, multipart.NewReader(r.Body, boundary), scope)
	if len(parts) == 0 {
		parts = append(parts, azureBatchFaultPart("InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format."))
	}
	writeAzureBatchResponse(w, rid, parts)
}

// serveAzureBatchParts dispatches every part of one multipart level,
// recursing into changesets (table batches nest their operations in one).
func (s *Server) serveAzureBatchParts(outer *http.Request, mr *multipart.Reader, scope string) []azureBatchPart {
	var parts []azureBatchPart
	for i := 0; ; i++ {
		raw, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return append(parts, azureBatchFaultPart("InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format."))
		}
		if i >= 256 {
			return append(parts, azureBatchFaultPart("ExceedsMaxBatchRequestCount", "The batch operation exceeds maximum number of allowed subrequests."))
		}
		body, err := io.ReadAll(raw)
		if err != nil {
			return append(parts, azureBatchFaultPart("InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format."))
		}
		if nb, _ := azureBatchBoundary(raw.Header.Get("Content-Type")); nb != "" {
			parts = append(parts, s.serveAzureBatchParts(outer, multipart.NewReader(strings.NewReader(string(body)), nb), scope)...)
			continue
		}
		parts = append(parts, s.serveAzureBatchPart(outer, raw.Header.Get("Content-ID"), i, scope, body))
	}
	return parts
}

// serveAzureBatchPart parses one application/http part and dispatches it
// through the normal handler into a recorder.
func (s *Server) serveAzureBatchPart(outer *http.Request, contentID string, index int, scope string, body []byte) azureBatchPart {
	part := azureBatchPart{id: contentID, headers: http.Header{}}
	if part.id == "" {
		part.id = fmt.Sprint(index)
	}
	sub, err := http.ReadRequest(bufio.NewReader(strings.NewReader(string(body))))
	if err != nil {
		p := azureBatchFaultPart("InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format.")
		p.id = part.id
		return p
	}
	// A batch cannot nest; Azurite rejects it as unsupported.
	if strings.Contains(sub.URL.RawQuery, "comp=batch") {
		p := azureBatchFaultPart("InvalidInput", "Batch operation is not supported for this request.")
		p.id = part.id
		return p
	}
	// A container-scoped batch only serves sub-requests inside its container.
	if scope != "" && !strings.HasPrefix(sub.URL.Path, "/"+scope+"/") {
		p := azureBatchFaultPart("InvalidInput", "One of the request inputs is not valid.")
		p.id = part.id
		return p
	}
	sub.Host = outer.Host
	sub.RequestURI = sub.URL.RequestURI()
	if sub.Header.Get("Authorization") == "" {
		sub.Header.Set("Authorization", outer.Header.Get("Authorization"))
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, sub)
	res := rec.Result()
	part.status = res.StatusCode
	part.headers = res.Header
	part.body, _ = io.ReadAll(res.Body)
	_ = res.Body.Close()
	return part
}

// writeAzureBatchResponse assembles the 202 multipart/mixed answer.
func writeAzureBatchResponse(w http.ResponseWriter, rid string, parts []azureBatchPart) {
	boundary := "batchresponse_" + rid
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+boundary)
	w.WriteHeader(http.StatusAccepted)
	var b strings.Builder
	for _, p := range parts {
		b.WriteString("--" + boundary + "\r\n")
		b.WriteString("Content-Type: application/http\r\n")
		b.WriteString("Content-ID: " + p.id + "\r\n\r\n")
		reason := http.StatusText(p.status)
		if reason == "" {
			reason = "status"
		}
		fmt.Fprintf(&b, "HTTP/1.1 %d %s\r\n", p.status, reason)
		for k, vs := range p.headers {
			if strings.EqualFold(k, "Transfer-Encoding") || strings.EqualFold(k, "Content-Length") {
				continue
			}
			for _, v := range vs {
				b.WriteString(k + ": " + v + "\r\n")
			}
		}
		b.WriteString("\r\n")
		b.Write(p.body)
		b.WriteString("\r\n")
	}
	b.WriteString("--" + boundary + "--\r\n")
	_, _ = io.WriteString(w, b.String())
}
