// Package gcprest implements gcpRestJson.
package gcprest

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec implements proto.Codec for GCS JSON API.
type Codec struct{}

func (Codec) Protocol() model.Protocol { return model.ProtoGCPRESTSON }

func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	if a := r.URL.Query().Get("Action"); a != "" {
		return named(svc, a)
	}
	path := r.URL.Path
	switch {
	case strings.Contains(path, "/batch"):
		return named(svc, "storage.objects.insert")
	case (r.Method == http.MethodPost || r.Method == http.MethodPut) && strings.Contains(path, "/upload/"):
		return named(svc, "storage.objects.insert")
	case r.Method == http.MethodPost && strings.Contains(path, "/copyTo/"):
		return named(svc, "storage.objects.copy")
	case r.Method == http.MethodPost && strings.Contains(path, "/rewriteTo/"):
		return named(svc, "storage.objects.rewrite")
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/compose"):
		return named(svc, "storage.objects.compose")
	case r.Method == http.MethodPatch && strings.Contains(path, "/o/"):
		return named(svc, "storage.objects.patch")
	case r.Method == http.MethodPatch:
		return named(svc, "storage.buckets.patch")
	case r.Method == http.MethodGet && strings.Contains(path, "/b/") && strings.Contains(path, "/o/"):
		return named(svc, "storage.objects.get")
	case r.Method == http.MethodGet && strings.Contains(path, "/b/") && strings.HasSuffix(path, "/o"):
		return named(svc, "storage.objects.list")
	case r.Method == http.MethodPost && strings.Contains(path, "/b") && !strings.Contains(path, "/o"):
		return named(svc, "storage.buckets.insert")
	case r.Method == http.MethodGet && strings.Contains(path, "/b/") && !strings.Contains(path, "/o"):
		return named(svc, "storage.buckets.get")
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/b"):
		return named(svc, "storage.buckets.list")
	case r.Method == http.MethodDelete && strings.Contains(path, "/o/"):
		return named(svc, "storage.objects.delete")
	case r.Method == http.MethodDelete:
		return named(svc, "storage.buckets.delete")
	}
	if len(svc.Operations) > 0 {
		return &svc.Operations[0], nil
	}
	return nil, spi.NotImplemented(svc.ID, r.Method+" "+path, "emulate")
}

func named(svc *model.Service, n string) (*model.Operation, error) {
	if op := svc.OperationByName(n); op != nil {
		return op, nil
	}
	return &model.Operation{Name: n, HTTP: model.HTTPBinding{Method: http.MethodGet, Code: 200}}, nil
}

func (Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	in := map[string]any{}
	body, _ := io.ReadAll(r.Body)
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}
	for k, vs := range r.URL.Query() {
		in[k] = vs[0]
	}
	in["_path"] = r.URL.Path
	req := &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}
	if op.Name == "storage.objects.insert" {
		req.Body = io.NopCloser(strings.NewReader(string(body)))
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if resp.Output == nil {
		return nil
	}
	return json.NewEncoder(w).Encode(resp.Output)
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": status, "message": f.Message, "errors": []any{map[string]any{"reason": f.Code, "message": f.Message}}},
	})
}
