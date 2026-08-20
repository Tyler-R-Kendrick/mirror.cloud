// Package restjson implements restJson1.
package restjson

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/proto/awsjson"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec is restJson1, reusing JSON encode/decode with HTTP routing from the model.
type Codec struct{}

func (Codec) Protocol() model.Protocol { return model.ProtoRESTJSON1 }

func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	for i := range svc.Operations {
		op := &svc.Operations[i]
		if op.HTTP.Method == r.Method {
			return op, nil
		}
	}
	return awsjson.New10().Route(svc, r)
}

func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	body, _ := io.ReadAll(r.Body)
	in := map[string]any{}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}
	for k, vs := range r.URL.Query() {
		if _, ok := in[k]; !ok {
			in[k] = vs[0]
		}
	}
	return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
}

func (Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	status := resp.Status
	if status == 0 {
		status = op.HTTP.Code
		if status == 0 {
			status = 200
		}
	}
	for k, vs := range resp.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
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
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-errortype", f.Code)
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
		status = 501
	}
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(map[string]any{"message": f.Message, "__type": f.Code})
}
