// Package awsjson implements awsJson1_0 and awsJson1_1 codecs.
package awsjson

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec implements proto.Codec for AWS JSON protocols.
type Codec struct {
	proto model.Protocol
}

// New10 returns an awsJson1_0 codec.
func New10() *Codec { return &Codec{proto: model.ProtoAWSJSON10} }

// New11 returns an awsJson1_1 codec.
func New11() *Codec { return &Codec{proto: model.ProtoAWSJSON11} }

func (c *Codec) Protocol() model.Protocol { return c.proto }

func (c *Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return nil, spi.NotImplemented(svc.ID, "unknown", "emulate")
	}
	name := target
	if i := strings.LastIndex(target, "."); i >= 0 {
		name = target[i+1:]
	}
	if op := svc.OperationByName(name); op != nil {
		return op, nil
	}
	return nil, spi.NotImplemented(svc.ID, name, "emulate")
}

func (c *Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	in := map[string]any{}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, &spi.Fault{Code: "SerializationException", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
		}
	}
	return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
}

func (c *Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
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
	ct := "application/x-amz-json-1.0"
	if c.proto == model.ProtoAWSJSON11 {
		ct = "application/x-amz-json-1.1"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(status)
	out := resp.Output
	if out == nil {
		out = map[string]any{}
	}
	return json.NewEncoder(w).Encode(out)
}

func (c *Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	status := f.HTTPStatus
	if status == 0 {
		if f.Fault == "server" {
			status = 500
		} else {
			status = 400
		}
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-errortype", f.Code)
	w.Header().Set("x-amzn-requestid", requestID)
	if f.Code == "MirrorNotImplemented" {
		w.Header().Set("x-mirror-not-implemented", svc.ID+"."+op.Name)
	}
	w.WriteHeader(status)
	body := map[string]any{"__type": f.Code, "message": f.Message}
	for k, v := range f.Fields {
		body[k] = v
	}
	return json.NewEncoder(w).Encode(body)
}
