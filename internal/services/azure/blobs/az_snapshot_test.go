package blobs

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestAzureBlobCharacterization(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any, body []byte) any {
		t.Helper()
		req := &spi.Request{Identity: id, Operation: op, Input: in}
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		res, err := p.Invoke(ctx, req)
		if err != nil {
			f := err.(*spi.Fault)
			return map[string]any{"error": f.Code, "status": f.HTTPStatus, "message": f.Message}
		}
		if op == "GetBlob" && res.Stream != nil {
			b, _ := io.ReadAll(res.Stream)
			_ = res.Stream.Close()
			return map[string]any{"status": res.Status, "body": string(b)}
		}
		out := map[string]any{"status": res.Status, "output": res.Output}
		return out
	}
	golden.AssertJSON(t, map[string]any{
		"create":     inv("CreateContainer", map[string]any{"container": "snap"}, nil),
		"duplicate":  inv("CreateContainer", map[string]any{"container": "snap"}, nil),
		"empty":      inv("CreateContainer", map[string]any{}, nil),
		"get":        inv("GetContainer", map[string]any{"container": "snap"}, nil),
		"list":       inv("ListContainers", nil, nil),
		"put":        inv("PutBlob", map[string]any{"container": "snap", "blob": "o"}, []byte("hello")),
		"media":      inv("GetBlob", map[string]any{"container": "snap", "blob": "o"}, nil),
		"blobs":      inv("ListBlobs", map[string]any{"container": "snap"}, nil),
		"delete":     inv("DeleteBlob", map[string]any{"container": "snap", "blob": "o"}, nil),
		"missing":    inv("GetBlob", map[string]any{"container": "snap", "blob": "nope"}, nil),
		"nobucket":   inv("GetContainer", map[string]any{"container": "nope"}, nil),
		"del_miss_b": inv("DeleteBlob", map[string]any{"container": "snap", "blob": "nope"}, nil),
		"del_miss_c": inv("DeleteContainer", map[string]any{"container": "nope"}, nil),
	})
}
