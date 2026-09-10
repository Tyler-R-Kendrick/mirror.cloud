package gcs

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestGCSJSONCharacterization(t *testing.T) {
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
		if in["alt"] == "media" && res.Stream != nil {
			b, _ := io.ReadAll(res.Stream)
			_ = res.Stream.Close()
			return map[string]any{"status": res.Status, "body": string(b)}
		}
		return res.Output
	}
	golden.AssertJSON(t, map[string]any{
		"create":    inv("storage.buckets.insert", map[string]any{"name": "snap"}, nil),
		"duplicate": inv("storage.buckets.insert", map[string]any{"name": "snap"}, nil),
		"empty":     inv("storage.buckets.insert", map[string]any{}, nil),
		"get":       inv("storage.buckets.get", map[string]any{"bucket": "snap"}, nil),
		"list":      inv("storage.buckets.list", map[string]any{}, nil),
		"insert":    inv("storage.objects.insert", map[string]any{"bucket": "snap", "name": "o"}, []byte("hello")),
		"meta":      inv("storage.objects.get", map[string]any{"bucket": "snap", "object": "o"}, nil),
		"media":     inv("storage.objects.get", map[string]any{"bucket": "snap", "object": "o", "alt": "media"}, nil),
		"copy":      inv("storage.objects.copy", map[string]any{"bucket": "snap", "object": "o", "destinationBucket": "snap", "destinationObject": "o2"}, nil),
		"objects":   inv("storage.objects.list", map[string]any{"bucket": "snap"}, nil),
		"delete":    inv("storage.objects.delete", map[string]any{"bucket": "snap", "object": "o2"}, nil),
		"missing":   inv("storage.objects.get", map[string]any{"bucket": "snap", "object": "nope"}, nil),
		"nobucket":  inv("storage.buckets.get", map[string]any{"bucket": "nope"}, nil),
	})
}
