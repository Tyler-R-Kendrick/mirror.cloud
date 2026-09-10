package blobs

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzBlobBytes(f *testing.F) {
	f.Add("o", "hello")
	f.Add("", "v")
	f.Add("a/b", "x")
	f.Fuzz(func(t *testing.T, name, body string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateContainer", Input: map[string]any{"container": "b"}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBlob", Input: map[string]any{"container": "b", "blob": name}, Body: io.NopCloser(bytes.NewReader([]byte(body)))})
		got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBlob", Input: map[string]any{"container": "b", "blob": name}})
		if err == nil && got != nil && got.Stream != nil {
			_, _ = io.ReadAll(got.Stream)
			_ = got.Stream.Close()
		}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteBlob", Input: map[string]any{"container": "b", "blob": name}})
	})
}
