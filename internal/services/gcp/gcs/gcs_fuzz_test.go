package gcs

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzParsePath(f *testing.F) {
	f.Add("/storage/v1/b/bucket/o/key")
	f.Add("/upload/storage/v1/b/b/o")
	f.Add("/b/x/o/a%2Fb")
	f.Add("")
	f.Fuzz(func(t *testing.T, path string) {
		_, _ = parsePath(path)
	})
}

func FuzzObjectBytes(f *testing.F) {
	f.Add("o", "hello")
	f.Add("", "v")
	f.Add("a/b", "x")
	f.Fuzz(func(t *testing.T, name, body string) {
		p := New(spitest.Deps(t))
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.insert", Input: map[string]any{"name": "b"}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": name}, Body: io.NopCloser(bytes.NewReader([]byte(body)))})
		got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.get", Input: map[string]any{"bucket": "b", "object": name, "alt": "media"}})
		if err == nil && got != nil && got.Stream != nil {
			_, _ = io.ReadAll(got.Stream)
			_ = got.Stream.Close()
		}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.delete", Input: map[string]any{"bucket": "b", "object": name}})
	})
}
