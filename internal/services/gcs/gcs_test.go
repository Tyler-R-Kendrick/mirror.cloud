package gcs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBucketCRUD(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	req := &spi.Request{Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"}, Input: map[string]any{"name": "b1"}, Operation: "storage.buckets.insert"}
	res, err := p.Invoke(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Output["name"] != "b1" {
		t.Fatalf("%v", res.Output)
	}
	req.Operation = "storage.buckets.get"
	if _, err := p.Invoke(ctx, req); err != nil {
		t.Fatal(err)
	}
	req.Operation = "storage.buckets.list"
	list, err := p.Invoke(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	items, _ := list.Output["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("list %v", list.Output)
	}
	req.Operation = "storage.buckets.patch"
	req.Input["location"] = "EU"
	if _, err := p.Invoke(ctx, req); err != nil {
		t.Fatal(err)
	}
	req.Operation = "storage.buckets.delete"
	if _, err := p.Invoke(ctx, req); err != nil {
		t.Fatal(err)
	}
}

func TestObjectMediaAndRange(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.insert", Input: map[string]any{"name": "b"}}); err != nil {
		t.Fatal(err)
	}
	body := []byte("abcdefghij")
	ins, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": "o"}, Body: io.NopCloser(bytes.NewReader(body))})
	if err != nil {
		t.Fatal(err)
	}
	if ins.Output["name"] != "o" {
		t.Fatal(ins.Output)
	}
	meta, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.get", Input: map[string]any{"bucket": "b", "name": "o", "object": "o"}})
	if err != nil {
		t.Fatal(err)
	}
	if meta.Output["size"] != "10" {
		t.Fatalf("size %v", meta.Output["size"])
	}
	full, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.get", Input: map[string]any{"bucket": "b", "object": "o", "alt": "media"}})
	if err != nil {
		t.Fatal(err)
	}
	all, _ := io.ReadAll(full.Stream)
	_ = full.Stream.Close()
	if !bytes.Equal(all, body) {
		t.Fatalf("media %q", all)
	}
	hr := httptest.NewRequest(http.MethodGet, "/storage/v1/b/b/o/o?alt=media", nil)
	hr.Header.Set("Range", "bytes=0-3")
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.get", Input: map[string]any{"bucket": "b", "object": "o", "alt": "media"}, HTTP: hr})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != 206 {
		t.Fatalf("range status %d", got.Status)
	}
	b, _ := io.ReadAll(got.Stream)
	_ = got.Stream.Close()
	if string(b) != "abcd" {
		t.Fatalf("range %q", b)
	}
}

func TestListPrefixDelimiter(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.insert", Input: map[string]any{"name": "b"}})
	for _, n := range []string{"a/x", "a/y", "a/n/z", "z"} {
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": n}, Body: io.NopCloser(strings.NewReader("1"))})
		if err != nil {
			t.Fatal(err)
		}
	}
	list, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.list", Input: map[string]any{"bucket": "b", "prefix": "a/", "delimiter": "/"}})
	if err != nil {
		t.Fatal(err)
	}
	items, _ := list.Output["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items %v", list.Output)
	}
	found := false
	for _, pfx := range asAny(list.Output["prefixes"]) {
		if pfx == "a/n/" {
			found = true
		}
	}
	if !found {
		t.Fatalf("prefixes %v", list.Output["prefixes"])
	}
}

func asAny(v any) []any {
	s, _ := v.([]any)
	return s
}

func TestGenerationMatch(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.insert", Input: map[string]any{"name": "b"}})
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": "o"}, Body: io.NopCloser(strings.NewReader("1"))})
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.get", Input: map[string]any{"bucket": "b", "object": "o", "ifGenerationMatch": "nope"}})
	if err == nil {
		t.Fatal("expected 412")
	}
	if f := err.(*spi.Fault); f.HTTPStatus != 412 || f.Code != "conditionNotMet" {
		t.Fatalf("%v", f)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": "o", "ifGenerationMatch": "0"}, Body: io.NopCloser(strings.NewReader("2"))})
	if err == nil {
		t.Fatal("expected 412 on ifGenerationMatch=0")
	}
	if f := err.(*spi.Fault); f.HTTPStatus != 412 {
		t.Fatalf("%v", f)
	}
}

func TestResumable(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.insert", Input: map[string]any{"name": "b"}})
	sess, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": "o", "uploadType": "resumable"}})
	if err != nil {
		t.Fatal(err)
	}
	uid := sess.Output["upload_id"].(string)
	put := func(start, end, total int, chunk string) {
		hr := httptest.NewRequest(http.MethodPut, "/upload/storage/v1/b/b/o?upload_id="+uid, strings.NewReader(chunk))
		hr.Header.Set("Content-Range", "bytes "+strconv.Itoa(start)+"-"+strconv.Itoa(end)+"/"+strconv.Itoa(total))
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": "o", "upload_id": uid}, HTTP: hr, Body: io.NopCloser(strings.NewReader(chunk))})
		if err != nil {
			t.Fatal(err)
		}
	}
	put(0, 4, 10, "hello")
	put(5, 9, 10, "world")
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.get", Input: map[string]any{"bucket": "b", "object": "o", "alt": "media"}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(got.Stream)
	if string(b) != "helloworld" {
		t.Fatalf("got %q", b)
	}
}

func TestCopyComposeDelete(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.insert", Input: map[string]any{"name": "b"}})
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": "a"}, Body: io.NopCloser(strings.NewReader("AA"))})
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"bucket": "b", "name": "c"}, Body: io.NopCloser(strings.NewReader("CC"))})
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.copy", Input: map[string]any{"bucket": "b", "object": "a", "destinationBucket": "b", "destinationObject": "a2"}})
	if err != nil {
		t.Fatal(err)
	}
	rew, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.rewrite", Input: map[string]any{"bucket": "b", "object": "a", "destinationBucket": "b", "destinationObject": "a3"}})
	if err != nil {
		t.Fatal(err)
	}
	if rew.Output["done"] != true {
		t.Fatalf("rewrite %v", rew.Output)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.compose", Input: map[string]any{"bucket": "b", "object": "out", "sourceObjects": []any{map[string]any{"name": "a"}, map[string]any{"name": "c"}}}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.get", Input: map[string]any{"bucket": "b", "object": "out", "alt": "media"}})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(got.Stream)
	if string(b) != "AACC" {
		t.Fatalf("%q", b)
	}
	patched, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.patch", Input: map[string]any{"bucket": "b", "object": "out", "contentType": "text/plain"}})
	if err != nil {
		t.Fatal(err)
	}
	if patched.Output["contentType"] != "text/plain" || patched.Output["metageneration"] != "2" {
		t.Fatalf("patch %v", patched.Output)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.delete", Input: map[string]any{"name": "b", "bucket": "b"}})
	if err == nil {
		t.Fatal("expected 409 for non-empty bucket")
	}
	if f := err.(*spi.Fault); f.HTTPStatus != 409 {
		t.Fatalf("%v", f)
	}
	for _, n := range []string{"a", "a2", "a3", "c", "out"} {
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.delete", Input: map[string]any{"bucket": "b", "object": n}})
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.buckets.delete", Input: map[string]any{"name": "b", "bucket": "b"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "storage.objects.insert", Input: map[string]any{"_path": "/batch/storage/v1"}})
	if err == nil {
		t.Fatal("expected batch not implemented")
	}
	if f := err.(*spi.Fault); f.Code != "MirrorNotImplemented" || f.HTTPStatus != 501 {
		t.Fatalf("batch %v", f)
	}
}
