package blobs

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestContainerAndBlobLifecycle(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any, body []byte) *spi.Response {
		t.Helper()
		req := &spi.Request{Identity: id, Operation: op, Input: in}
		if body != nil {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}
		res, err := p.Invoke(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	created := inv("CreateContainer", map[string]any{"container": "ctr"}, nil)
	if created.Output["name"] != "ctr" {
		t.Fatalf("create %#v", created.Output)
	}
	got := inv("GetContainer", map[string]any{"container": "ctr"}, nil)
	if got.Output["name"] != "ctr" {
		t.Fatalf("get %#v", got.Output)
	}
	list := inv("ListContainers", nil, nil)
	if len(list.Output["_list"].([]any)) != 1 {
		t.Fatalf("list %#v", list.Output)
	}
	put := inv("PutBlob", map[string]any{"container": "ctr", "blob": "o"}, []byte("hello-azure"))
	if put.Output["name"] != "o" {
		t.Fatalf("put %#v", put.Output)
	}
	blob := inv("GetBlob", map[string]any{"container": "ctr", "blob": "o"}, nil)
	b, _ := io.ReadAll(blob.Stream)
	_ = blob.Stream.Close()
	if string(b) != "hello-azure" {
		t.Fatalf("get blob %q", b)
	}
	blobs := inv("ListBlobs", map[string]any{"container": "ctr"}, nil)
	if len(blobs.Output["_list"].([]any)) != 1 {
		t.Fatalf("blobs %#v", blobs.Output)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteBlob", Input: map[string]any{"container": "ctr", "blob": "o"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBlob", Input: map[string]any{"container": "ctr", "blob": "o"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "BlobNotFound" {
		t.Fatalf("missing blob %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteContainer", Input: map[string]any{"container": "ctr"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetContainer", Input: map[string]any{"container": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "ContainerNotFound" {
		t.Fatalf("missing container %#v", err)
	}
}

func TestCreateContainerRejectsEmptyAndDuplicate(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateContainer", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 {
		t.Fatalf("empty %#v", err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateContainer", Input: map[string]any{"container": "dup"}}); err != nil {
		t.Fatal(err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateContainer", Input: map[string]any{"container": "dup"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 409 || f.Code != "ContainerAlreadyExists" {
		t.Fatalf("duplicate %#v", err)
	}
}

func TestAccountsDoNotShareContainers(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	a := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	b := spi.Identity{Account: "222222222222", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: a, Operation: "CreateContainer", Input: map[string]any{"container": "shared"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: b, Operation: "GetContainer", Input: map[string]any{"container": "shared"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "ContainerNotFound" {
		t.Fatalf("cross-account %#v", err)
	}
}
