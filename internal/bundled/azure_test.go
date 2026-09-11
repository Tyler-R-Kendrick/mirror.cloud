package bundled_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func azurePack(t testing.TB) spi.BehaviorPack {
	t.Helper()
	p, err := bundled.New("azure.blobs", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAzureBundleBehaves(t *testing.T) {
	p := azurePack(t)
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
	if blob.Output["_raw"] != "hello-azure" {
		t.Fatalf("get blob %#v", blob.Output)
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

func TestAzureDeleteMissingContainerAndBlob(t *testing.T) {
	p := azurePack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateContainer", Input: map[string]any{"container": "ctr"}}); err != nil {
		t.Fatal(err)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteBlob", Input: map[string]any{"container": "ctr", "blob": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "BlobNotFound" {
		t.Fatalf("delete missing blob %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteContainer", Input: map[string]any{"container": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "ContainerNotFound" {
		t.Fatalf("delete missing container %#v", err)
	}
}

func TestAzureCreateContainerRejectsEmptyAndDuplicate(t *testing.T) {
	p := azurePack(t)
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

func TestAzureBlobCharacterization(t *testing.T) {
	p := azurePack(t)
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
		if op == "GetBlob" {
			return map[string]any{"status": res.Status, "body": res.Output["_raw"]}
		}
		return map[string]any{"status": res.Status, "output": res.Output}
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

func TestAzureContainerMetadataAclLease(t *testing.T) {
	p := azurePack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res
	}
	inv("CreateContainer", map[string]any{"container": "ctr"})
	meta := inv("SetContainerMetadata", map[string]any{"container": "ctr", "metadata": map[string]any{"keya": "vala"}})
	if fmt.Sprint(meta.Output["metadata"]) == "" {
		t.Fatalf("set metadata %#v", meta.Output)
	}
	got := inv("GetContainerMetadata", map[string]any{"container": "ctr"})
	md, _ := got.Output["metadata"].(map[string]any)
	if fmt.Sprint(md["keya"]) != "vala" {
		t.Fatalf("get metadata %#v", got.Output)
	}
	inv("SetContainerAcl", map[string]any{"container": "ctr", "acl": "<SignedIdentifiers><SignedIdentifier><Id>p1</Id></SignedIdentifier></SignedIdentifiers>", "public_access": "blob"})
	acl := inv("GetContainerAcl", map[string]any{"container": "ctr"})
	if !strings.Contains(fmt.Sprint(acl.Output["acl"]), "p1") || fmt.Sprint(acl.Output["public_access"]) != "blob" {
		t.Fatalf("acl %#v", acl.Output)
	}
	acq := inv("AcquireContainerLease", map[string]any{"container": "ctr", "proposed_lease_id": "ca761232ed4211cebacd00aa0057b223", "lease_duration": "-1"})
	if acq.Output["lease_id"] != "ca761232ed4211cebacd00aa0057b223" || acq.Output["lease_status"] != "locked" {
		t.Fatalf("acquire %#v", acq.Output)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "AcquireContainerLease", Input: map[string]any{"container": "ctr"}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "LeaseAlreadyPresent" {
		t.Fatalf("duplicate lease %#v", err)
	}
	chg := inv("ChangeContainerLease", map[string]any{"container": "ctr", "lease_id": "ca761232ed4211cebacd00aa0057b223", "proposed_lease_id": "3c7e72ebb4304526bc53d8ecef03798f"})
	if chg.Output["lease_id"] != "3c7e72ebb4304526bc53d8ecef03798f" {
		t.Fatalf("change %#v", chg.Output)
	}
	inv("RenewContainerLease", map[string]any{"container": "ctr", "lease_id": "3c7e72ebb4304526bc53d8ecef03798f"})
	inv("ReleaseContainerLease", map[string]any{"container": "ctr", "lease_id": "3c7e72ebb4304526bc53d8ecef03798f"})
	props := inv("GetContainer", map[string]any{"container": "ctr"})
	if props.Output["lease_status"] != "unlocked" {
		t.Fatalf("released %#v", props.Output)
	}
	inv("AcquireContainerLease", map[string]any{"container": "ctr", "proposed_lease_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "lease_duration": "30"})
	brk := inv("BreakContainerLease", map[string]any{"container": "ctr"})
	if brk.Output["lease_state"] != "broken" {
		t.Fatalf("break %#v", brk.Output)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetContainerMetadata", Input: map[string]any{"container": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "ContainerNotFound" {
		t.Fatalf("missing metadata %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetContainerAcl", Input: map[string]any{"container": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "ContainerNotFound" {
		t.Fatalf("missing acl %#v", err)
	}
}

func TestAzurePutBlockListFoldsInRequestOrder(t *testing.T) {
	p := azurePack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	inv := func(op string, in map[string]any) *spi.Response {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res
	}
	inv("CreateContainer", map[string]any{"container": "ctr"})
	inv("PutBlock", map[string]any{"container": "ctr", "blob": "part", "blockid": "YQ==", "body": "A"})
	inv("PutBlock", map[string]any{"container": "ctr", "blob": "part", "blockid": "Yg==", "body": "B"})
	inv("PutBlockList", map[string]any{"container": "ctr", "blob": "part", "blockids": []any{"Yg==", "YQ=="}})
	got := inv("GetBlob", map[string]any{"container": "ctr", "blob": "part"})
	if got.Output["_raw"] != "BA" {
		t.Fatalf("fold got %#v, want BA (request order, not store order)", got.Output["_raw"])
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBlockList", Input: map[string]any{
		"container": "ctr", "blob": "part", "blockids": []any{"nope"},
	}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "InvalidBlockList" || f.HTTPStatus != 400 {
		t.Fatalf("missing block %#v", err)
	}
}

func FuzzBlobBytes(f *testing.F) {
	f.Add("o", "hello")
	f.Add("", "v")
	f.Add("a/b", "x")
	f.Fuzz(func(t *testing.T, name, body string) {
		p := azurePack(t)
		ctx := context.Background()
		id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateContainer", Input: map[string]any{"container": "b"}})
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBlob", Input: map[string]any{"container": "b", "blob": name}, Body: io.NopCloser(bytes.NewReader([]byte(body)))})
		got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBlob", Input: map[string]any{"container": "b", "blob": name}})
		if err == nil && got != nil && got.Output["_raw"] != nil {
			_ = got.Output["_raw"]
		}
		_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteBlob", Input: map[string]any{"container": "b", "blob": name}})
	})
}
