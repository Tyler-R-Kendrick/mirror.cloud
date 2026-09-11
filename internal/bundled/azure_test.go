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

func TestAzureServicePropertiesStatsAccountInfo(t *testing.T) {
	p := azurePack(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetServiceProperties", Input: map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Output["properties"] != "" && got.Output["properties"] != nil {
		t.Fatalf("default properties %#v", got.Output)
	}
	body := "<StorageServiceProperties><Cors /></StorageServiceProperties>"
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SetServiceProperties", Input: map[string]any{"body": body}}); err != nil {
		t.Fatal(err)
	}
	got, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetServiceProperties", Input: map[string]any{}})
	if err != nil || fmt.Sprint(got.Output["properties"]) != body {
		t.Fatalf("roundtrip %#v %v", got, err)
	}
	acct, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetAccountInfo", Input: map[string]any{}})
	if err != nil || acct.Output["account_kind"] != "StorageV2" || acct.Output["sku_name"] != "Standard_RAGRS" || acct.Output["hns"] != "false" {
		t.Fatalf("account %#v %v", acct, err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetServiceStats", Input: map[string]any{}})
	if f, ok := err.(*spi.Fault); !ok || f.Code != "InvalidQueryParameterValue" || f.HTTPStatus != 400 {
		t.Fatalf("stats primary %#v", err)
	}
	st, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetServiceStats", Input: map[string]any{"secondary": true}})
	if err != nil || st.Output["geo_status"] != "live" {
		t.Fatalf("stats secondary %#v %v", st, err)
	}
}

func TestAzureBlobMetadataPropertiesHead(t *testing.T) {
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
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "hello"})
	inv("SetBlobMetadata", map[string]any{"container": "ctr", "blob": "o", "metadata": map[string]any{"a": "b"}})
	md := inv("GetBlobMetadata", map[string]any{"container": "ctr", "blob": "o"})
	got, _ := md.Output["metadata"].(map[string]any)
	if fmt.Sprint(got["a"]) != "b" {
		t.Fatalf("metadata %#v", md.Output)
	}
	inv("SetBlobProperties", map[string]any{"container": "ctr", "blob": "o", "content_type": "text/plain", "cache_control": "no-cache"})
	props := inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "o"})
	if props.Output["content_type"] != "text/plain" || props.Output["cache_control"] != "no-cache" || props.Output["blob_type"] != "BlockBlob" || props.Output["content_length"] != "5" {
		t.Fatalf("properties %#v", props.Output)
	}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBlobProperties", Input: map[string]any{"container": "ctr", "blob": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "BlobNotFound" {
		t.Fatalf("missing properties %#v", err)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBlobMetadata", Input: map[string]any{"container": "ctr", "blob": "missing"}})
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 404 || f.Code != "BlobNotFound" {
		t.Fatalf("missing metadata %#v", err)
	}
}

func TestAzurePageBlob(t *testing.T) {
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
	fault := func(op string, in map[string]any, status int, code string) {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		f, ok := err.(*spi.Fault)
		if !ok || f.HTTPStatus != status || f.Code != code {
			t.Fatalf("%s: got %#v, want %d %s", op, err, status, code)
		}
	}
	pageIn := func(start, end int64, body string) map[string]any {
		return map[string]any{"container": "ctr", "blob": "p", "page_write": "update",
			"range_start": start, "range_end": end, "body": body}
	}

	inv("CreateContainer", map[string]any{"container": "ctr"})
	inv("CreatePageBlob", map[string]any{"container": "ctr", "blob": "p", "content_length": "1024"})
	props := inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "p"})
	if props.Output["blob_type"] != "PageBlob" || props.Output["content_length"] != "1024" || props.Output["sequence_number"] != "0" {
		t.Fatalf("page properties %#v", props.Output)
	}
	dl := inv("GetBlob", map[string]any{"container": "ctr", "blob": "p"})
	if raw := fmt.Sprint(dl.Output["_raw"]); len(raw) != 1024 || strings.Count(raw, "\x00") != 1024 {
		t.Fatalf("fresh page blob download %d bytes", len(raw))
	}

	inv("PutPage", pageIn(0, 511, strings.Repeat("a", 512)))
	inv("PutPage", pageIn(512, 1023, strings.Repeat("b", 512)))
	ranges := inv("GetPageRanges", map[string]any{"container": "ctr", "blob": "p"})
	got, _ := ranges.Output["ranges"].([]any)
	if len(got) != 1 || fmt.Sprint(got[0].(map[string]any)["start"]) != "0" || fmt.Sprint(got[0].(map[string]any)["end"]) != "1023" {
		t.Fatalf("merged ranges %#v", ranges.Output)
	}
	clipped := inv("GetPageRanges", map[string]any{"container": "ctr", "blob": "p", "range_start": int64(0), "range_end": int64(511)})
	got, _ = clipped.Output["ranges"].([]any)
	if len(got) != 1 || fmt.Sprint(got[0].(map[string]any)["end"]) != "511" {
		t.Fatalf("clipped ranges %#v", clipped.Output)
	}

	inv("ClearPages", map[string]any{"container": "ctr", "blob": "p", "range_start": int64(0), "range_end": int64(511)})
	dl = inv("GetBlob", map[string]any{"container": "ctr", "blob": "p"})
	raw := fmt.Sprint(dl.Output["_raw"])
	if len(raw) != 1024 || raw[:512] != strings.Repeat("\x00", 512) || raw[512:] != strings.Repeat("b", 512) {
		t.Fatalf("after clear %q", raw[:32])
	}

	inv("ResizePageBlob", map[string]any{"container": "ctr", "blob": "p", "content_length": "512"})
	props = inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "p"})
	if props.Output["content_length"] != "512" {
		t.Fatalf("shrunk %#v", props.Output)
	}
	inv("ResizePageBlob", map[string]any{"container": "ctr", "blob": "p", "content_length": "1536"})
	props = inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "p"})
	if props.Output["content_length"] != "1536" {
		t.Fatalf("grown %#v", props.Output)
	}

	if out := inv("SetBlobSequenceNumber", map[string]any{"container": "ctr", "blob": "p", "sequence_number_action": "increment"}); out.Output["sequence_number"] != "1" {
		t.Fatalf("increment %#v", out.Output)
	}
	if out := inv("SetBlobSequenceNumber", map[string]any{"container": "ctr", "blob": "p", "sequence_number_action": "update", "sequence_number": "10"}); out.Output["sequence_number"] != "10" {
		t.Fatalf("update %#v", out.Output)
	}
	if out := inv("SetBlobSequenceNumber", map[string]any{"container": "ctr", "blob": "p", "sequence_number_action": "max", "sequence_number": "5"}); out.Output["sequence_number"] != "10" {
		t.Fatalf("max %#v", out.Output)
	}

	fault("CreatePageBlob", map[string]any{"container": "ctr", "blob": "p", "content_length": "512"}, 409, "BlobAlreadyExists")
	fault("CreatePageBlob", map[string]any{"container": "ctr", "blob": "bad", "content_length": "100"}, 400, "InvalidHeaderValue")
	fault("PutPage", map[string]any{"container": "ctr", "blob": "missing", "page_write": "update", "range_start": int64(0), "range_end": int64(511), "body": strings.Repeat("a", 512)}, 404, "BlobNotFound")
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "x"})
	fault("PutPage", map[string]any{"container": "ctr", "blob": "o", "page_write": "update", "range_start": int64(0), "range_end": int64(511), "body": strings.Repeat("a", 512)}, 409, "InvalidBlobType")
	fault("PutPage", map[string]any{"container": "ctr", "blob": "p", "page_write": "update", "range_start": int64(1), "range_end": int64(512), "body": strings.Repeat("a", 512)}, 400, "InvalidHeaderValue")
	fault("PutPage", map[string]any{"container": "ctr", "blob": "p", "page_write": "update", "range_start": int64(1536), "range_end": int64(2047), "body": strings.Repeat("a", 512)}, 416, "RequestedRangeNotSatisfiable")
	fault("GetPageRanges", map[string]any{"container": "ctr", "blob": "o"}, 409, "InvalidBlobType")
}

func TestAzureAppendBlob(t *testing.T) {
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
	fault := func(op string, in map[string]any, status int, code string) {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		f, ok := err.(*spi.Fault)
		if !ok || f.HTTPStatus != status || f.Code != code {
			t.Fatalf("%s: got %#v, want %d %s", op, err, status, code)
		}
	}

	inv("CreateContainer", map[string]any{"container": "ctr"})
	inv("CreateAppendBlob", map[string]any{"container": "ctr", "blob": "log"})
	props := inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "log"})
	if props.Output["blob_type"] != "AppendBlob" || props.Output["content_length"] != "0" {
		t.Fatalf("append properties %#v", props.Output)
	}
	app := inv("AppendBlock", map[string]any{"container": "ctr", "blob": "log", "body": "one"})
	if app.Output["append_offset"] != "0" || app.Output["committed_block_count"] != "1" {
		t.Fatalf("append %#v", app.Output)
	}
	app = inv("AppendBlock", map[string]any{"container": "ctr", "blob": "log", "body": "two"})
	if app.Output["append_offset"] != "3" || app.Output["committed_block_count"] != "2" {
		t.Fatalf("append 2 %#v", app.Output)
	}
	dl := inv("GetBlob", map[string]any{"container": "ctr", "blob": "log"})
	if fmt.Sprint(dl.Output["_raw"]) != "onetwo" {
		t.Fatalf("download %#v", dl.Output)
	}

	// Azurite: create-append over an existing page blob replaces it.
	inv("CreatePageBlob", map[string]any{"container": "ctr", "blob": "pg", "content_length": "512"})
	inv("CreateAppendBlob", map[string]any{"container": "ctr", "blob": "pg"})
	props = inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "pg"})
	if props.Output["blob_type"] != "AppendBlob" || props.Output["content_length"] != "0" {
		t.Fatalf("override %#v", props.Output)
	}

	fault("AppendBlock", map[string]any{"container": "ctr", "blob": "missing", "body": "x"}, 404, "BlobNotFound")
	fault("AppendBlock", map[string]any{"container": "ctr", "blob": "log", "body": ""}, 400, "InvalidHeaderValue")
	fault("AppendBlock", map[string]any{"container": "ctr", "blob": "log", "body": strings.Repeat("a", 4194305)}, 413, "RequestBodyTooLarge")
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "x"})
	fault("AppendBlock", map[string]any{"container": "ctr", "blob": "o", "body": "x"}, 409, "InvalidBlobType")
}

func TestAzureSnapshotCopy(t *testing.T) {
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
	fault := func(op string, in map[string]any, status int, code string) {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		f, ok := err.(*spi.Fault)
		if !ok || f.HTTPStatus != status || f.Code != code {
			t.Fatalf("%s: got %#v, want %d %s", op, err, status, code)
		}
	}

	inv("CreateContainer", map[string]any{"container": "ctr"})
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "hello-azure", "content_md5": "b64==", "metadata": map[string]any{"a": "b"}})
	snap := inv("CreateSnapshot", map[string]any{"container": "ctr", "blob": "o"})
	sid := fmt.Sprint(snap.Output["snapshot"])
	if sid == "" {
		t.Fatalf("snapshot id %#v", snap.Output)
	}
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "changed"})
	dl := inv("GetBlob", map[string]any{"container": "ctr", "blob": "o", "snapshot": sid})
	if fmt.Sprint(dl.Output["_raw"]) != "hello-azure" {
		t.Fatalf("snapshot download %#v", dl.Output)
	}
	props := inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "o", "snapshot": sid})
	if props.Output["content_length"] != "11" || props.Output["content_md5"] != "b64==" {
		t.Fatalf("snapshot properties %#v", props.Output)
	}
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o"}, 409, "SnapshotsPresent")
	inv("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "snapshot": sid})
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "o", "snapshot": sid}, 404, "BlobNotFound")
	inv("DeleteBlob", map[string]any{"container": "ctr", "blob": "o"})

	// x-ms-delete-snapshots: only drops snapshots, keeps the base.
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "changed", "content_md5": "b64==", "metadata": map[string]any{"a": "b"}})
	inv("CreateSnapshot", map[string]any{"container": "ctr", "blob": "o"})
	inv("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "delete_snapshots": "only"})
	dl = inv("GetBlob", map[string]any{"container": "ctr", "blob": "o"})
	if fmt.Sprint(dl.Output["_raw"]) != "changed" {
		t.Fatalf("base after delete-snapshots-only %#v", dl.Output)
	}

	src := "http://acct.blob.core.windows.net/ctr/o"
	cp := inv("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "c2", "copy_source": src, "source_container": "ctr", "source_blob": "o"})
	if cp.Output["copy_status"] != "success" || fmt.Sprint(cp.Output["copy_id"]) == "" {
		t.Fatalf("copy %#v", cp.Output)
	}
	dl = inv("GetBlob", map[string]any{"container": "ctr", "blob": "c2"})
	if fmt.Sprint(dl.Output["_raw"]) != "changed" {
		t.Fatalf("copy download %#v", dl.Output)
	}
	props = inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "c2"})
	if md, _ := props.Output["metadata"].(map[string]any); fmt.Sprint(md["a"]) != "b" {
		t.Fatalf("copy inherited metadata %#v", props.Output)
	}
	inv("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "c3", "copy_source": src, "source_container": "ctr", "source_blob": "o", "metadata": map[string]any{"x": "y"}})
	props = inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "c3"})
	if md, _ := props.Output["metadata"].(map[string]any); fmt.Sprint(md["x"]) != "y" || len(md) != 1 {
		t.Fatalf("copy override metadata %#v", props.Output)
	}

	// Synchronized copy echoes source Content-MD5 when the source has one.
	sync := inv("CopyBlobFromURL", map[string]any{"container": "ctr", "blob": "c4", "copy_source": src, "source_container": "ctr", "source_blob": "o"})
	if sync.Output["content_md5"] != "b64==" {
		t.Fatalf("sync copy md5 %#v", sync.Output)
	}

	// Copy from a snapshot source.
	snap = inv("CreateSnapshot", map[string]any{"container": "ctr", "blob": "o"})
	sid = fmt.Sprint(snap.Output["snapshot"])
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "newer"})
	inv("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "c5", "copy_source": src + "?snapshot=" + sid, "source_container": "ctr", "source_blob": "o", "source_snapshot": sid})
	dl = inv("GetBlob", map[string]any{"container": "ctr", "blob": "c5"})
	if fmt.Sprint(dl.Output["_raw"]) != "changed" {
		t.Fatalf("snapshot copy %#v", dl.Output)
	}

	// Page blob copy preserves type and sequence number.
	inv("CreatePageBlob", map[string]any{"container": "ctr", "blob": "pg", "content_length": "512", "sequence_number": "7"})
	inv("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "pg2", "copy_source": "http://acct.blob.core.windows.net/ctr/pg", "source_container": "ctr", "source_blob": "pg"})
	props = inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "pg2"})
	if props.Output["blob_type"] != "PageBlob" || props.Output["sequence_number"] != "7" || props.Output["content_length"] != "512" {
		t.Fatalf("page copy %#v", props.Output)
	}

	fault("AbortCopy", map[string]any{"container": "ctr", "blob": "c2", "copy_id": "nope", "copy_action": "abort"}, 409, "NoPendingCopyOperation")
	fault("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "c6", "copy_source": "/devstoreaccount1/ctr/o", "source_invalid": true}, 400, "InvalidHeaderValue")
	fault("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "c7", "copy_source": "http://acct.blob.core.windows.net/ctr/missing", "source_container": "ctr", "source_blob": "missing"}, 404, "BlobNotFound")

	inv("StageBlockFromURL", map[string]any{"container": "ctr", "blob": "sb", "blockid": "YQ==", "copy_source": src, "source_container": "ctr", "source_blob": "o", "source_range_start": int64(0), "source_range_end": int64(4)})
	inv("PutBlockList", map[string]any{"container": "ctr", "blob": "sb", "blockids": []any{"YQ=="}})
	dl = inv("GetBlob", map[string]any{"container": "ctr", "blob": "sb"})
	if fmt.Sprint(dl.Output["_raw"]) != "newer"[:5] {
		t.Fatalf("staged from url %#v", dl.Output)
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

func TestAzureConditions(t *testing.T) {
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
	fault := func(op string, in map[string]any, status int, code string) {
		t.Helper()
		_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: op, Input: in})
		f, ok := err.(*spi.Fault)
		if !ok || f.HTTPStatus != status || f.Code != code {
			t.Fatalf("%s: got %#v, want %d %s", op, err, status, code)
		}
	}
	etagOf := func(blob string) string {
		t.Helper()
		props := inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": blob})
		q := fmt.Sprint(props.Output["etag"])
		if !strings.HasPrefix(q, "\"") || !strings.HasSuffix(q, "\"") || props.Output["last_modified"] != "0" {
			t.Fatalf("properties %#v", props.Output)
		}
		return strings.Trim(q, "\"")
	}

	inv("CreateContainer", map[string]any{"container": "ctr"})
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "hello"})
	etag := etagOf("o")

	// Read conditions (download and HEAD share the validator).
	inv("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{etag}})
	inv("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{"*"}})
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{"bogus"}}, 412, "ConditionNotMet")
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{etag}}, 304, "ConditionNotMet")
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{"*"}}, 400, "UnsatisfiableCondition")
	// Azurite validates read conditions before the 404.
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "missing", "if_match_list": []any{"x"}}, 412, "ConditionNotMet")
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "missing", "if_none_match_list": []any{"*"}}, 400, "UnsatisfiableCondition")
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "missing"}, 404, "BlobNotFound")
	// The atomic clock is frozen at epoch, so last_modified == 0 and the
	// boundary rows (same-instant) are what these values exercise.
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_modified_since_unix": int64(0)}, 304, "ConditionNotMet")
	inv("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_modified_since_unix": int64(-1)})
	fault("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_unmodified_since_unix": int64(-1)}, 412, "ConditionNotMet")
	inv("GetBlobProperties", map[string]any{"container": "ctr", "blob": "o", "if_unmodified_since_unix": int64(0)})
	// Azurite read precedence: a passing if-modified-since overrides an
	// if-none-match hit, and a passing if-none-match overrides an
	// if-modified-since miss.
	inv("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{etag}, "if_modified_since_unix": int64(-1)})
	inv("GetBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{"bogus"}, "if_modified_since_unix": int64(0)})

	// The etag tracks content.
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "changed"})
	if etagOf("o") == etag {
		t.Fatalf("etag did not change after overwrite")
	}
	etag = etagOf("o")

	// Write conditions on delete.
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{"bogus"}}, 412, "ConditionNotMet")
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{etag}}, 412, "ConditionNotMet")
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{"a", "b"}}, 400, "MultipleConditionHeadersNotSupported")
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{"a", "b"}}, 400, "MultipleConditionHeadersNotSupported")
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{etag}, "if_none_match_list": []any{"x"}}, 400, "MultipleConditionHeadersNotSupported")
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{etag}, "if_modified_since_unix": int64(-1), "if_unmodified_since_unix": int64(1)}, 400, "MultipleConditionHeadersNotSupported")
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_modified_since_unix": int64(1)}, 412, "ConditionNotMet")
	fault("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_unmodified_since_unix": int64(-1)}, 412, "ConditionNotMet")
	// The one allowed pair is if-none-match + if-modified-since.
	inv("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{"bogus"}, "if_modified_since_unix": int64(-1)})

	// if-none-match * passes a write, if-match * passes, if-match etag passes.
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "v1"})
	inv("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_none_match_list": []any{"*"}})
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "v1"})
	inv("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{"*"}})
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o", "body": "v1"})
	inv("DeleteBlob", map[string]any{"container": "ctr", "blob": "o", "if_match_list": []any{etagOf("o")}})

	// PutBlob with if-none-match * on an existing blob is a 409.
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o2", "body": "x"})
	fault("PutBlob", map[string]any{"container": "ctr", "blob": "o2", "body": "y", "if_none_match_list": []any{"*"}}, 409, "BlobAlreadyExists")
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "o3", "body": "y", "if_none_match_list": []any{"*"}})

	// Page blob sequence-number conditions.
	inv("CreatePageBlob", map[string]any{"container": "ctr", "blob": "p", "content_length": "512"})
	inv("SetBlobSequenceNumber", map[string]any{"container": "ctr", "blob": "p", "sequence_number_action": "update", "sequence_number": "5"})
	pg := func(extra map[string]any) map[string]any {
		in := map[string]any{"container": "ctr", "blob": "p", "page_write": "update",
			"range_start": int64(0), "range_end": int64(511), "body": strings.Repeat("a", 512)}
		for k, v := range extra {
			in[k] = v
		}
		return in
	}
	inv("PutPage", pg(map[string]any{"seq_eq": "5"}))
	fault("PutPage", pg(map[string]any{"seq_eq": "4"}), 412, "SequenceNumberConditionNotMet")
	inv("PutPage", pg(map[string]any{"seq_lt": "6"}))
	fault("PutPage", pg(map[string]any{"seq_lt": "5"}), 412, "SequenceNumberConditionNotMet")
	inv("PutPage", pg(map[string]any{"seq_le": "5"}))
	fault("PutPage", pg(map[string]any{"seq_le": "4"}), 412, "SequenceNumberConditionNotMet")
	clr := func(extra map[string]any) map[string]any {
		in := map[string]any{"container": "ctr", "blob": "p", "range_start": int64(0), "range_end": int64(511)}
		for k, v := range extra {
			in[k] = v
		}
		return in
	}
	inv("ClearPages", clr(map[string]any{"seq_eq": "5"}))
	fault("ClearPages", clr(map[string]any{"seq_eq": "9"}), 412, "SequenceNumberConditionNotMet")

	// Append conditions: max size is checked before append position.
	inv("CreateAppendBlob", map[string]any{"container": "ctr", "blob": "a"})
	inv("AppendBlock", map[string]any{"container": "ctr", "blob": "a", "body": "x", "max_size": "1"})
	fault("AppendBlock", map[string]any{"container": "ctr", "blob": "a", "body": "y", "max_size": "1"}, 412, "MaxBlobSizeConditionNotMet")
	inv("AppendBlock", map[string]any{"container": "ctr", "blob": "a", "body": "y", "append_pos": "1"})
	fault("AppendBlock", map[string]any{"container": "ctr", "blob": "a", "body": "z", "append_pos": "0"}, 412, "AppendPositionConditionNotMet")
	fault("AppendBlock", map[string]any{"container": "ctr", "blob": "a", "body": "z", "if_match_list": []any{"bogus"}}, 412, "ConditionNotMet")

	// Copy with if-none-match * fails only when the destination exists.
	inv("PutBlob", map[string]any{"container": "ctr", "blob": "src", "body": "s"})
	fault("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "o2", "copy_source": "http://acct.blob.core.windows.net/ctr/src", "source_container": "ctr", "source_blob": "src", "if_none_match_list": []any{"*"}}, 409, "BlobAlreadyExists")
	inv("StartCopyFromURL", map[string]any{"container": "ctr", "blob": "c9", "copy_source": "http://acct.blob.core.windows.net/ctr/src", "source_container": "ctr", "source_blob": "src", "if_none_match_list": []any{"*"}})
	fault("CopyBlobFromURL", map[string]any{"container": "ctr", "blob": "o2", "copy_source": "http://acct.blob.core.windows.net/ctr/src", "source_container": "ctr", "source_blob": "src", "if_none_match_list": []any{"*"}}, 409, "BlobAlreadyExists")
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
