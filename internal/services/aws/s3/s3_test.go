package s3_test

import (
	"bytes"
	"context"
	"io"
	"regexp"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func ident() spi.Identity {
	return spi.Identity{Account: "123456789012", Region: "us-east-1"}
}

func invoke(t *testing.T, p *s3.Pack, op string, in map[string]any, body []byte) (*spi.Response, error) {
	return invokeAs(t, p, ident(), op, in, body)
}

func invokeAs(t *testing.T, p *s3.Pack, id spi.Identity, op string, in map[string]any, body []byte) (*spi.Response, error) {
	t.Helper()
	var rc io.ReadCloser
	if body != nil {
		rc = io.NopCloser(bytes.NewReader(body))
	}
	if in == nil {
		in = map[string]any{}
	}
	return p.Invoke(context.Background(), &spi.Request{
		ServiceID: "aws.s3",
		Operation: op,
		Input:     in,
		Identity:  id,
		Body:      rc,
	})
}

func mustInvokeAs(t *testing.T, p *s3.Pack, id spi.Identity, op string, in map[string]any, body []byte) *spi.Response {
	t.Helper()
	resp, err := invokeAs(t, p, id, op, in, body)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp
}

func mustInvoke(t *testing.T, p *s3.Pack, op string, in map[string]any, body []byte) *spi.Response {
	t.Helper()
	resp, err := invoke(t, p, op, in, body)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp
}

func asFault(t *testing.T, err error) *spi.Fault {
	t.Helper()
	f, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("got %T %v, want *spi.Fault", err, err)
	}
	return f
}

func readStream(t *testing.T, resp *spi.Response) []byte {
	t.Helper()
	if resp.Stream == nil {
		t.Fatal("nil stream")
	}
	defer resp.Stream.Close()
	b, err := io.ReadAll(resp.Stream)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCreatePutGetBytesMatch(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	body := []byte("payload-bytes")
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "k"}, body)
	resp := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "k"}, nil)
	if got := readStream(t, resp); !bytes.Equal(got, body) {
		t.Fatalf("get bytes %q want %q", got, body)
	}
}

func TestCopyObjectTaggingDirective(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "source", "Tagging": "team=data"}, []byte("body"))
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "copied", "CopySource": "b/source"}, nil)
	copied := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "copied"}, nil)
	if tags := copied.Output["TagSet"].([]any); len(tags) != 1 || tags[0].(map[string]any)["Key"] != "team" {
		t.Fatalf("copied tags = %#v", tags)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "replaced", "CopySource": "b/source", "TaggingDirective": "REPLACE", "Tagging": "owner=mirror"}, nil)
	replaced := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "replaced"}, nil)
	if tags := replaced.Output["TagSet"].([]any); len(tags) != 1 || tags[0].(map[string]any)["Key"] != "owner" {
		t.Fatalf("replaced tags = %#v", tags)
	}
}

func TestListObjectsV2Prefix(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "a/1"}, []byte("1"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "a/2"}, []byte("2"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "z/9"}, []byte("9"))
	resp := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "b", "Prefix": "a/"}, nil)
	contents, _ := resp.Output["Contents"].([]any)
	keys := map[string]bool{}
	for _, item := range contents {
		m, _ := item.(map[string]any)
		keys[m["Key"].(string)] = true
	}
	if !keys["a/1"] || !keys["a/2"] || keys["z/9"] || len(keys) != 2 {
		t.Fatalf("prefix list: %v", keys)
	}
}

func TestMultipartETagForm(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "k"}, nil)
	id, _ := created.Output["UploadId"].(string)
	if id == "" {
		t.Fatal("missing UploadId")
	}
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, []byte("AAA"))
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, []byte("BBB"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", map[string]any{"UploadId": id}, nil)
	etag, _ := done.Output["ETag"].(string)
	if !regexp.MustCompile(`^"[0-9a-f]{32}-2"$`).MatchString(etag) {
		t.Fatalf("multipart etag form: %q", etag)
	}
	got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "k"}, nil))
	if string(got) != "AAABBB" {
		t.Fatalf("assembled %q", got)
	}
}

func TestListPartsAndMultipartUploads(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "k"}, nil)
	id, _ := created.Output["UploadId"].(string)
	mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "k", "UploadId": id, "PartNumber": 1}, []byte("AAA"))
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "k", "UploadId": id}, nil)
	parts, _ := listed.Output["Parts"].([]any)
	if len(parts) != 1 {
		t.Fatalf("ListParts %v", listed.Output)
	}
	ups := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "b"}, nil)
	uploads, _ := ups.Output["Uploads"].([]any)
	if len(uploads) != 1 {
		t.Fatalf("ListMultipartUploads %v", ups.Output)
	}
	mustInvoke(t, p, "CompleteMultipartUpload", map[string]any{"Bucket": "b", "Key": "k", "UploadId": id}, nil)
	after := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "b"}, nil)
	uploads, _ = after.Output["Uploads"].([]any)
	if len(uploads) != 0 {
		t.Fatalf("completed upload still listed: %v", after.Output)
	}
}

func TestMissingBucket404(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "nope", "Key": "k"}, []byte("x"))
	f := asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("put missing bucket: %+v", f)
	}
	_, err = invoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "nope"}, nil)
	f = asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("list missing bucket: %+v", f)
	}
	_, err = invoke(t, p, "HeadBucket", map[string]any{"Bucket": "nope"}, nil)
	f = asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("head missing bucket: %+v", f)
	}
}

func TestReplicationFiltersStatusMetadataAndDeleteMarker(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	west := ident()
	west.Region = "us-west-2"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "source"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "source", "Status": "Enabled"}, nil)
	mustInvokeAs(t, p, west, "CreateBucket", map[string]any{"Bucket": "destination"}, nil)
	mustInvokeAs(t, p, west, "PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutBucketReplication", map[string]any{
		"Bucket": "source",
		"ReplicationConfiguration": map[string]any{"Rules": []any{map[string]any{
			"Status": "Enabled",
			"Filter": map[string]any{"And": map[string]any{
				"Prefix": "logs/",
				"Tags":   []any{map[string]any{"Key": "environment", "Value": "test"}},
			}},
			"DeleteMarkerReplication": map[string]any{"Status": "Enabled"},
			"Destination":             map[string]any{"Bucket": "arn:aws:s3:::destination", "StorageClass": "STANDARD_IA"},
		}}},
	}, nil)

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "other/file", "Tagging": "environment=test"}, []byte("skip"))
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "other/file"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("unmatched object was replicated: %v", err)
	}

	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "logs/file", "Tagging": "environment=test"}, []byte("replicated"))
	if got := put.Headers.Get("x-amz-replication-status"); got != "COMPLETED" {
		t.Fatalf("source replication status %q", got)
	}
	dst := mustInvokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	if got := string(readStream(t, dst)); got != "replicated" {
		t.Fatalf("replica body %q", got)
	}
	if got := dst.Headers.Get("x-amz-replication-status"); got != "REPLICA" {
		t.Fatalf("destination replication status %q", got)
	}
	if got := dst.Headers.Get("x-amz-storage-class"); got != "STANDARD_IA" {
		t.Fatalf("destination storage class %q", got)
	}

	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "source", "Key": "logs/file", "TagSet": []any{
		map[string]any{"Key": "environment", "Value": "test"},
		map[string]any{"Key": "owner", "Value": "mirror"},
	}}, nil)
	tags := mustInvokeAs(t, p, west, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	gotTags := tags.Output["TagSet"].([]any)
	if len(gotTags) != 2 || gotTags[1].(map[string]any)["Value"] != "mirror" {
		t.Fatalf("replica tags %v", gotTags)
	}
	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "source", "Key": "logs/file", "LegalHold": map[string]any{"Status": "ON"}}, nil)
	legalHold := mustInvokeAs(t, p, west, "GetObjectLegalHold", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	if got := asMapForTest(legalHold.Output["LegalHold"])["Status"]; got != "ON" {
		t.Fatalf("replica legal hold %v", legalHold.Output)
	}

	del := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "source", "Key": "logs/file"}, nil)
	if got := del.Headers.Get("x-amz-replication-status"); got != "COMPLETED" {
		t.Fatalf("delete-marker replication status %q", got)
	}
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("replica delete marker not visible: %v", err)
	}

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "logs/batch", "Tagging": "environment=test"}, []byte("batch"))
	deleted := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "source", "Objects": []any{map[string]any{"Key": "logs/batch"}}}, nil)
	if got := deleted.Output["Deleted"].([]any)[0].(map[string]any)["DeleteMarker"]; got != true {
		t.Fatalf("batch delete marker %v", deleted.Output)
	}
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/batch"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("batch replica delete marker not visible: %v", err)
	}
}

func asMapForTest(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}
