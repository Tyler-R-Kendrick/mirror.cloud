package s3_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"testing"
	"time"

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

func completedPart(number int, response *spi.Response) any {
	return map[string]any{"PartNumber": number, "ETag": response.Headers.Get("ETag")}
}

func completeInput(uploadID string, parts ...any) map[string]any {
	return map[string]any{"UploadId": uploadID, "MultipartUpload": map[string]any{"Parts": parts}}
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

func TestCopyObjectConditions(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	source := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "source"}, []byte("source"))
	etag := source.Headers.Get("ETag")
	copyObject := func(key string, input map[string]any, headers map[string]string) (*spi.Response, error) {
		t.Helper()
		in := map[string]any{"Bucket": "b", "Key": key, "CopySource": "b/source"}
		for name, value := range input {
			in[name] = value
		}
		httpReq := httptest.NewRequest(http.MethodPut, "/b/"+key, nil)
		httpReq.Header.Set("x-amz-copy-source", "b/source")
		for name, value := range headers {
			httpReq.Header.Set(name, value)
		}
		return p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Input: in, Identity: ident(), HTTP: httpReq})
	}
	wantPrecondition := func(err error) {
		t.Helper()
		fault := asFault(t, err)
		if fault.Code != "PreconditionFailed" || fault.HTTPStatus != http.StatusPreconditionFailed {
			t.Fatalf("fault = %#v", fault)
		}
	}

	_, err := copyObject("wrong-etag", nil, map[string]string{"x-amz-copy-source-if-match": `"wrong"`})
	wantPrecondition(err)
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "wrong-etag"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("failed copy wrote destination")
	}

	before := time.Unix(-1, 0).UTC().Format(http.TimeFormat)
	after := time.Unix(1, 0).UTC().Format(http.TimeFormat)
	if _, err := copyObject("matched", nil, map[string]string{
		"x-amz-copy-source-if-match":            etag,
		"x-amz-copy-source-if-unmodified-since": before,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = copyObject("none-matched", nil, map[string]string{
		"x-amz-copy-source-if-none-match":     etag,
		"x-amz-copy-source-if-modified-since": before,
	})
	wantPrecondition(err)

	for _, test := range []struct {
		name, header, value string
		ok                  bool
	}{
		{"modified", "x-amz-copy-source-if-modified-since", before, true},
		{"not-modified", "x-amz-copy-source-if-modified-since", after, false},
		{"unmodified", "x-amz-copy-source-if-unmodified-since", after, true},
		{"changed", "x-amz-copy-source-if-unmodified-since", before, false},
	} {
		_, err := copyObject(test.name, nil, map[string]string{test.header: test.value})
		if test.ok && err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		if !test.ok {
			wantPrecondition(err)
		}
	}

	destination := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "destination"}, []byte("old"))
	_, err = copyObject("destination", map[string]any{"IfNoneMatch": "*"}, nil)
	wantPrecondition(err)
	_, err = copyObject("destination", map[string]any{"IfMatch": `"wrong"`}, nil)
	wantPrecondition(err)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "destination"}, nil)); string(got) != "old" {
		t.Fatalf("failed condition replaced destination with %q", got)
	}
	if _, err := copyObject("destination", map[string]any{"IfMatch": destination.Headers.Get("ETag")}, nil); err != nil {
		t.Fatal(err)
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "destination"}, nil)); string(got) != "source" {
		t.Fatalf("conditional copy = %q", got)
	}
}

func TestCopyObjectSourceVersions(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "b", "Status": "Enabled"}, nil)
	key := "reports/a b+c?.json"
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": key}, []byte("first"))
	firstVersion := first.Headers.Get("x-amz-version-id")
	_ = deps.Clock.Advance(time.Second)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": key}, []byte("second"))
	source := "b/" + url.PathEscape(key)

	copyVersion := mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "b", "Key": "version-copy", "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	if got := copyVersion.Headers.Get("x-amz-copy-source-version-id"); got != firstVersion {
		t.Fatalf("source version header = %q want %q", got, firstVersion)
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "version-copy"}, nil)); string(got) != "first" {
		t.Fatalf("version copy = %q", got)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "current-copy", "CopySource": source}, nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "current-copy"}, nil)); string(got) != "second" {
		t.Fatalf("current copy = %q", got)
	}

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "part-copy"}, nil)
	uploadID := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": uploadID, "PartNumber": 1, "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "part-copy"}, nil)); string(got) != "first" {
		t.Fatalf("version part copy = %q", got)
	}

	deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "b", "Key": key}, nil)
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "deleted", "CopySource": source}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("copied current delete marker")
	}
	mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "b", "Key": "restored", "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	for _, invalid := range []struct {
		source, code string
	}{
		{"b/bad%zz", "InvalidArgument"},
		{source + "?versionId=missing", "NoSuchKey"},
		{source + "?versionId=", "InvalidArgument"},
		{source + "?versionId=" + deleted.Headers.Get("x-amz-version-id"), "InvalidRequest"},
	} {
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "invalid", "CopySource": invalid.source}, nil)
		if fault := asFault(t, err); fault.Code != invalid.code {
			t.Fatalf("%q fault = %#v", invalid.source, fault)
		}
	}
}

func TestUploadPartCopyConditionsAndRange(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	body := bytes.Repeat([]byte("0123456789"), 600000)
	source := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "large"}, body)
	createUpload := func(key string) string {
		t.Helper()
		response := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": key}, nil)
		return response.Output["UploadId"].(string)
	}

	_, err := invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("rejected"), "PartNumber": 1, "CopySource": "b/large", "CopySourceIfMatch": `"wrong"`,
	}, nil)
	if fault := asFault(t, err); fault.Code != "PreconditionFailed" {
		t.Fatalf("condition fault = %#v", fault)
	}

	uploadID := createUpload("range")
	part := mustInvoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": uploadID, "PartNumber": 1, "CopySource": "b/large",
		"CopySourceIfMatch": source.Headers.Get("ETag"), "CopySourceRange": "bytes=10-19",
	}, nil)
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "range"}, nil)); string(got) != "0123456789" {
		t.Fatalf("range copy = %q", got)
	}

	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("invalid-range"), "PartNumber": 1, "CopySource": "b/large", "CopySourceRange": "bytes=7000000-7000001",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRange" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("range fault = %#v", fault)
	}
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("malformed-range"), "PartNumber": 1, "CopySource": "b/large", "CopySourceRange": "0-1",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("malformed range fault = %#v", fault)
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "small"}, []byte("small"))
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("too-small"), "PartNumber": 1, "CopySource": "b/small", "CopySourceRange": "bytes=0-1",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("small range fault = %#v", fault)
	}
}

func TestListObjectsV2Prefix(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "a/1", "StorageClass": "STANDARD_IA"}, []byte("1"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "a/2"}, []byte("2"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "z/9"}, []byte("9"))
	resp := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "b", "Prefix": "a/"}, nil)
	contents, _ := resp.Output["Contents"].([]any)
	keys := map[string]bool{}
	for _, item := range contents {
		m, _ := item.(map[string]any)
		keys[m["Key"].(string)] = true
		if m["LastModified"] == "" || m["StorageClass"] == "" || m["Key"] == "a/1" && m["StorageClass"] != "STANDARD_IA" {
			t.Fatalf("object metadata: %#v", m)
		}
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
	firstBody := bytes.Repeat([]byte("A"), 5<<20)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, firstBody)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, []byte("BBB"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, first), completedPart(2, second)), nil)
	etag, _ := done.Output["ETag"].(string)
	if !regexp.MustCompile(`^"[0-9a-f]{32}-2"$`).MatchString(etag) {
		t.Fatalf("multipart etag form: %q", etag)
	}
	got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "k"}, nil))
	if len(got) != len(firstBody)+3 || !bytes.Equal(got[:len(firstBody)], firstBody) || string(got[len(firstBody):]) != "BBB" {
		t.Fatalf("assembled %d bytes", len(got))
	}
}

func TestCompleteMultipartUploadManifest(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	create := func(key string) string {
		t.Helper()
		return mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": key}, nil).Output["UploadId"].(string)
	}
	wantFault := func(uploadID, code string, parts ...any) {
		t.Helper()
		_, err := invoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, parts...), nil)
		if fault := asFault(t, err); fault.Code != code || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("complete fault = %#v want %s", fault, code)
		}
	}

	noncontiguous := create("noncontiguous")
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": noncontiguous, "PartNumber": 1}, []byte("omitted"))
	third := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": noncontiguous, "PartNumber": 3}, []byte("third"))
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(noncontiguous, completedPart(3, third)), nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "noncontiguous"}, nil)); string(got) != "third" {
		t.Fatalf("noncontiguous completion = %q", got)
	}

	wrongETag := create("wrong-etag")
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": wrongETag, "PartNumber": 1}, []byte("one"))
	wantFault(wrongETag, "InvalidPart", map[string]any{"PartNumber": 1, "ETag": `"wrong"`})
	missing := create("missing")
	wantFault(missing, "InvalidPart", map[string]any{"PartNumber": 9, "ETag": `"missing"`})

	badOrder := create("order")
	large := bytes.Repeat([]byte("A"), 5<<20)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": badOrder, "PartNumber": 2}, large)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": badOrder, "PartNumber": 1}, []byte("last"))
	wantFault(badOrder, "InvalidPartOrder", completedPart(2, second), completedPart(1, first))

	tooSmall := create("small")
	smallFirst := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": tooSmall, "PartNumber": 1}, []byte("small"))
	smallLast := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": tooSmall, "PartNumber": 2}, []byte("last"))
	wantFault(tooSmall, "EntityTooSmall", completedPart(1, smallFirst), completedPart(2, smallLast))
	wantFault(create("empty"), "InvalidPart")
}

func TestListPartsAndMultipartUploads(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "k"}, nil)
	id, _ := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "k", "UploadId": id, "PartNumber": 1}, []byte("AAA"))
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
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
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
