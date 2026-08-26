package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
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

func TestObjectMetadata(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "b", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{
		"Bucket": "b", "Key": "source", "CacheControl": "max-age=60", "ContentDisposition": `attachment; filename="one.txt"`,
		"ContentEncoding": "gzip", "ContentLanguage": "en-US", "ContentType": "text/plain", "Expires": "Wed, 21 Oct 2026 07:28:00 GMT",
		"Metadata": map[string]any{"Owner": "mirror", "Empty": ""}, "WebsiteRedirectLocation": "/old",
	}, []byte("first"))
	assert := func(name string, response *spi.Response, contentType, owner string) {
		t.Helper()
		if response.Headers.Get("Content-Type") != contentType || response.Headers.Get("x-amz-meta-owner") != owner {
			t.Fatalf("%s metadata = %v", name, response.Headers)
		}
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "source", "VersionId": first.Headers.Get("x-amz-version-id")}, nil)
	assert("get", get, "text/plain", "mirror")
	if get.Headers.Get("Cache-Control") != "max-age=60" || get.Headers.Get("Content-Disposition") != `attachment; filename="one.txt"` || get.Headers.Get("Content-Encoding") != "gzip" || get.Headers.Get("Content-Language") != "en-US" || get.Headers.Get("Expires") != "Wed, 21 Oct 2026 07:28:00 GMT" || get.Headers.Get("x-amz-website-redirect-location") != "/old" {
		t.Fatalf("get system metadata = %v", get.Headers)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "source", "VersionId": first.Headers.Get("x-amz-version-id")}, nil)
	assert("head", head, "text/plain", "mirror")

	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "copied", "CopySource": "b/source"}, nil)
	copied := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "copied"}, nil)
	assert("copied", copied, "text/plain", "mirror")
	if copied.Headers.Get("x-amz-website-redirect-location") != "" {
		t.Fatalf("copy inherited website redirect = %v", copied.Headers)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "redirected", "CopySource": "b/source", "WebsiteRedirectLocation": "/new"}, nil)
	if redirected := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "redirected"}, nil); redirected.Headers.Get("x-amz-website-redirect-location") != "/new" {
		t.Fatalf("explicit copy redirect = %v", redirected.Headers)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "b", "Key": "replaced", "CopySource": "b/source", "MetadataDirective": "REPLACE",
		"ContentType": "application/json", "Metadata": map[string]any{"Owner": "new"},
	}, nil)
	replaced := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "replaced"}, nil)
	assert("replaced", replaced, "application/json", "new")
	if replaced.Headers.Get("Cache-Control") != "" || replaced.Headers.Get("Content-Encoding") != "" {
		t.Fatalf("replace inherited system metadata = %v", replaced.Headers)
	}

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "default"}, []byte("body"))
	defaultHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "default"}, nil)
	assert("default", defaultHead, "binary/octet-stream", "")
	golden.AssertJSON(t, map[string]any{
		"get":      map[string]any{"contentType": get.Headers.Get("Content-Type"), "cacheControl": get.Headers.Get("Cache-Control"), "owner": get.Headers.Get("x-amz-meta-owner"), "redirect": get.Headers.Get("x-amz-website-redirect-location")},
		"head":     map[string]any{"contentType": head.Headers.Get("Content-Type"), "owner": head.Headers.Get("x-amz-meta-owner")},
		"replaced": map[string]any{"contentType": replaced.Headers.Get("Content-Type"), "cacheControl": replaced.Headers.Get("Cache-Control"), "owner": replaced.Headers.Get("x-amz-meta-owner")},
		"default":  map[string]any{"contentType": defaultHead.Headers.Get("Content-Type")},
	})
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

func TestCopyObjectDirectiveValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "source"}, []byte("body"))
	errors := map[string]any{}
	for _, test := range []struct{ input, value string }{
		{"MetadataDirective", "INVALID"},
		{"MetadataDirective", "copy"},
		{"TaggingDirective", "INVALID"},
		{"TaggingDirective", "replace"},
	} {
		key := test.input + "-" + test.value
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": key, "CopySource": "b/source", test.input: test.value}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s=%s fault = %#v", test.input, test.value, fault)
		}
		errors[key] = fault.Code
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("invalid directive created %s: %v", key, err)
		}
	}
	golden.AssertJSON(t, errors)
}

func TestExpectedBucketOwnerAndDeleteBoundary(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "k"}, []byte("body"))
	if _, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "b", "ExpectedBucketOwner": ident().Account}, nil); err != nil {
		t.Fatalf("matching owner: %v", err)
	}
	errors := map[string]any{}
	for _, expected := range []string{"12345678901", "12345678901x"} {
		_, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "b", "ExpectedBucketOwner": expected}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketOwnerAWSAccountID" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("expected owner %q = %#v", expected, fault)
		}
		errors[expected] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"HeadBucket", map[string]any{"Bucket": "b"}},
		{"GetObject", map[string]any{"Bucket": "b", "Key": "k"}},
		{"HeadObject", map[string]any{"Bucket": "b", "Key": "k"}},
		{"PutObjectTagging", map[string]any{"Bucket": "b", "Key": "k", "TagSet": []any{}}},
		{"DeleteObject", map[string]any{"Bucket": "b", "Key": "k"}},
	} {
		test.input["ExpectedBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
	}
	for _, operation := range []string{"DeleteObject", "DeleteObjects", "GetObjectTagging"} {
		input := map[string]any{"Bucket": "missing", "Key": "k", "Objects": []any{map[string]any{"Key": "k"}}}
		_, err := invoke(t, p, operation, input, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", operation, fault)
		}
		errors[operation+"Missing"] = fault.Code
	}
	golden.AssertJSON(t, errors)
}

func TestExpectedSourceBucketOwnerAndCopyBoundary(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "source"}, nil)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "destination"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "key"}, []byte("body"))
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "copy", "CopySource": "source/key", "ExpectedSourceBucketOwner": ident().Account}, nil)

	upload := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "destination", "Key": "multipart"}, nil)
	uploadID := upload.Output["UploadId"].(string)
	mustInvoke(t, p, "UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 1, "CopySource": "source/key", "ExpectedSourceBucketOwner": ident().Account}, nil)
	httpReq := httptest.NewRequest(http.MethodPut, "/destination/header-denied", nil)
	httpReq.Header.Set("x-amz-copy-source", "source/key")
	httpReq.Header.Set("x-amz-source-expected-bucket-owner", "999999999999")
	if _, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Identity: ident(), HTTP: httpReq, Input: map[string]any{"Bucket": "destination", "Key": "header-denied", "CopySource": "source/key"}}); asFault(t, err).Code != "AccessDenied" {
		t.Fatalf("mismatched source owner header: %v", err)
	}
	httpReq = httptest.NewRequest(http.MethodPut, "/destination/header-copy", nil)
	httpReq.Header.Set("x-amz-copy-source", "source/key")
	httpReq.Header.Set("x-amz-source-expected-bucket-owner", ident().Account)
	if _, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Identity: ident(), HTTP: httpReq, Input: map[string]any{"Bucket": "destination", "Key": "header-copy", "CopySource": "source/key"}}); err != nil {
		t.Fatalf("matching source owner header: %v", err)
	}

	errors := map[string]any{}
	for _, expected := range []string{"12345678901", "12345678901x"} {
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "invalid-" + expected, "CopySource": "source/key", "ExpectedSourceBucketOwner": expected}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketOwnerAWSAccountID" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("expected source owner %q = %#v", expected, fault)
		}
		errors[expected] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"CopyObject", map[string]any{"Bucket": "destination", "Key": "denied", "CopySource": "source/key"}},
		{"UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 2, "CopySource": "source/key"}},
	} {
		test.input["ExpectedSourceBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"CopyObject", map[string]any{"Bucket": "destination", "Key": "missing", "CopySource": "missing/key"}},
		{"UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 2, "CopySource": "missing/key"}},
	} {
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing source = %#v", test.operation, fault)
		}
		errors[test.operation+"Missing"] = fault.Code
	}
	for _, key := range []string{"denied", "header-denied"} {
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "destination", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("denied copy %q created object: %v", key, err)
		}
	}
	if parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID}, nil).Output["Parts"].([]any); len(parts) != 1 {
		t.Fatalf("rejected part mutated upload: %#v", parts)
	}
	golden.AssertJSON(t, errors)
}

func TestTagValidationAndBucketSemantics(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	characterization := map[string]any{}
	for _, operation := range []string{"PutBucketTagging", "GetBucketTagging", "DeleteBucketTagging"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "missing", "TagSet": []any{}}, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", operation, fault)
		}
		characterization[operation+"MissingBucket"] = fault.Code
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	_, err := invoke(t, p, "GetBucketTagging", map[string]any{"Bucket": "b"}, nil)
	if asFault(t, err).Code != "NoSuchTagSet" {
		t.Fatalf("untagged bucket = %v", err)
	}
	characterization["untaggedBucket"] = asFault(t, err).Code
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "source"}, []byte("body"))
	valid := []any{map[string]any{"Key": "team α", "Value": ""}}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "TagSet": valid}, nil)

	tags := func(count int) []any {
		out := make([]any, count)
		for i := range out {
			out[i] = map[string]any{"Key": fmt.Sprintf("key%d", i), "Value": "value"}
		}
		return out
	}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "TagSet": tags(10)}, nil)
	mustInvoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "b", "TagSet": tags(50)}, nil)
	characterization["acceptedObjectTags"] = 10
	characterization["acceptedBucketTags"] = 50
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "TagSet": valid}, nil)
	mustInvoke(t, p, "DeleteBucketTagging", map[string]any{"Bucket": "b"}, nil)
	for _, test := range []struct {
		name string
		set  any
		code string
	}{
		{"missing-tag-set", nil, "MalformedXML"},
		{"missing-value", []any{map[string]any{"Key": "key"}}, "MalformedXML"},
		{"duplicate-key", []any{map[string]any{"Key": "key", "Value": "one"}, map[string]any{"Key": "key", "Value": "two"}}, "InvalidTag"},
		{"reserved-key", []any{map[string]any{"Key": "aws:team", "Value": "one"}}, "InvalidTag"},
		{"empty-key", []any{map[string]any{"Key": "", "Value": "one"}}, "InvalidTag"},
		{"long-key", []any{map[string]any{"Key": strings.Repeat("k", 129), "Value": "one"}}, "InvalidTag"},
		{"invalid-key", []any{map[string]any{"Key": "team?", "Value": "one"}}, "InvalidTag"},
		{"long-value", []any{map[string]any{"Key": "team", "Value": strings.Repeat("v", 257)}}, "InvalidTag"},
		{"too-many-object-tags", tags(11), "InvalidTag"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := invoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "TagSet": test.set}, nil)
			fault := asFault(t, err)
			if fault.Code != test.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
			characterization[test.name] = fault.Code
			got := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source"}, nil)
			if set := asSliceForTest(got.Output["TagSet"]); len(set) != 1 || asMapForTest(set[0])["Key"] != "team α" {
				t.Fatalf("rejected write changed tags: %#v", got.Output)
			}
		})
	}
	if _, err := invoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "b", "TagSet": tags(51)}, nil); asFault(t, err).Code != "InvalidTag" {
		t.Fatalf("too many bucket tags = %v", err)
	}
	for _, operation := range []string{"PutObject", "CreateMultipartUpload"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "b", "Key": operation, "Tagging": "key=one&key=two"}, []byte("body"))
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s duplicate header = %#v", operation, fault)
		}
		characterization[operation+"DuplicateHeader"] = fault.Code
	}
	headerTags := url.Values{}
	for i := range 11 {
		headerTags.Set(fmt.Sprintf("key%d", i), "value")
	}
	rejectedKeys := []string{"PutObject", "copy"}
	for _, test := range []struct{ name, tagging string }{
		{"malformed-header", "%zz=value"},
		{"invalid-header-key", "team%3F=value"},
		{"too-many-header-tags", headerTags.Encode()},
	} {
		key := "rejected-" + test.name
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": key, "Tagging": test.tagging}, []byte("body"))
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s = %#v", test.name, fault)
		}
		characterization[test.name] = fault.Code
		rejectedKeys = append(rejectedKeys, key)
	}
	_, err = invoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "copy", "CopySource": "b/source", "TaggingDirective": "REPLACE", "Tagging": "key=one&key=two"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("copy duplicate header = %#v", fault)
	}
	for _, key := range rejectedKeys {
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("rejected %s created object: %v", key, err)
		}
	}
	characterization["storedTags"] = mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source"}, nil).Output["TagSet"]
	golden.AssertJSON(t, characterization)
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

func TestObjectReadConditions(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "conditional"}, []byte("body"))
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "conditional"}, nil)
	modified, err := http.ParseTime(head.Headers.Get("Last-Modified"))
	if err != nil {
		t.Fatal(err)
	}
	past, future := modified.Add(-time.Hour).Format(http.TimeFormat), modified.Add(time.Hour).Format(http.TimeFormat)
	etag := put.Headers.Get("ETag")
	call := func(operation string, conditions map[string]any) (*spi.Response, error) {
		t.Helper()
		input := map[string]any{"Bucket": "b", "Key": "conditional"}
		if operation == "GetObjectAttributes" {
			input["ObjectAttributes"] = []string{"ETag"}
		}
		for key, value := range conditions {
			input[key] = value
		}
		response, err := invoke(t, p, operation, input, nil)
		if response != nil && response.Stream != nil {
			_ = response.Stream.Close()
		}
		return response, err
	}
	for _, operation := range []string{"GetObject", "HeadObject", "GetObjectAttributes"} {
		for _, conditions := range []map[string]any{
			{"IfMatch": `"wrong"`},
			{"IfUnmodifiedSince": past},
		} {
			_, err := call(operation, conditions)
			if fault := asFault(t, err); fault.Code != "PreconditionFailed" || fault.HTTPStatus != http.StatusPreconditionFailed {
				t.Fatalf("%s %#v fault = %#v", operation, conditions, fault)
			}
		}
		for _, conditions := range []map[string]any{
			{"IfNoneMatch": etag},
			{"IfNoneMatch": "*"},
			{"IfModifiedSince": future},
		} {
			response, err := call(operation, conditions)
			if err != nil || response.Status != http.StatusNotModified {
				t.Fatalf("%s %#v = %#v %v", operation, conditions, response, err)
			}
		}
		for _, conditions := range []map[string]any{
			{"IfMatch": `"wrong", ` + etag, "IfUnmodifiedSince": past},
			{"IfNoneMatch": `"wrong"`, "IfModifiedSince": future},
		} {
			response, err := call(operation, conditions)
			if err != nil || response.Status == http.StatusNotModified {
				t.Fatalf("%s precedence %#v: %#v %v", operation, conditions, response, err)
			}
		}
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
	versioned := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": key, "VersionId": firstVersion}, nil)
	if versioned.Headers.Get("ETag") != first.Headers.Get("ETag") || versioned.Headers.Get("x-amz-version-id") != firstVersion || string(readStream(t, versioned)) != "first" {
		t.Fatalf("versioned get headers = %v", versioned.Headers)
	}
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": key, "VersionId": firstVersion}, nil); head.Headers.Get("ETag") != first.Headers.Get("ETag") || head.Headers.Get("x-amz-version-id") != firstVersion || head.Headers.Get("Content-Length") != "5" {
		t.Fatalf("versioned head headers = %v", head.Headers)
	}
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
	markerVersion := deleted.Headers.Get("x-amz-version-id")
	for _, operation := range []string{"GetObject", "HeadObject"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "b", "Key": key}, nil)
		if fault := asFault(t, err); fault.HTTPStatus != http.StatusNotFound || fault.Headers.Get("x-amz-delete-marker") != "true" || fault.Headers.Get("x-amz-version-id") != markerVersion {
			t.Fatalf("%s current marker fault = %#v", operation, fault)
		}
		_, err = invoke(t, p, operation, map[string]any{"Bucket": "b", "Key": key, "VersionId": markerVersion}, nil)
		if fault := asFault(t, err); fault.Code != "MethodNotAllowed" || fault.HTTPStatus != http.StatusMethodNotAllowed || fault.Headers.Get("Last-Modified") == "" || fault.Headers.Get("x-amz-delete-marker") != "true" || fault.Headers.Get("x-amz-version-id") != markerVersion {
			t.Fatalf("%s explicit marker fault = %#v", operation, fault)
		}
	}
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
		{source + "?versionId=" + markerVersion, "InvalidRequest"},
	} {
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "invalid", "CopySource": invalid.source}, nil)
		if fault := asFault(t, err); fault.Code != invalid.code {
			t.Fatalf("%q fault = %#v", invalid.source, fault)
		}
	}
}

func TestVersionedObjectTaggingCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "b", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "source", "Tagging": "stage=first&team=storage"}, []byte("first"))
	firstVersion := first.Headers.Get("x-amz-version-id")
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "source"}, []byte("second"))
	secondVersion := second.Headers.Get("x-amz-version-id")

	firstTags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "VersionId": firstVersion}, nil)
	if firstTags.Headers.Get("x-amz-version-id") != firstVersion || len(asSliceForTest(firstTags.Output["TagSet"])) != 2 {
		t.Fatalf("first version tags = %#v headers %v", firstTags.Output, firstTags.Headers)
	}
	if current := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "source"}, nil); current.Headers.Get("x-amz-tagging-count") != "" {
		t.Fatalf("new untagged version inherited tags: %v", current.Headers)
	}
	if old := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "source", "VersionId": firstVersion}, nil); old.Headers.Get("x-amz-tagging-count") != "2" {
		t.Fatalf("old version tag count = %v", old.Headers)
	} else {
		_ = old.Stream.Close()
	}

	currentTag := []any{map[string]any{"Key": "stage", "Value": "second"}}
	putCurrent := mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "TagSet": currentTag}, nil)
	if putCurrent.Headers.Get("x-amz-version-id") != secondVersion {
		t.Fatalf("current tag version = %v", putCurrent.Headers)
	}
	explicitTag := []any{map[string]any{"Key": "stage", "Value": "retagged"}}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "VersionId": firstVersion, "TagSet": explicitTag}, nil)

	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "copied", "CopySource": "b/source?versionId=" + firstVersion}, nil)
	copiedTags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "copied"}, nil)
	if tags := asSliceForTest(copiedTags.Output["TagSet"]); len(tags) != 1 || asMapForTest(tags[0])["Value"] != "retagged" {
		t.Fatalf("version copy tags = %#v", copiedTags.Output)
	}

	deletedTags := mustInvoke(t, p, "DeleteObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "VersionId": firstVersion}, nil)
	if deletedTags.Headers.Get("x-amz-version-id") != firstVersion || len(asSliceForTest(mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "VersionId": firstVersion}, nil).Output["TagSet"])) != 0 {
		t.Fatalf("deleted version tags = %v", deletedTags.Headers)
	}
	current := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source"}, nil)
	if tags := asSliceForTest(current.Output["TagSet"]); current.Headers.Get("x-amz-version-id") != secondVersion || len(tags) != 1 || asMapForTest(tags[0])["Value"] != "second" {
		t.Fatalf("current tags changed with old version: %#v headers %v", current.Output, current.Headers)
	}

	marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "b", "Key": "source"}, nil).Headers.Get("x-amz-version-id")
	retained := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "VersionId": secondVersion}, nil)
	if tags := asSliceForTest(retained.Output["TagSet"]); len(tags) != 1 || asMapForTest(tags[0])["Value"] != "second" {
		t.Fatalf("delete marker lost version tags: %#v", retained.Output)
	}
	_, currentErr := invoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source"}, nil)
	_, markerErr := invoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "source", "VersionId": marker}, nil)
	for _, operation := range []string{"GetObjectTagging", "PutObjectTagging", "DeleteObjectTagging"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "b", "Key": "missing", "TagSet": currentTag}, nil)
		if fault := asFault(t, err); fault.Code != "NoSuchKey" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing object fault = %#v", operation, fault)
		}
	}

	golden.AssertJSON(t, map[string]any{
		"firstVersionTags": firstTags.Output["TagSet"],
		"currentTags":      current.Output["TagSet"],
		"retainedTags":     retained.Output["TagSet"],
		"copiedTags":       copiedTags.Output["TagSet"],
		"currentMarker":    asFault(t, currentErr).Code,
		"explicitMarker":   asFault(t, markerErr).Code,
	})
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
	object := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "k"}, nil)
	if object.Headers.Get("ETag") != etag || mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "k"}, nil).Headers.Get("ETag") != etag {
		t.Fatal("multipart ETag was not persisted")
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "b", "Key": "copy", "CopySource": "b/k", "CopySourceIfMatch": etag}, nil)
	got := readStream(t, object)
	if len(got) != len(firstBody)+3 || !bytes.Equal(got[:len(firstBody)], firstBody) || string(got[len(firstBody):]) != "BBB" {
		t.Fatalf("assembled %d bytes", len(got))
	}
}

func TestMultipartPartReads(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "b", "Status": "Enabled"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "k", "ChecksumAlgorithm": "SHA256"}, nil)
	id := created.Output["UploadId"].(string)
	firstBody := bytes.Repeat([]byte("A"), 5<<20)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, firstBody)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, []byte("tail"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, first), completedPart(2, second)), nil)
	version := done.Headers.Get("x-amz-version-id")
	if version == "" {
		t.Fatal("missing multipart version")
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "k"}, []byte("newer"))

	input := map[string]any{"Bucket": "b", "Key": "k", "VersionId": version, "PartNumber": 2, "ChecksumMode": "ENABLED"}
	get := mustInvoke(t, p, "GetObject", input, nil)
	if body := readStream(t, get); string(body) != "tail" {
		t.Fatalf("part body = %q", body)
	}
	if get.Status != http.StatusPartialContent || get.Headers.Get("Content-Length") != "4" || get.Headers.Get("Content-Range") != "bytes 5242880-5242883/5242884" || get.Headers.Get("x-amz-mp-parts-count") != "2" {
		t.Fatalf("part headers = status %d %v", get.Status, get.Headers)
	}
	if get.Headers.Get("x-amz-checksum-sha256") != second.Headers.Get("x-amz-checksum-sha256") || get.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
		t.Fatalf("part checksum = %v", get.Headers)
	}
	head := mustInvoke(t, p, "HeadObject", input, nil)
	if head.Status != http.StatusPartialContent || head.Headers.Get("Content-Length") != "4" || head.Headers.Get("Content-Range") != get.Headers.Get("Content-Range") || head.Headers.Get("x-amz-mp-parts-count") != "2" || head.Headers.Get("x-amz-checksum-sha256") != second.Headers.Get("x-amz-checksum-sha256") {
		t.Fatalf("head part = status %d %v", head.Status, head.Headers)
	}

	whole := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "k", "PartNumber": 1}, nil)
	if body := readStream(t, whole); string(body) != "newer" || whole.Status != http.StatusPartialContent || whole.Headers.Get("x-amz-mp-parts-count") != "" {
		t.Fatalf("ordinary part one = %q status %d %v", body, whole.Status, whole.Headers)
	}
	for _, number := range []int{0, 3, 10001} {
		_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "k", "VersionId": version, "PartNumber": number}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidPartNumber" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("part %d fault = %#v", number, fault)
		}
	}
	_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "k", "VersionId": version, "PartNumber": 1, "Range": "bytes=0-1"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("part and range fault = %#v", fault)
	}

	golden.AssertJSON(t, map[string]any{
		"get":  map[string]any{"body": "tail", "status": get.Status, "length": get.Headers.Get("Content-Length"), "range": get.Headers.Get("Content-Range"), "parts": get.Headers.Get("x-amz-mp-parts-count"), "checksum": get.Headers.Get("x-amz-checksum-sha256")},
		"head": map[string]any{"status": head.Status, "length": head.Headers.Get("Content-Length"), "range": head.Headers.Get("Content-Range"), "parts": head.Headers.Get("x-amz-mp-parts-count"), "checksum": head.Headers.Get("x-amz-checksum-sha256")},
	})
}

func TestObjectByteRanges(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	body := []byte("0123456789")
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	checksum := base64.StdEncoding.EncodeToString(sum)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "range", "ChecksumCRC32": checksum}, body)
	get := func(value string) (*spi.Response, []byte, error) {
		response, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "range", "Range": value, "ChecksumMode": "ENABLED"}, nil)
		if err != nil {
			return response, nil, err
		}
		return response, readStream(t, response), nil
	}
	for _, test := range []struct {
		value, body, contentRange string
		checksum                  bool
	}{
		{"bytes=2-5", "2345", "bytes 2-5/10", false},
		{"bytes=7-", "789", "bytes 7-9/10", false},
		{"bytes=-3", "789", "bytes 7-9/10", false},
		{"bytes=-20", "0123456789", "bytes 0-9/10", true},
		{"bytes=8-99", "89", "bytes 8-9/10", false},
	} {
		response, got, err := get(test.value)
		if err != nil || response.Status != http.StatusPartialContent || string(got) != test.body || response.Headers.Get("Content-Range") != test.contentRange || response.Headers.Get("Content-Length") != strconv.Itoa(len(test.body)) || response.Headers.Get("Accept-Ranges") != "bytes" {
			t.Fatalf("range %q = %q %#v %v", test.value, got, response, err)
		}
		if hasChecksum := response.Headers.Get("x-amz-checksum-crc32") != ""; hasChecksum != test.checksum {
			t.Fatalf("range %q checksum headers = %v", test.value, response.Headers)
		}
	}
	for _, value := range []string{"2-5", "items=0-1", "bytes=bad", "bytes=5-2", "bytes=0-1,3-4"} {
		response, got, err := get(value)
		if err != nil || response.Status != http.StatusOK || string(got) != string(body) || response.Headers.Get("Content-Range") != "" {
			t.Fatalf("ignored range %q = %q %#v %v", value, got, response, err)
		}
	}
	for _, value := range []string{"bytes=10-", "bytes=-0"} {
		_, _, err := get(value)
		fault := asFault(t, err)
		if fault.Code != "InvalidRange" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable || fault.Headers.Get("Content-Range") != "bytes */10" {
			t.Fatalf("range %q fault = %#v", value, fault)
		}
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "range", "Range": "bytes=-3", "ChecksumMode": "ENABLED"}, nil)
	if head.Status != http.StatusPartialContent || head.Headers.Get("Content-Length") != "3" || head.Headers.Get("Content-Range") != "bytes 7-9/10" || head.Headers.Get("Accept-Ranges") != "bytes" || head.Headers.Get("x-amz-checksum-crc32") != "" {
		t.Fatalf("head range = %#v", head)
	}
}

func TestGetObjectAttributesContract(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	_, err := invoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "missing", "Key": "k", "ObjectAttributes": []string{"ETag"}}, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
		t.Fatalf("missing attributes bucket = %#v", fault)
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "standard"}, []byte("body"))
	if standard := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "b", "Key": "standard", "ObjectAttributes": []string{"StorageClass"}}, nil); len(standard.Output) != 0 {
		t.Fatalf("standard storage class attributes = %#v", standard.Output)
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "b", "Status": "Enabled"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "composite", "ChecksumAlgorithm": "SHA256", "StorageClass": "STANDARD_IA"}, nil)
	id := created.Output["UploadId"].(string)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, bytes.Repeat([]byte("A"), 5<<20))
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, bytes.Repeat([]byte("B"), 5<<20))
	third := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 3}, []byte("tail"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, first), completedPart(2, second), completedPart(3, third)), nil)
	version := done.Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "composite"}, []byte("newer"))

	attrs := []string{"ETag", "Checksum", "ObjectParts", "StorageClass", "ObjectSize"}
	page := mustInvoke(t, p, "GetObjectAttributes", map[string]any{
		"Bucket": "b", "Key": "composite", "VersionId": version, "ObjectAttributes": attrs, "MaxParts": 2,
	}, nil)
	if page.Output["ETag"] != done.Output["ETag"] || page.Output["ObjectSize"] != 10<<20+4 || page.Output["StorageClass"] != "STANDARD_IA" || page.Headers.Get("x-amz-version-id") != version || page.Headers.Get("Last-Modified") == "" {
		t.Fatalf("object attributes = %#v %v", page.Output, page.Headers)
	}
	checksum := asMapForTest(page.Output["Checksum"])
	if checksum["ChecksumSHA256"] != strings.SplitN(done.Output["ChecksumSHA256"].(string), "-", 2)[0] || checksum["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("object checksum = %#v", checksum)
	}
	objectParts := asMapForTest(page.Output["ObjectParts"])
	listed := objectParts["Parts"].([]any)
	if objectParts["TotalPartsCount"] != 3 || objectParts["IsTruncated"] != true || objectParts["MaxParts"] != 2 || objectParts["PartNumberMarker"] != "0" || objectParts["NextPartNumberMarker"] != "2" || len(listed) != 2 || asMapForTest(listed[0])["PartNumber"] != 1 || asMapForTest(listed[1])["ChecksumSHA256"] != second.Headers.Get("x-amz-checksum-sha256") {
		t.Fatalf("object parts page = %#v", objectParts)
	}
	lastPage := mustInvoke(t, p, "GetObjectAttributes", map[string]any{
		"Bucket": "b", "Key": "composite", "VersionId": version, "ObjectAttributes": []any{"ObjectParts"}, "PartNumberMarker": "2", "MaxParts": 2,
	}, nil).Output
	lastParts := asMapForTest(lastPage["ObjectParts"])
	if lastParts["IsTruncated"] != false || lastParts["PartNumberMarker"] != "2" || lastParts["NextPartNumberMarker"] != "3" || len(lastParts["Parts"].([]any)) != 1 || asMapForTest(lastParts["Parts"].([]any)[0])["PartNumber"] != 3 {
		t.Fatalf("object parts final page = %#v", lastParts)
	}
	selected := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "b", "Key": "composite", "VersionId": version, "ObjectAttributes": []string{"ObjectSize"}}, nil)
	if len(selected.Output) != 1 || selected.Output["ObjectSize"] == nil {
		t.Fatalf("selected attributes = %#v", selected.Output)
	}
	for field, value := range map[string]any{"MaxParts": 1001, "PartNumberMarker": "invalid"} {
		_, err := invoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "b", "Key": "composite", "VersionId": version, "ObjectAttributes": []string{"ObjectParts"}, field: value}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid %s fault = %#v", field, fault)
		}
	}

	full := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "full", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil)
	fullID := full.Output["UploadId"].(string)
	fullFirst := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": fullID, "PartNumber": 1}, bytes.Repeat([]byte("C"), 5<<20))
	fullSecond := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": fullID, "PartNumber": 2}, []byte("end"))
	fullDone := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(fullID, completedPart(1, fullFirst), completedPart(2, fullSecond)), nil)
	fullAttrs := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "b", "Key": "full", "ObjectAttributes": []string{"Checksum", "ObjectParts"}}, nil).Output
	if fullChecksum := asMapForTest(fullAttrs["Checksum"]); fullChecksum["ChecksumCRC32"] != fullDone.Output["ChecksumCRC32"] || fullChecksum["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("full checksum attributes = %#v", fullChecksum)
	}
	if fullParts := asMapForTest(fullAttrs["ObjectParts"]); len(fullParts) != 1 || fullParts["TotalPartsCount"] != 2 {
		t.Fatalf("full object parts = %#v", fullParts)
	}

	golden.AssertJSON(t, map[string]any{"page": page.Output, "lastPage": lastPage, "full": fullAttrs})
}

func TestWriteChecksumValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	body := []byte("123456789")
	md5sum, sha1sum, sha256sum, sha512sum := md5.Sum(body), sha1.Sum(body), sha256.Sum256(body), sha512.Sum512(body)
	crc32sum, crc32csum := make([]byte, 4), make([]byte, 4)
	binary.BigEndian.PutUint32(crc32sum, crc32.ChecksumIEEE(body))
	binary.BigEndian.PutUint32(crc32csum, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	b64 := func(sum []byte) string { return base64.StdEncoding.EncodeToString(sum) }
	checksums := map[string]string{
		"ContentMD5":        b64(md5sum[:]),
		"ChecksumMD5":       b64(md5sum[:]),
		"ChecksumCRC32":     b64(crc32sum),
		"ChecksumCRC32C":    b64(crc32csum),
		"ChecksumCRC64NVME": "rosUhgp5mIg=",
		"ChecksumSHA1":      b64(sha1sum[:]),
		"ChecksumSHA256":    b64(sha256sum[:]),
		"ChecksumSHA512":    b64(sha512sum[:]),
	}
	responseHeaders := map[string]string{
		"ChecksumMD5": "x-amz-checksum-md5", "ChecksumCRC32": "x-amz-checksum-crc32", "ChecksumCRC32C": "x-amz-checksum-crc32c",
		"ChecksumCRC64NVME": "x-amz-checksum-crc64nvme", "ChecksumSHA1": "x-amz-checksum-sha1",
		"ChecksumSHA256": "x-amz-checksum-sha256", "ChecksumSHA512": "x-amz-checksum-sha512",
	}
	for name, value := range checksums {
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": name, name: value}, body)
		if header := responseHeaders[name]; header != "" {
			if put.Headers.Get(header) != value || put.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
				t.Fatalf("%s put checksum headers = %v", name, put.Headers)
			}
			get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": name, "ChecksumMode": "ENABLED"}, nil)
			if get.Headers.Get(header) != value || get.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
				t.Fatalf("%s get checksum headers = %v", name, get.Headers)
			}
			_ = get.Stream.Close()
			if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": name, "ChecksumMode": "ENABLED"}, nil); head.Headers.Get(header) != value {
				t.Fatalf("%s head checksum headers = %v", name, head.Headers)
			}
		}
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": name + "-bad", name: "AA=="}, body)
		if fault := asFault(t, err); fault.Code != "BadDigest" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s fault = %#v", name, fault)
		}
	}
	_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "malformed", "ChecksumMD5": "!"}, body)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("malformed checksum fault = %#v", fault)
	}
	_, err = invoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "xxhash", "ChecksumXXHASH64": "AA=="}, body)
	if fault := asFault(t, err); fault.Code != "MirrorNotImplemented" || fault.HTTPStatus != http.StatusNotImplemented {
		t.Fatalf("xxhash checksum fault = %#v", fault)
	}

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "multipart", "ChecksumAlgorithm": "MD5"}, nil)
	uploadID := created.Output["UploadId"].(string)
	_, err = invoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1, "ChecksumMD5": "AA=="}, body)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("upload checksum fault = %#v", fault)
	}
	part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1, "ChecksumMD5": checksums["ChecksumMD5"]}, body)
	if part.Headers.Get("x-amz-checksum-md5") != checksums["ChecksumMD5"] {
		t.Fatalf("upload checksum headers = %v", part.Headers)
	}
	complete := completeInput(uploadID, completedPart(1, part))
	complete["ChecksumMD5"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("complete checksum fault = %#v", fault)
	}
	partDigest := md5.Sum(body)
	compositeDigest := md5.Sum(partDigest[:])
	composite := base64.StdEncoding.EncodeToString(compositeDigest[:]) + "-1"
	complete["ChecksumMD5"] = composite
	done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
	if done.Output["ChecksumMD5"] != composite || done.Output["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("complete checksum output = %#v", done.Output)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "multipart", "ChecksumMode": "ENABLED"}, nil)
	if head.Headers.Get("x-amz-checksum-md5") != composite || head.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
		t.Fatalf("multipart checksum metadata = %v", head.Headers)
	}
}

func TestMultipartChecksumContract(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "missing", "Key": "k"}, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
		t.Fatalf("create missing bucket fault = %#v", fault)
	}
	wantCreateFault := func(input map[string]any, code string) {
		t.Helper()
		input["Bucket"], input["Key"] = "b", code
		_, err := invoke(t, p, "CreateMultipartUpload", input, nil)
		if fault := asFault(t, err); fault.Code != code || fault.HTTPStatus < http.StatusBadRequest {
			t.Fatalf("create checksum fault = %#v want %s", fault, code)
		}
	}
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "SHA256", "ChecksumType": "FULL_OBJECT"}, "InvalidRequest")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "CRC64NVME", "ChecksumType": "COMPOSITE"}, "InvalidRequest")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "CRC32", "ChecksumType": "invalid"}, "InvalidArgument")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "XXHASH64"}, "MirrorNotImplemented")

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "full", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil)
	if created.Headers.Get("x-amz-checksum-algorithm") != "CRC32" || created.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("create checksum headers = %v", created.Headers)
	}
	id := created.Output["UploadId"].(string)
	body := []byte("full object")
	_, err = invoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "full", "UploadId": id, "PartNumber": 1, "ChecksumAlgorithm": "SHA1"}, body)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("requested part algorithm fault = %#v", fault)
	}
	_, err = invoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "full", "UploadId": id, "PartNumber": 1, "ChecksumSHA1": "AA=="}, body)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("part algorithm fault = %#v", fault)
	}
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "full", "UploadId": id, "PartNumber": 1}, body)
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "full", "UploadId": id}, nil)
	if listed.Output["ChecksumAlgorithm"] != "CRC32" || listed.Output["ChecksumType"] != "FULL_OBJECT" || listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] == "" {
		t.Fatalf("listed checksum contract = %#v", listed.Output)
	}
	complete := completeInput(id, completedPart(1, part))
	complete["MultipartUpload"].(map[string]any)["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "InvalidPart" {
		t.Fatalf("manifest checksum fault = %#v", fault)
	}
	delete(complete["MultipartUpload"].(map[string]any)["Parts"].([]any)[0].(map[string]any), "ChecksumCRC32")
	complete["ChecksumType"] = "COMPOSITE"
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("complete checksum type fault = %#v", fault)
	}
	complete["ChecksumType"] = "FULL_OBJECT"
	complete["ChecksumCRC32"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("complete checksum fault = %#v", fault)
	}
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	want := base64.StdEncoding.EncodeToString(sum)
	complete["ChecksumCRC32"] = want
	done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
	if done.Output["ChecksumCRC32"] != want || done.Output["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("complete checksum = %#v", done.Output)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "full", "ChecksumMode": "ENABLED"}, nil)
	if head.Headers.Get("x-amz-checksum-crc32") != want || head.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("stored checksum = %v", head.Headers)
	}

	composite := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "gap", "ChecksumAlgorithm": "SHA256"}, nil)
	compositeID := composite.Output["UploadId"].(string)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "gap", "UploadId": compositeID, "PartNumber": 2}, []byte("second"))
	_, err = invoke(t, p, "CompleteMultipartUpload", completeInput(compositeID, completedPart(2, second)), nil)
	if fault := asFault(t, err); fault.Code != "InternalError" || fault.HTTPStatus != http.StatusInternalServerError {
		t.Fatalf("nonconsecutive composite fault = %#v", fault)
	}
}

func TestMultipartChecksumCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "snapshot", "ChecksumAlgorithm": "SHA256", "StorageClass": "STANDARD_IA", "Tagging": "env=snapshot"}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "snapshot", "UploadId": id, "PartNumber": 1}, []byte("snapshot"))
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "snapshot", "UploadId": id}, nil)
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "snapshot"}, nil)
	tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "snapshot"}, nil).Output["TagSet"]
	golden.AssertJSON(t, map[string]any{
		"create":   map[string]any{"algorithm": created.Output["ChecksumAlgorithm"], "type": created.Output["ChecksumType"], "storageClass": "STANDARD_IA", "tags": "env=snapshot"},
		"part":     map[string]any{"checksum": part.Headers.Get("x-amz-checksum-sha256")},
		"list":     map[string]any{"algorithm": listed.Output["ChecksumAlgorithm"], "type": listed.Output["ChecksumType"], "part": listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumSHA256"]},
		"complete": map[string]any{"checksum": done.Output["ChecksumSHA256"], "type": done.Output["ChecksumType"]},
		"object":   map[string]any{"storageClass": head.Headers.Get("x-amz-storage-class"), "tags": tags},
	})
}

func TestMultipartCreationAttributes(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{
		"Bucket": "b", "Key": "attributes", "StorageClass": "STANDARD_IA", "Tagging": "team=storage&env=test",
	}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "b", "Key": "attributes", "UploadId": id, "PartNumber": 1}, []byte("body"))
	complete := completeInput(id, completedPart(1, part))
	complete["StorageClass"], complete["Tagging"] = "STANDARD", "ignored=true"
	mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)

	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "attributes"}, nil)
	if head.Headers.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Fatalf("multipart storage class = %v", head.Headers)
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "b", "Key": "attributes"}, nil)
	if get.Headers.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Fatalf("multipart get storage class = %v", get.Headers)
	}
	_ = get.Stream.Close()
	tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "b", "Key": "attributes"}, nil).Output["TagSet"].([]any)
	if len(tags) != 2 || asMapForTest(tags[0])["Key"] != "env" || asMapForTest(tags[0])["Value"] != "test" || asMapForTest(tags[1])["Key"] != "team" || asMapForTest(tags[1])["Value"] != "storage" {
		t.Fatalf("multipart tags = %#v", tags)
	}
	standard := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "b", "Key": "standard"}, []byte("body"))
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "b", "Key": "standard"}, nil); standard.Headers.Get("x-amz-storage-class") != "" || head.Headers.Get("x-amz-storage-class") != "" {
		t.Fatalf("standard storage class headers = put %v head %v", standard.Headers, head.Headers)
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
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(noncontiguous, completedPart(3, third)), nil)
	if !regexp.MustCompile(`-1"$`).MatchString(done.Headers.Get("ETag")) {
		t.Fatalf("selected part ETag = %q", done.Headers.Get("ETag"))
	}
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

	sized := create("sized")
	sizedPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": sized, "PartNumber": 1}, []byte("sized"))
	sizedInput := completeInput(sized, completedPart(1, sizedPart))
	sizedInput["MpuObjectSize"] = "4"
	_, err := invoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("object size fault = %#v", fault)
	}
	sizedInput["MpuObjectSize"] = "invalid"
	_, err = invoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("invalid object size fault = %#v", fault)
	}
	sizedInput["MpuObjectSize"] = "5"
	mustInvoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	zero := create("zero")
	zeroPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": zero, "PartNumber": 1}, []byte{})
	zeroInput := completeInput(zero, completedPart(1, zeroPart))
	zeroInput["MpuObjectSize"] = "invalid"
	_, err = invoke(t, p, "CompleteMultipartUpload", zeroInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("zero object size fault = %#v", fault)
	}
	zeroInput["MpuObjectSize"] = "0"
	mustInvoke(t, p, "CompleteMultipartUpload", zeroInput, nil)
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
	if len(parts) != 1 || listed.Output["ChecksumAlgorithm"] != "CRC64NVME" || listed.Output["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("ListParts %v", listed.Output)
	}
	paged := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "paged", "StorageClass": "STANDARD_IA", "ChecksumAlgorithm": "CRC32"}, nil)
	pagedID := paged.Output["UploadId"].(string)
	for _, number := range []int{3, 1, 2} {
		input := map[string]any{"Bucket": "b", "Key": "paged", "UploadId": pagedID, "PartNumber": number}
		if number == 3 {
			sum := make([]byte, 4)
			binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE([]byte("CCC")))
			input["ChecksumCRC32"] = base64.StdEncoding.EncodeToString(sum)
		}
		mustInvoke(t, p, "UploadPart", input, bytes.Repeat([]byte{byte('A' + number - 1)}, 3))
	}
	firstPage := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "paged", "UploadId": pagedID, "MaxParts": 2}, nil)
	firstParts := firstPage.Output["Parts"].([]any)
	if len(firstParts) != 2 || firstParts[0].(map[string]any)["PartNumber"] != 1 || firstParts[1].(map[string]any)["PartNumber"] != 2 || firstPage.Output["IsTruncated"] != true || firstPage.Output["NextPartNumberMarker"] != 2 || firstPage.Output["StorageClass"] != "STANDARD_IA" || firstPage.Output["ChecksumAlgorithm"] != "CRC32" || firstPage.Output["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("ListParts first page %v", firstPage.Output)
	}
	secondPage := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "paged", "UploadId": pagedID, "PartNumberMarker": 2, "MaxParts": 2}, nil)
	last := secondPage.Output["Parts"].([]any)[0].(map[string]any)
	if last["PartNumber"] != 3 || last["LastModified"] == "" || last["ChecksumCRC32"] == nil || secondPage.Output["IsTruncated"] != false || secondPage.Output["PartNumberMarker"] != 2 {
		t.Fatalf("ListParts second page %v", secondPage.Output)
	}
	for _, input := range []map[string]any{
		{"Bucket": "b", "Key": "paged", "UploadId": "missing"},
		{"Bucket": "b", "Key": "wrong", "UploadId": pagedID},
	} {
		_, err := invoke(t, p, "ListParts", input, nil)
		if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("ListParts missing upload fault = %#v", fault)
		}
	}
	_, err := invoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "paged", "UploadId": pagedID, "MaxParts": 1001}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("ListParts max fault = %#v", fault)
	}
	ups := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "b"}, nil)
	uploads, _ := ups.Output["Uploads"].([]any)
	if len(uploads) != 2 {
		t.Fatalf("ListMultipartUploads %v", ups.Output)
	}
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
	mustInvoke(t, p, "AbortMultipartUpload", map[string]any{"Bucket": "b", "Key": "paged", "UploadId": pagedID}, nil)
	after := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "b"}, nil)
	uploads, _ = after.Output["Uploads"].([]any)
	if len(uploads) != 0 {
		t.Fatalf("completed upload still listed: %v", after.Output)
	}
}

func TestListMultipartUploadsPaginationAndDelimiter(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	create := func(key, storageClass string) string {
		t.Helper()
		response := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": key, "StorageClass": storageClass}, nil)
		_ = deps.Clock.Advance(time.Second)
		return response.Output["UploadId"].(string)
	}
	create("photos/2026/b.jpg", "STANDARD")
	firstSame := create("same", "STANDARD_IA")
	create("alpha", "STANDARD")
	secondSame := create("same", "STANDARD")
	create("space key", "STANDARD")

	firstPage := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "b", "MaxUploads": 3}, nil)
	first := firstPage.Output["Uploads"].([]any)
	if len(first) != 3 || first[0].(map[string]any)["Key"] != "alpha" || first[1].(map[string]any)["Key"] != "photos/2026/b.jpg" || first[2].(map[string]any)["UploadId"] != firstSame || first[2].(map[string]any)["StorageClass"] != "STANDARD_IA" || first[2].(map[string]any)["Initiated"] == "" || firstPage.Output["IsTruncated"] != true || firstPage.Output["NextKeyMarker"] != "same" || firstPage.Output["NextUploadIdMarker"] != firstSame {
		t.Fatalf("first multipart page = %v", firstPage.Output)
	}
	secondPage := mustInvoke(t, p, "ListMultipartUploads", map[string]any{
		"Bucket": "b", "KeyMarker": "same", "UploadIdMarker": firstSame, "MaxUploads": 3,
	}, nil)
	second := secondPage.Output["Uploads"].([]any)
	if len(second) != 2 || second[0].(map[string]any)["UploadId"] != secondSame || second[1].(map[string]any)["Key"] != "space key" || secondPage.Output["IsTruncated"] != false {
		t.Fatalf("second multipart page = %v", secondPage.Output)
	}
	grouped := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "b", "Prefix": "photos/", "Delimiter": "/"}, nil)
	groups := grouped.Output["CommonPrefixes"].([]any)
	if len(grouped.Output["Uploads"].([]any)) != 0 || len(groups) != 1 || groups[0].(map[string]any)["Prefix"] != "photos/2026/" {
		t.Fatalf("grouped multipart uploads = %v", grouped.Output)
	}
	encoded := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "b", "Prefix": "space", "EncodingType": "url"}, nil)
	if encoded.Output["Uploads"].([]any)[0].(map[string]any)["Key"] != "space%20key" || encoded.Output["EncodingType"] != "url" {
		t.Fatalf("encoded multipart uploads = %v", encoded.Output)
	}
	for _, test := range []struct {
		input      map[string]any
		code       string
		httpStatus int
	}{
		{map[string]any{"Bucket": "missing"}, "NoSuchBucket", http.StatusNotFound},
		{map[string]any{"Bucket": "b", "MaxUploads": 0}, "InvalidArgument", http.StatusBadRequest},
	} {
		_, err := invoke(t, p, "ListMultipartUploads", test.input, nil)
		if fault := asFault(t, err); fault.Code != test.code || fault.HTTPStatus != test.httpStatus {
			t.Fatalf("invalid multipart listing fault = %#v", fault)
		}
	}
}

func TestMultipartOperationsRejectMissingUpload(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "k"}, nil)
	uploadID := created.Output["UploadId"].(string)
	for _, operation := range []string{"UploadPart", "CompleteMultipartUpload", "ListParts", "AbortMultipartUpload"} {
		for _, input := range []map[string]any{
			{"Bucket": "b", "Key": "k", "UploadId": "missing", "PartNumber": 1},
			{"Bucket": "b", "Key": "wrong", "UploadId": uploadID, "PartNumber": 1},
			{"Bucket": "wrong", "Key": "k", "UploadId": uploadID, "PartNumber": 1},
		} {
			if operation == "CompleteMultipartUpload" {
				input["MultipartUpload"] = map[string]any{"Parts": []any{}}
			}
			_, err := invoke(t, p, operation, input, []byte("part"))
			if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.HTTPStatus != http.StatusNotFound {
				t.Fatalf("%s fault = %#v", operation, fault)
			}
		}
	}
	mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "k", "UploadId": uploadID}, nil)
}

func TestMultipartPartNumberBounds(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "b"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "b", "Key": "k"}, nil)
	uploadID := created.Output["UploadId"].(string)
	for _, input := range []map[string]any{
		{"UploadId": uploadID},
		{"UploadId": uploadID, "PartNumber": -1},
		{"UploadId": uploadID, "PartNumber": 0},
		{"UploadId": uploadID, "PartNumber": 10001},
	} {
		_, err := invoke(t, p, "UploadPart", input, []byte("part"))
		if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("UploadPart %#v fault = %#v", input, fault)
		}
	}
	last := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 10000}, []byte("last"))
	for _, number := range []int{0, 10001} {
		input := completeInput(uploadID, map[string]any{"PartNumber": number, "ETag": last.Headers.Get("ETag")})
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		if fault := asFault(t, err); fault.Code != "InvalidPart" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("complete part %d fault = %#v", number, fault)
		}
	}
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "b", "Key": "k", "UploadId": uploadID}, nil)
	if listed.Output["Parts"].([]any)[0].(map[string]any)["PartNumber"] != 10000 {
		t.Fatalf("valid boundary part = %v", listed.Output)
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

	mustInvoke(t, p, "PutBucketReplication", map[string]any{
		"Bucket": "source", "ReplicationConfiguration": map[string]any{"Rules": []any{map[string]any{
			"Status": "Enabled", "Filter": map[string]any{"Prefix": "plain/"},
			"Destination": map[string]any{"Bucket": "arn:aws:s3:::destination"},
		}}},
	}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "plain/file", "Tagging": "owner=mirror"}, []byte("tagged"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "plain/file"}, []byte("untagged"))
	if tags := asSliceForTest(mustInvokeAs(t, p, west, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "plain/file"}, nil).Output["TagSet"]); len(tags) != 0 {
		t.Fatalf("replica inherited overwritten tags: %#v", tags)
	}
}

func asMapForTest(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func asSliceForTest(value any) []any {
	result, _ := value.([]any)
	return result
}
