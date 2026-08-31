package chaos

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

type failBlobs struct {
	spi.BlobStore
	fail    bool
	failKey string
}

type failAfterReader struct {
	io.Reader
}

func (r failAfterReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return 0, errors.New("injected read failure")
	}
	return n, err
}

func (failAfterReader) Close() error { return nil }

func (f failBlobs) Put(ctx context.Context, key string, r io.Reader) (spi.BlobInfo, error) {
	if f.fail || f.failKey != "" && strings.Contains(key, f.failKey) {
		return spi.BlobInfo{}, errors.New("injected blob failure")
	}
	return f.BlobStore.Put(ctx, key, r)
}

func TestReplicaVersionBlobFailureLeavesNoPartialCurrent(t *testing.T) {
	deps := spitest.Deps(t)
	blobs := &failBlobs{BlobStore: deps.Blobs}
	deps.Blobs = blobs
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body []byte) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != nil {
			stream = io.NopCloser(bytes.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	for _, bucket := range []string{"source", "destination"} {
		if _, err := call("CreateBucket", map[string]any{"Bucket": bucket}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := call("PutBucketVersioning", map[string]any{"Bucket": bucket, "Status": "Enabled"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := call("PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": map[string]any{"Role": "arn:aws:iam::000000000000:role/replication", "Rules": []any{map[string]any{"Status": "Enabled", "Destination": map[string]any{"Bucket": "destination"}}}}}, nil); err != nil {
		t.Fatal(err)
	}
	blobs.failKey = "destination/key@"
	put, err := call("PutObject", map[string]any{"Bucket": "source", "Key": "key"}, []byte("body"))
	if err != nil || put.Headers.Get("x-amz-replication-status") != "FAILED" {
		t.Fatalf("source put: %#v %v", put, err)
	}
	if _, err := call("GetObject", map[string]any{"Bucket": "destination", "Key": "key"}, nil); err == nil {
		t.Fatal("failed version replication left a partial current object")
	}
}

func TestConcurrentBodyReadFailuresLeaveNoPartialObjects(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "read-failures"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("object-%d", i)
			var body io.ReadCloser = io.NopCloser(strings.NewReader("complete"))
			if i%2 != 0 {
				body = failAfterReader{Reader: strings.NewReader("partial")}
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "read-failures", "Key": key}, Body: body})
			if i%2 == 0 && err != nil || i%2 != 0 && err == nil {
				errs <- fmt.Errorf("put %d: %v", i, err)
				return
			}
			get, getErr := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{"Bucket": "read-failures", "Key": key}})
			if i%2 != 0 {
				if getErr == nil {
					errs <- fmt.Errorf("failed put %d left object", i)
				}
				return
			}
			if getErr != nil {
				errs <- fmt.Errorf("get %d: %v", i, getErr)
				return
			}
			data, _ := io.ReadAll(get.Stream)
			_ = get.Stream.Close()
			if string(data) != "complete" {
				errs <- fmt.Errorf("get %d body %q", i, data)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestConcurrentS3ResponseIDsRemainDistinct(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	ids := make(chan string, 64)
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := http.Get(fmt.Sprintf("%s/missing-response-id-%d/key", ts.URL, i))
			if err != nil {
				errs <- err
				return
			}
			response.Body.Close()
			requestID := response.Header.Get("x-amz-request-id")
			if response.StatusCode != http.StatusNotFound || requestID == "" || response.Header.Get("x-amz-id-2") == "" {
				errs <- fmt.Errorf("response %d headers %#v", response.StatusCode, response.Header)
				return
			}
			ids <- requestID
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate request ID %q", id)
		}
		seen[id] = true
	}
	if len(seen) != 64 {
		t.Fatalf("unique request IDs = %d, want 64", len(seen))
	}
}

func TestConcurrentSigV4AUnsignedTrailersDoNotCrossContaminate(t *testing.T) {
	deps := spitest.Deps(t)
	if err := deps.Clock.Advance(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).Sub(deps.Clock.Now())); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.S3ValidatePresignedSignatures = true
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, httptest.NewRequest(http.MethodPut, "/v4a-unsigned", nil))
	if created.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", created.Code, created.Body.String())
	}

	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			extension := ""
			want := http.StatusOK
			if i%2 != 0 {
				extension = ";chunk-signature=unexpected"
				want = http.StatusForbidden
			}
			raw := "5" + extension + "\r\nhello\r\n0\r\nx-amz-checksum-crc32c:mnG7TA==\r\n\r\n"
			request := httptest.NewRequest(http.MethodPut, "/v4a-unsigned/object", strings.NewReader(raw))
			request.Host = "s3.localhost.localstack.cloud:4566"
			request.Header.Set("Content-Encoding", "aws-chunked")
			request.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			request.Header.Set("X-Amz-Date", "20990101T000000Z")
			request.Header.Set("X-Amz-Decoded-Content-Length", "5")
			request.Header.Set("X-Amz-Region-Set", "us-east-1")
			request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
			request.Header.Set("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=test/20990101/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-region-set;x-amz-trailer,Signature=304402201f09d982734f868ab87f6e305473f7ef74a6882095dbf5d0f0b97bede169993402204a4c59017095e2ffaf861e04fc6c73b5d1c9b0d8c041b7fd2acb05d0a4c356f3")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != want {
				errs <- fmt.Errorf("request %d status %d, want %d", i, recorder.Code, want)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestConcurrentMalformedAWSChunksDoNotCrossContaminate(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, httptest.NewRequest(http.MethodPut, "/chunk-errors", nil))
	if created.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", created.Code, created.Body.String())
	}

	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			raw := "5\r\nhello\r\n0\r\n\r\n"
			want := http.StatusOK
			if i%2 != 0 {
				raw = "5\r\nhello\r\n"
				want = http.StatusForbidden
			}
			request := httptest.NewRequest(http.MethodPut, "/chunk-errors/object", strings.NewReader(raw))
			request.Header.Set("Content-Encoding", "aws-chunked")
			request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			request.Header.Set("X-Amz-Decoded-Content-Length", "5")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != want {
				errs <- fmt.Errorf("request %d status %d, want %d", i, recorder.Code, want)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestConcurrentAWSChunkedContentEncodingsDoNotCrossContaminate(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, httptest.NewRequest(http.MethodPut, "/chunk-encodings", nil))
	if created.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", created.Code, created.Body.String())
	}

	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			encoding, want := "aws-chunked", ""
			if i%2 != 0 {
				encoding, want = "gzip, aws-chunked", "gzip"
			}
			path := fmt.Sprintf("/chunk-encodings/object-%d", i)
			request := httptest.NewRequest(http.MethodPut, path, strings.NewReader("5\r\nhello\r\n0\r\n\r\n"))
			request.Header.Set("Content-Encoding", encoding)
			request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			request.Header.Set("X-Amz-Decoded-Content-Length", "5")
			put := httptest.NewRecorder()
			handler.ServeHTTP(put, request)
			if put.Code != http.StatusOK {
				errs <- fmt.Errorf("put %d: %d %s", i, put.Code, put.Body.String())
				return
			}
			get := httptest.NewRecorder()
			handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, path, nil))
			if get.Code != http.StatusOK || get.Body.String() != "hello" || get.Header().Get("Content-Encoding") != want {
				errs <- fmt.Errorf("get %d: %d encoding=%q body=%q", i, get.Code, get.Header().Get("Content-Encoding"), get.Body.String())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestConcurrentCrossRegionBucketResolution(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	east := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body []byte) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != nil {
			stream = io.NopCloser(bytes.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: east, Operation: operation, Input: input, Body: stream})
	}
	if _, err := call("CreateBucket", map[string]any{"Bucket": "cross-region-chaos", "LocationConstraint": "us-west-2"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutObject", map[string]any{"Bucket": "cross-region-chaos", "Key": "key"}, []byte("body")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			operation := "HeadBucket"
			input := map[string]any{"Bucket": "cross-region-chaos"}
			if i%2 != 0 {
				operation = "ListObjectsV2"
			}
			response, err := call(operation, input, nil)
			if err != nil {
				errs <- err
				return
			}
			if response.Headers.Get("x-amz-bucket-region") != "us-west-2" {
				errs <- fmt.Errorf("%s region headers: %#v", operation, response.Headers)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestRejectedReplicationConfigurationPreservesCurrent(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
	}
	for _, bucket := range []string{"source", "destination"} {
		if _, err := call("CreateBucket", map[string]any{"Bucket": bucket}); err != nil {
			t.Fatal(err)
		}
		if _, err := call("PutBucketVersioning", map[string]any{"Bucket": bucket, "Status": "Enabled"}); err != nil {
			t.Fatal(err)
		}
	}
	valid := map[string]any{"Role": "role", "Rules": []any{map[string]any{"Status": "Enabled", "Destination": map[string]any{"Bucket": "destination"}}}}
	if _, err := call("PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": valid}); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Suspended"}); err != nil {
		t.Fatal(err)
	}
	_, err := call("PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": valid})
	var fault *spi.Fault
	if !errors.As(err, &fault) || fault.Code != "InvalidRequest" {
		t.Fatalf("unversioned replacement: %v", err)
	}
	if _, err := call("PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Enabled"}); err != nil {
		t.Fatal(err)
	}
	invalid := map[string]any{"Role": "role", "Rules": []any{map[string]any{
		"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Tag": map[string]any{"Key": "stage", "Value": "test"}},
		"DeleteMarkerReplication": map[string]any{"Status": "Enabled"}, "Destination": map[string]any{"Bucket": "destination"},
	}}}
	_, err = call("PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": invalid})
	if !errors.As(err, &fault) || fault.Code != "InvalidRequest" {
		t.Fatalf("invalid replacement: %v", err)
	}
	got, err := call("GetBucketReplication", map[string]any{"Bucket": "source"})
	if err != nil || !reflect.DeepEqual(got.Output["ReplicationConfiguration"], valid) {
		t.Fatalf("stored configuration changed: %#v %v", got, err)
	}
}

func TestConcurrentPutsSameKey(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "bucket"}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			body := bytes.Repeat([]byte{byte(n)}, 16)
			_, _ = p.Invoke(ctx, &spi.Request{
				Identity: id, Operation: "PutObject",
				Input: map[string]any{"Bucket": "bucket", "Key": "k"},
				Body:  io.NopCloser(bytes.NewReader(body)),
			})
		}(i)
	}
	wg.Wait()
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{"Bucket": "bucket", "Key": "k"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Stream == nil {
		t.Fatal("missing body after concurrent puts")
	}
	_ = got.Stream.Close()
}

func TestConcurrentArchiveRestoresConverge(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body []byte) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != nil {
			stream = io.NopCloser(bytes.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	if _, err := call("CreateBucket", map[string]any{"Bucket": "archive"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutObject", map[string]any{"Bucket": "archive", "Key": "cold", "StorageClass": "GLACIER"}, []byte("body")); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := call("RestoreObject", map[string]any{"Bucket": "archive", "Key": "cold", "RestoreRequest": map[string]any{"Days": 1}}, nil)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent restore: %v", err)
		}
	}
	got, err := call("GetObject", map[string]any{"Bucket": "archive", "Key": "cold"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Stream)
	_ = got.Stream.Close()
	if string(body) != "body" || got.Headers.Get("x-amz-restore") == "" {
		t.Fatalf("restore did not converge body=%q headers=%v", body, got.Headers)
	}
}

func TestConcurrentInvalidWritesLeaveNoObject(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "classes"}}); err != nil {
		t.Fatal(err)
	}

	type result struct {
		err  error
		want string
	}
	results := make(chan result, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			input := map[string]any{"Bucket": "classes", "Key": "object", "StorageClass": "INVALID"}
			want := "InvalidStorageClass"
			if n%2 != 0 {
				input = map[string]any{"Bucket": "classes", "Key": strings.Repeat("x", 1025)}
				want = "KeyTooLongError"
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: input, Body: io.NopCloser(bytes.NewReader([]byte("bad")))})
			results <- result{err: err, want: want}
		}(i)
	}
	wg.Wait()
	close(results)
	for result := range results {
		var fault *spi.Fault
		if !errors.As(result.err, &fault) || fault.Code != result.want {
			t.Fatalf("invalid write want %s: %v", result.want, result.err)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "HeadObject", Input: map[string]any{"Bucket": "classes", "Key": "object"}}); err == nil {
		t.Fatal("invalid storage classes created an object")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "HeadObject", Input: map[string]any{"Bucket": "classes", "Key": strings.Repeat("x", 1025)}}); err == nil {
		t.Fatal("oversized keys created an object")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "classes", "Key": "object", "StorageClass": "STANDARD_IA"}, Body: io.NopCloser(bytes.NewReader([]byte("good")))}); err != nil {
		t.Fatalf("valid write after invalid load: %v", err)
	}
}

func TestCustomerEncryptionValidationFailurePreservesObject(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body []byte) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != nil {
			stream = io.NopCloser(bytes.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	if _, err := call("CreateBucket", map[string]any{"Bucket": "customer-encryption"}, nil); err != nil {
		t.Fatal(err)
	}
	rawKey := []byte("0123456789abcdef0123456789abcdef")
	digest := md5.Sum(rawKey)
	input := map[string]any{"Bucket": "customer-encryption", "Key": "object", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": base64.StdEncoding.EncodeToString(rawKey), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(digest[:])}
	if _, err := call("PutObject", input, []byte("original")); err != nil {
		t.Fatal(err)
	}
	invalid := map[string]any{"Bucket": "customer-encryption", "Key": "object", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": input["SSECustomerKey"], "SSECustomerKeyMD5": "AAAAAAAAAAAAAAAAAAAAAA=="}
	if _, err := call("PutObject", invalid, []byte("replacement")); err == nil {
		t.Fatal("invalid replacement accepted")
	}
	got, err := call("GetObject", input, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Stream)
	_ = got.Stream.Close()
	if string(body) != "original" {
		t.Fatalf("validation failure replaced object with %q", body)
	}
}

func TestCustomerEncryptedCopyValidationFailurePreservesDestination(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body []byte) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != nil {
			stream = io.NopCloser(bytes.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	_, _ = call("CreateBucket", map[string]any{"Bucket": "copy-sse-c"}, nil)
	rawKey := []byte("0123456789abcdef0123456789abcdef")
	digest := md5.Sum(rawKey)
	key, keyMD5 := base64.StdEncoding.EncodeToString(rawKey), base64.StdEncoding.EncodeToString(digest[:])
	if _, err := call("PutObject", map[string]any{"Bucket": "copy-sse-c", "Key": "source", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": key, "SSECustomerKeyMD5": keyMD5}, []byte("source")); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutObject", map[string]any{"Bucket": "copy-sse-c", "Key": "destination"}, []byte("original")); err != nil {
		t.Fatal(err)
	}
	invalid := map[string]any{"Bucket": "copy-sse-c", "Key": "destination", "CopySource": "copy-sse-c/source", "CopySourceSSECustomerAlgorithm": "AES256", "CopySourceSSECustomerKey": key, "CopySourceSSECustomerKeyMD5": "AAAAAAAAAAAAAAAAAAAAAA=="}
	if _, err := call("CopyObject", invalid, nil); err == nil {
		t.Fatal("copy with invalid source key accepted")
	}
	got, err := call("GetObject", map[string]any{"Bucket": "copy-sse-c", "Key": "destination"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Stream)
	_ = got.Stream.Close()
	if string(body) != "original" {
		t.Fatalf("failed copy replaced destination with %q", body)
	}
}

func TestConcurrentXXHashValidationPreservesObject(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "xxhash"}}); err != nil {
		t.Fatal(err)
	}
	body := []byte("123456789")
	results := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func(valid bool) {
			defer wg.Done()
			checksum := "AA=="
			if valid {
				checksum = "jLhB20DmroM="
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "xxhash", "Key": "object", "ChecksumXXHASH64": checksum}, Body: io.NopCloser(bytes.NewReader(body))})
			results <- err
		}(i%2 == 0)
	}
	wg.Wait()
	close(results)
	succeeded, rejected := 0, 0
	for err := range results {
		if err == nil {
			succeeded++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "BadDigest" {
			t.Fatalf("concurrent put: %v", err)
		}
		rejected++
	}
	if succeeded != 16 || rejected != 16 {
		t.Fatalf("successful=%d rejected=%d", succeeded, rejected)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{"Bucket": "xxhash", "Key": "object", "ChecksumMode": "ENABLED"}})
	if err != nil {
		t.Fatal(err)
	}
	stored, _ := io.ReadAll(got.Stream)
	_ = got.Stream.Close()
	if !bytes.Equal(stored, body) || got.Headers.Get("x-amz-checksum-xxhash64") != "jLhB20DmroM=" {
		t.Fatalf("stored body=%q headers=%v", stored, got.Headers)
	}
}

func TestConcurrentNonEmptyBucketDeletesAreRejected(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "non-empty"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "non-empty", "Key": "object"}, Body: io.NopCloser(bytes.NewReader([]byte("body")))}); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteBucket", Input: map[string]any{"Bucket": "non-empty"}})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "BucketNotEmpty" {
			t.Fatalf("concurrent delete: %v", err)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{"Bucket": "non-empty", "Key": "object"}}); err != nil {
		t.Fatalf("object after concurrent deletes: %v", err)
	}
}

func TestConcurrentBucketCreationHasOneOwner(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	accounts := []string{"111111111111", "222222222222"}
	type result struct {
		account string
		err     error
	}
	results := make(chan result, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			account := accounts[n%len(accounts)]
			_, err := p.Invoke(ctx, &spi.Request{Identity: spi.Identity{Account: account, Region: "us-east-1"}, Operation: "CreateBucket", Input: map[string]any{"Bucket": "global-name"}})
			results <- result{account: account, err: err}
		}(i)
	}
	wg.Wait()
	close(results)
	winners := map[string]bool{}
	for result := range results {
		if result.err == nil {
			winners[result.account] = true
			continue
		}
		var fault *spi.Fault
		if !errors.As(result.err, &fault) || fault.Code != "BucketAlreadyExists" {
			t.Fatalf("create %s: %v", result.account, result.err)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("bucket owners = %#v", winners)
	}
}

func TestConcurrentAccountRegionalBucketCreationHasOneWinner(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-west-2"}
	input := map[string]any{"Bucket": "regional-111111111111-us-west-2-an", "BucketNamespace": "account-regional", "LocationConstraint": id.Region}
	errCh := make(chan error, 32)
	var wg sync.WaitGroup
	for range cap(errCh) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: input})
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	winners := 0
	for err := range errCh {
		if err == nil {
			winners++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "BucketAlreadyOwnedByYou" {
			t.Fatalf("concurrent create: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("successful creates = %d, want 1", winners)
	}
}

func TestConcurrentCrossRegionBucketsPaginateWithoutAccountLeaks(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	accounts := []string{"111111111111", "222222222222"}
	regions := []string{"us-east-1", "us-west-2"}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			account, region := accounts[n%2], regions[(n/2)%2]
			input := map[string]any{"Bucket": fmt.Sprintf("list-%c-%02d", 'a'+rune(n%2), n/2)}
			if region != "us-east-1" {
				input["LocationConstraint"] = region
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: spi.Identity{Account: account, Region: region}, Operation: "CreateBucket", Input: input})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent create: %v", err)
		}
	}

	id := spi.Identity{Account: accounts[0], Region: regions[0]}
	token := ""
	seen := map[string]bool{}
	for {
		input := map[string]any{"MaxBuckets": 3, "Prefix": "list-"}
		if token != "" {
			input["ContinuationToken"] = token
		}
		page, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListBuckets", Input: input})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page.Output["Buckets"].([]any) {
			bucket := item.(map[string]any)
			name, region := bucket["Name"].(string), bucket["BucketRegion"].(string)
			if !strings.HasPrefix(name, "list-a-") || (region != "us-east-1" && region != "us-west-2") || seen[name] {
				t.Fatalf("leaked or duplicate bucket: %#v", bucket)
			}
			seen[name] = true
		}
		next, _ := page.Output["ContinuationToken"].(string)
		if next == "" {
			break
		}
		if next == token {
			t.Fatalf("pagination did not advance: %q", token)
		}
		token = next
	}
	if len(seen) != 16 {
		t.Fatalf("listed %d buckets, want 16", len(seen))
	}
}

func TestConcurrentInvalidBucketLocationsDoNotReserveName(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-west-2"}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			input := map[string]any{"Bucket": "regional-name"}
			if n%2 != 0 {
				input["LocationConstraint"] = "eu-west-1"
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: input})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "IllegalLocationConstraintException" {
			t.Fatalf("invalid regional create: %v", err)
		}
	}
	if got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "regional-name", "LocationConstraint": "us-west-2"}}); err != nil || got.Headers.Get("Location") != "http://regional-name.s3.amazonaws.com/" {
		t.Fatalf("valid create after invalid load: %#v %v", got, err)
	}
}

func TestConcurrentInvalidBucketNamesDoNotReserveState(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	names := []string{"ab", "UPPERCASE", "192.168.5.4", "reserved--x-s3"}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": names[n%len(names)]}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "InvalidBucketName" {
			t.Fatalf("invalid bucket create: %v", err)
		}
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "valid-after-invalid-names"}}); err != nil {
		t.Fatalf("valid create after invalid load: %v", err)
	}
}

func TestConcurrentCreateBucketTagsRemainAtomic(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	valid := []any{map[string]any{"Key": "team", "Value": "storage"}}
	invalid := []any{map[string]any{"Key": "duplicate", "Value": "one"}, map[string]any{"Key": "duplicate", "Value": "two"}}
	type result struct {
		valid bool
		err   error
	}
	results := make(chan result, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tags := invalid
			if n%2 == 0 {
				tags = valid
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{
				"Bucket": "atomic-create-tags", "CreateBucketConfiguration": map[string]any{"Tags": tags},
			}})
			results <- result{valid: n%2 == 0, err: err}
		}(i)
	}
	wg.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result.err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(result.err, &fault) || result.valid && fault.Code != "BucketAlreadyOwnedByYou" || !result.valid && fault.Code != "InvalidTag" {
			t.Fatalf("concurrent create valid=%t: %v", result.valid, result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful creates = %d, want 1", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketTagging", Input: map[string]any{"Bucket": "atomic-create-tags"}})
	if err != nil || !reflect.DeepEqual(response.Output["TagSet"], valid) {
		t.Fatalf("persisted create tags = %#v %v", response, err)
	}
}

func TestConcurrentCreateBucketOwnershipRemainsAtomic(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	bucket := "atomic-ownership-" + id.Account + "-" + id.Region + "-an"
	type result struct {
		ownership string
		err       error
	}
	results := make(chan result, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ownership := "invalid"
			if n%2 == 0 {
				ownership = []string{"BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced"}[n%3]
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{
				"Bucket": bucket, "BucketNamespace": "account-regional", "ObjectOwnership": ownership,
			}})
			results <- result{ownership: ownership, err: err}
		}(i)
	}
	wg.Wait()
	close(results)
	successes, winner := 0, ""
	for result := range results {
		if result.err == nil {
			successes++
			winner = result.ownership
			continue
		}
		var fault *spi.Fault
		valid := result.ownership != "invalid"
		if !errors.As(result.err, &fault) || valid && fault.Code != "BucketAlreadyOwnedByYou" || !valid && fault.Code != "InvalidArgument" && fault.Code != "BucketAlreadyOwnedByYou" {
			t.Fatalf("concurrent create ownership=%s: %v", result.ownership, result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful creates = %d, want 1", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketOwnershipControls", Input: map[string]any{"Bucket": bucket}})
	if err != nil {
		t.Fatal(err)
	}
	controls, _ := response.Output["OwnershipControls"].(map[string]any)
	rules, _ := controls["Rules"].([]any)
	if len(rules) != 1 || rules[0].(map[string]any)["ObjectOwnership"] != winner {
		t.Fatalf("persisted create ownership = %#v, want %s", response, winner)
	}
}

func TestConcurrentBucketOwnershipControlsRemainValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "ownership-controls-chaos"}}); err != nil {
		t.Fatal(err)
	}
	valid := map[string]bool{"BucketOwnerPreferred": true, "ObjectWriter": true, "BucketOwnerEnforced": true}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ownership := "invalid"
			if n%2 == 0 {
				ownership = []string{"BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced"}[n%3]
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketOwnershipControls", Input: map[string]any{
				"Bucket": "ownership-controls-chaos", "OwnershipControls": map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": ownership}}},
			}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MalformedXML" {
			t.Fatalf("concurrent ownership-controls put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful ownership-controls puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketOwnershipControls", Input: map[string]any{"Bucket": "ownership-controls-chaos"}})
	if err != nil {
		t.Fatal(err)
	}
	controls, _ := response.Output["OwnershipControls"].(map[string]any)
	rules, _ := controls["Rules"].([]any)
	if len(rules) != 1 || !valid[rules[0].(map[string]any)["ObjectOwnership"].(string)] {
		t.Fatalf("persisted concurrent ownership controls = %#v", response.Output)
	}
}

func TestConcurrentPublicAccessBlockRemainsValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "public-access-block-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			configuration := map[string]any{"Unknown": true}
			if n%2 == 0 {
				configuration = map[string]any{"BlockPublicAcls": n%4 == 0}
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutPublicAccessBlock", Input: map[string]any{"Bucket": "public-access-block-chaos", "PublicAccessBlockConfiguration": configuration}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MalformedXML" {
			t.Fatalf("concurrent public-access-block put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful public-access-block puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetPublicAccessBlock", Input: map[string]any{"Bucket": "public-access-block-chaos"}})
	if err != nil {
		t.Fatal(err)
	}
	configuration, _ := response.Output["PublicAccessBlockConfiguration"].(map[string]any)
	for _, field := range []string{"BlockPublicAcls", "BlockPublicPolicy", "IgnorePublicAcls", "RestrictPublicBuckets"} {
		if _, ok := configuration[field].(bool); !ok {
			t.Fatalf("persisted public access block = %#v", response.Output)
		}
	}
}

func TestConcurrentBucketRequestPaymentRemainsValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "request-payment-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			payer := "Invalid"
			if n%2 == 0 {
				payer = []string{"Requester", "BucketOwner"}[(n/2)%2]
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketRequestPayment", Input: map[string]any{"Bucket": "request-payment-chaos", "RequestPaymentConfiguration": map[string]any{"Payer": payer}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MalformedXML" {
			t.Fatalf("concurrent request-payment put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful request-payment puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketRequestPayment", Input: map[string]any{"Bucket": "request-payment-chaos"}})
	if err != nil || response.Output["Payer"] != "Requester" && response.Output["Payer"] != "BucketOwner" {
		t.Fatalf("persisted request payer = %#v, err=%v", response, err)
	}
}

func TestConcurrentBucketAccelerationRemainsValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "accelerate-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			status := "Invalid"
			if n%2 == 0 {
				status = []string{"Enabled", "Suspended"}[(n/2)%2]
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketAccelerateConfiguration", Input: map[string]any{"Bucket": "accelerate-chaos", "AccelerateConfiguration": map[string]any{"Status": status}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MalformedXML" {
			t.Fatalf("concurrent accelerate put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful accelerate puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketAccelerateConfiguration", Input: map[string]any{"Bucket": "accelerate-chaos"}})
	if err != nil || response.Output["Status"] != "Enabled" && response.Output["Status"] != "Suspended" {
		t.Fatalf("persisted acceleration = %#v, err=%v", response, err)
	}
}

func TestConcurrentBucketLoggingRemainsValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	for _, bucket := range []string{"logging-chaos-source", "logging-chaos-target"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": bucket}}); err != nil {
			t.Fatal(err)
		}
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			logging := map[string]any{"TargetBucket": "missing"}
			if n%2 == 0 {
				logging = map[string]any{"TargetBucket": "logging-chaos-target", "TargetPrefix": fmt.Sprintf("logs/%d/", n)}
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketLogging", Input: map[string]any{"Bucket": "logging-chaos-source", "BucketLoggingStatus": map[string]any{"LoggingEnabled": logging}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "InvalidTargetBucketForLogging" {
			t.Fatalf("concurrent logging put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful logging puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketLogging", Input: map[string]any{"Bucket": "logging-chaos-source"}})
	logging, _ := response.Output["LoggingEnabled"].(map[string]any)
	if err != nil || logging["TargetBucket"] != "logging-chaos-target" || !strings.HasPrefix(logging["TargetPrefix"].(string), "logs/") {
		t.Fatalf("persisted concurrent logging = %#v, err=%v", response, err)
	}
}

func TestConcurrentBucketCorsRemainsValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "cors-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			method := "OPTIONS"
			if n%2 == 0 {
				method = []string{"GET", "HEAD"}[(n/2)%2]
			}
			rules := []any{map[string]any{"AllowedMethods": []any{method}, "AllowedOrigins": []any{"*"}, "ID": fmt.Sprintf("rule-%d", n)}}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketCors", Input: map[string]any{"Bucket": "cors-chaos", "CORSConfiguration": map[string]any{"CORSRules": rules}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "InvalidRequest" {
			t.Fatalf("concurrent CORS put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful CORS puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketCors", Input: map[string]any{"Bucket": "cors-chaos"}})
	rules, _ := response.Output["CORSRules"].([]any)
	var methods []any
	if len(rules) == 1 {
		methods, _ = rules[0].(map[string]any)["AllowedMethods"].([]any)
	}
	if err != nil || len(rules) != 1 || len(methods) != 1 || methods[0] != "GET" && methods[0] != "HEAD" {
		t.Fatalf("persisted concurrent CORS = %#v, err=%v", response, err)
	}
}

func TestConcurrentBucketWebsiteRemainsValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "website-chaos"}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i += 2 {
		key := fmt.Sprintf("index-%d.html", i)
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "website-chaos", "Key": key}, Body: io.NopCloser(strings.NewReader(key))}); err != nil {
			t.Fatal(err)
		}
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			suffix := "dir/index.html"
			if n%2 == 0 {
				suffix = fmt.Sprintf("index-%d.html", n)
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketWebsite", Input: map[string]any{"Bucket": "website-chaos", "WebsiteConfiguration": map[string]any{"IndexDocument": map[string]any{"Suffix": suffix}}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "InvalidArgument" {
			t.Fatalf("concurrent website put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful website puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketWebsite", Input: map[string]any{"Bucket": "website-chaos"}})
	index, _ := response.Output["IndexDocument"].(map[string]any)
	suffix, _ := index["Suffix"].(string)
	if err != nil || !strings.HasPrefix(suffix, "index-") || strings.Contains(suffix, "/") {
		t.Fatalf("persisted concurrent website = %#v, err=%v", response, err)
	}
	errs = make(chan error, 64)
	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			suffix := fmt.Sprintf("index-%d.html", n*2%32)
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketWebsite", Input: map[string]any{"Bucket": "website-chaos", "WebsiteConfiguration": map[string]any{"IndexDocument": map[string]any{"Suffix": suffix}}}})
			errs <- err
		}(i)
		go func() {
			defer wg.Done()
			httpRequest := httptest.NewRequest(http.MethodGet, "http://website-chaos.s3-website.localhost.localstack.cloud/", nil)
			response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{}, HTTP: httpRequest})
			if err == nil && response.Status == http.StatusOK {
				body, readErr := io.ReadAll(response.Stream)
				_ = response.Stream.Close()
				if readErr != nil || !strings.HasPrefix(string(body), "index-") {
					err = fmt.Errorf("website body %q: %v", body, readErr)
				}
			} else if err == nil {
				err = fmt.Errorf("website status %d", response.Status)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentBucketLifecycleRemainsValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "lifecycle-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			filter := map[string]any{"Prefix": fmt.Sprintf("objects/%d/", n)}
			if n%2 != 0 {
				filter["ObjectSizeGreaterThan"] = n
			}
			rules := []any{map[string]any{"ID": fmt.Sprintf("rule-%d", n), "Filter": filter, "Status": "Enabled", "Expiration": map[string]any{"Days": 1}}}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketLifecycleConfiguration", Input: map[string]any{"Bucket": "lifecycle-chaos", "LifecycleConfiguration": map[string]any{"Rules": rules}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MalformedXML" {
			t.Fatalf("concurrent lifecycle put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful lifecycle puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketLifecycleConfiguration", Input: map[string]any{"Bucket": "lifecycle-chaos"}})
	rules, _ := response.Output["Rules"].([]any)
	if err != nil || len(rules) != 1 || !strings.HasPrefix(rules[0].(map[string]any)["ID"].(string), "rule-") {
		t.Fatalf("persisted concurrent lifecycle = %#v, err=%v", response, err)
	}
}

func TestConcurrentNamedBucketConfigurationsRemainValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "named-configuration-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			configurationID := fmt.Sprintf("configuration-%02d", n%8)
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketAnalyticsConfiguration", Input: map[string]any{
				"Bucket": "named-configuration-chaos", "Id": configurationID,
				"AnalyticsConfiguration": map[string]any{"Id": configurationID, "Filter": map[string]any{"Prefix": fmt.Sprintf("objects/%02d/", n)}},
			}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent named configuration put: %v", err)
		}
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListBucketAnalyticsConfigurations", Input: map[string]any{"Bucket": "named-configuration-chaos"}})
	configurations, _ := response.Output["AnalyticsConfigurationList"].([]any)
	if err != nil || len(configurations) != 8 {
		t.Fatalf("persisted concurrent named configurations = %#v, err=%v", response, err)
	}
	for index, value := range configurations {
		configuration, _ := value.(map[string]any)
		if configuration["Id"] != fmt.Sprintf("configuration-%02d", index) {
			t.Fatalf("configuration order = %#v", configurations)
		}
	}
}

func TestConcurrentBucketPolicyWritesRemainValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "policy-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			policy := fmt.Sprintf(`{"Statement":[{"Sid":"policy-%02d"}]}`, n)
			if n%2 != 0 {
				policy = " " + policy
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketPolicy", Input: map[string]any{"Bucket": "policy-chaos", "Policy": policy}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MalformedPolicy" {
			t.Fatalf("concurrent policy put: %v", err)
		}
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketPolicy", Input: map[string]any{"Bucket": "policy-chaos"}})
	policy, _ := response.Output["Policy"].(string)
	var document map[string]any
	if err != nil || successes != 16 || json.Unmarshal([]byte(policy), &document) != nil || !strings.Contains(policy, `"Sid":"policy-`) {
		t.Fatalf("persisted concurrent policy = %q, successes=%d, err=%v", policy, successes, err)
	}
}

func TestConcurrentBucketEncryptionWritesRemainValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "encryption-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			rule := map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "AES256"}}
			rules := []any{rule}
			if n%2 != 0 {
				rules = append(rules, rule)
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketEncryption", Input: map[string]any{"Bucket": "encryption-chaos", "ServerSideEncryptionConfiguration": map[string]any{"Rules": rules}}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MalformedXML" {
			t.Fatalf("concurrent encryption put: %v", err)
		}
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketEncryption", Input: map[string]any{"Bucket": "encryption-chaos"}})
	rules, _ := response.Output["Rules"].([]any)
	if err != nil || successes != 16 || len(rules) != 1 || rules[0].(map[string]any)["ApplyServerSideEncryptionByDefault"].(map[string]any)["SSEAlgorithm"] != "AES256" {
		t.Fatalf("persisted concurrent encryption = %#v, successes=%d, err=%v", response, successes, err)
	}
}

func TestConcurrentObjectACLWritesRemainValid(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "acl-chaos"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "acl-chaos", "Key": "object"}, Body: io.NopCloser(strings.NewReader("body"))}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			acl := "public-read"
			if n%2 != 0 {
				acl = "invalid"
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObjectAcl", Input: map[string]any{"Bucket": "acl-chaos", "Key": "object", "ACL": acl}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "InvalidArgument" {
			t.Fatalf("concurrent ACL put: %v", err)
		}
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObjectAcl", Input: map[string]any{"Bucket": "acl-chaos", "Key": "object"}})
	grants, _ := response.Output["Grants"].([]any)
	if err != nil || successes != 16 || len(grants) != 2 {
		t.Fatalf("persisted concurrent ACL = %#v, successes=%d, err=%v", response, successes, err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketVersioning", Input: map[string]any{"Bucket": "acl-chaos", "Status": "Enabled"}}); err != nil {
		t.Fatal(err)
	}
	marker, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteObject", Input: map[string]any{"Bucket": "acl-chaos", "Key": "object"}})
	if err != nil || marker.Headers.Get("x-amz-delete-marker") != "true" {
		t.Fatalf("create ACL delete marker: %#v %v", marker, err)
	}
	errs = make(chan error, 32)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			operation := "GetObjectAcl"
			input := map[string]any{"Bucket": "acl-chaos", "Key": "object"}
			if n%2 == 0 {
				operation, input["ACL"] = "PutObjectAcl", "private"
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "MethodNotAllowed" && fault.Code != "NoSuchKey" {
			t.Fatalf("concurrent delete-marker ACL: %v", err)
		}
	}
}

func TestConcurrentBucketNotificationsRemainValid(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "notification-chaos"}}); err != nil {
		t.Fatal(err)
	}
	if err := deps.Store.Scope(id.Account, id.Region).Collection("queues").Put(ctx, "queue", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := "missing"
			if n%2 == 0 {
				name = "queue"
			}
			configuration := map[string]any{"QueueConfigurations": []any{map[string]any{"Id": fmt.Sprintf("rule-%d", n), "QueueArn": "arn:aws:sqs:us-east-1:111111111111:" + name, "Events": []any{"s3:ObjectCreated:*"}}}}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketNotificationConfiguration", Input: map[string]any{"Bucket": "notification-chaos", "NotificationConfiguration": configuration}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var fault *spi.Fault
		if !errors.As(err, &fault) || fault.Code != "InvalidArgument" {
			t.Fatalf("concurrent notification put: %v", err)
		}
	}
	if successes != 16 {
		t.Fatalf("successful notification puts = %d, want 16", successes)
	}
	response, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketNotificationConfiguration", Input: map[string]any{"Bucket": "notification-chaos"}})
	configurations, _ := response.Output["QueueConfigurations"].([]any)
	configuration := map[string]any{}
	if len(configurations) == 1 {
		configuration, _ = configurations[0].(map[string]any)
	}
	arn, _ := configuration["QueueArn"].(string)
	if err != nil || len(configurations) != 1 || !strings.HasSuffix(arn, ":queue") {
		t.Fatalf("persisted concurrent notifications = %#v, err=%v", response, err)
	}
}

func TestConcurrentInvalidVersioningWritesDoNotChangeState(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "versioning-state"}}); err != nil {
		t.Fatal(err)
	}

	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			status := ""
			if n%2 != 0 {
				status = "Invalid"
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketVersioning", Input: map[string]any{"Bucket": "versioning-state", "Status": status}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		var fault *spi.Fault
		if !errors.As(err, &fault) || (fault.Code != "IllegalVersioningConfigurationException" && fault.Code != "MalformedXML") {
			t.Fatalf("invalid versioning write: %v", err)
		}
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetBucketVersioning", Input: map[string]any{"Bucket": "versioning-state"}})
	if err != nil || len(got.Output) != 0 {
		t.Fatalf("versioning after invalid load = %#v, %v", got, err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutBucketVersioning", Input: map[string]any{"Bucket": "versioning-state", "Status": "Enabled"}}); err != nil {
		t.Fatalf("valid versioning write after invalid load: %v", err)
	}
}

func TestConcurrentVersionRestoration(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := context.Background()
	id := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body string) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: io.NopCloser(strings.NewReader(body))})
	}
	if _, err := call("CreateBucket", map[string]any{"Bucket": "version-restore"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutBucketVersioning", map[string]any{"Bucket": "version-restore", "Status": "Enabled"}, ""); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", n)
			input := map[string]any{"Bucket": "version-restore", "Key": key}
			if _, err := call("PutObject", input, "old"); err != nil {
				errs <- err
				return
			}
			newer, err := call("PutObject", input, "new")
			if err != nil {
				errs <- err
				return
			}
			versionID := newer.Headers.Get("x-amz-version-id")
			if n%2 == 0 {
				input["VersionId"] = versionID
				_, err = call("DeleteObject", input, "")
			} else {
				deleted, deleteErr := call("DeleteObjects", map[string]any{"Bucket": "version-restore", "Quiet": true, "Objects": []any{
					map[string]any{"Key": key, "VersionId": versionID},
					map[string]any{"Key": key, "VersionId": "missing"},
				}}, "")
				err = deleteErr
				if err == nil {
					failures, _ := deleted.Output["Errors"].([]any)
					if len(failures) != 1 || failures[0].(map[string]any)["Code"] != "NoSuchVersion" || deleted.Output["Deleted"] != nil {
						err = fmt.Errorf("%s multi-delete %#v", key, deleted.Output)
					}
				}
			}
			if err != nil {
				errs <- err
				return
			}
			delete(input, "VersionId")
			restored, err := call("GetObject", input, "")
			if err == nil {
				var body []byte
				body, err = io.ReadAll(restored.Stream)
				_ = restored.Stream.Close()
				if err == nil && string(body) != "old" {
					err = fmt.Errorf("%s restored %q", key, body)
				}
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTwoAccountsNeverSeeEachOtherUnderLoad(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	a := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	b := spi.Identity{Account: "222222222222", Region: "us-east-1"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: a, Operation: "CreateBucket", Input: map[string]any{"Bucket": "account-a"}})
	_, _ = p.Invoke(ctx, &spi.Request{Identity: b, Operation: "CreateBucket", Input: map[string]any{"Bucket": "account-b"}})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = p.Invoke(ctx, &spi.Request{Identity: a, Operation: "PutObject", Input: map[string]any{"Bucket": "account-a", "Key": "k"}, Body: io.NopCloser(bytes.NewReader([]byte("A")))})
		}()
		go func() {
			defer wg.Done()
			_, _ = p.Invoke(ctx, &spi.Request{Identity: b, Operation: "PutObject", Input: map[string]any{"Bucket": "account-b", "Key": "k"}, Body: io.NopCloser(bytes.NewReader([]byte("B")))})
		}()
	}
	wg.Wait()
	ga, err := p.Invoke(ctx, &spi.Request{Identity: a, Operation: "GetObject", Input: map[string]any{"Bucket": "account-a", "Key": "k"}})
	if err != nil {
		t.Fatal(err)
	}
	gb, err := p.Invoke(ctx, &spi.Request{Identity: b, Operation: "GetObject", Input: map[string]any{"Bucket": "account-b", "Key": "k"}})
	if err != nil {
		t.Fatal(err)
	}
	ba, _ := io.ReadAll(ga.Stream)
	bb, _ := io.ReadAll(gb.Stream)
	if string(ba) != "A" || string(bb) != "B" {
		t.Fatalf("leaked %q vs %q", ba, bb)
	}
}

func TestEncryptedBlobFailureLeavesNoObjectAndRecovers(t *testing.T) {
	deps := spitest.Deps(t)
	blobs := &failBlobs{BlobStore: deps.Blobs, fail: true}
	deps.Blobs = blobs
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "bucket"}})
	keyID := "arn:aws:kms:us-east-1:000000000000:key/chaos"
	spitest.SeedKMSKey(t, deps, id, keyID, "Enabled")
	input := map[string]any{"Bucket": "bucket", "Key": "k", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID, "BucketKeyEnabled": true}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: input, Body: io.NopCloser(bytes.NewReader([]byte("x")))})
	if err == nil {
		t.Fatal("expected injected failure")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "HeadObject", Input: map[string]any{"Bucket": "bucket", "Key": "k"}}); err == nil {
		t.Fatal("failed encrypted write left object metadata")
	}
	blobs.fail = false
	put, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: input, Body: io.NopCloser(bytes.NewReader([]byte("x")))})
	if err != nil || put.Headers.Get("x-amz-server-side-encryption") != "aws:kms" || put.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
		t.Fatalf("recovery put: %#v %v", put, err)
	}
}

func TestConcurrentKMSKeyValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	enabled := "arn:aws:kms:us-east-1:000000000000:key/concurrent"
	missing := "arn:aws:kms:us-east-1:000000000000:key/missing"
	spitest.SeedKMSKey(t, deps, id, enabled, "Enabled")
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "kms-chaos"}}); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keyID := enabled
			if i%2 != 0 {
				keyID = missing
			}
			_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "kms-chaos", "Key": fmt.Sprintf("key-%d", i), "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID}, Body: io.NopCloser(strings.NewReader("body"))})
			if i%2 == 0 && err != nil {
				errs <- err
			} else if i%2 != 0 {
				fault, ok := err.(*spi.Fault)
				if !ok || fault.Code != "KMS.NotFoundException" {
					errs <- fmt.Errorf("missing key fault: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestConcurrentGetObjectResponseOverrides(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body string) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != "" {
			stream = io.NopCloser(strings.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	if _, err := call("CreateBucket", map[string]any{"Bucket": "override-chaos"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := call("PutObject", map[string]any{"Bucket": "override-chaos", "Key": "object", "ContentType": "application/json"}, "body"); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			contentType := fmt.Sprintf("application/x-%d", i)
			response, err := call("GetObject", map[string]any{"Bucket": "override-chaos", "Key": "object", "ResponseContentType": contentType}, "")
			if err != nil {
				errs <- err
				return
			}
			body, _ := io.ReadAll(response.Stream)
			_ = response.Stream.Close()
			if response.Headers.Get("Content-Type") != contentType || string(body) != "body" {
				errs <- fmt.Errorf("override %d: %q %q", i, response.Headers.Get("Content-Type"), body)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	stored, err := call("HeadObject", map[string]any{"Bucket": "override-chaos", "Key": "object"}, "")
	if err != nil || stored.Headers.Get("Content-Type") != "application/json" {
		t.Fatalf("stored metadata changed: %#v %v", stored, err)
	}
}

func TestConcurrentUserMetadataRFC2047(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body string) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != "" {
			stream = io.NopCloser(strings.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	if _, err := call("CreateBucket", map[string]any{"Bucket": "rfc2047-chaos"}, ""); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("Ä-%d", i)
			if _, err := call("PutObject", map[string]any{"Bucket": "rfc2047-chaos", "Key": key, "Metadata": map[string]any{"value": value}}, "body"); err != nil {
				errs <- err
				return
			}
			response, err := call("HeadObject", map[string]any{"Bucket": "rfc2047-chaos", "Key": key}, "")
			want := fmt.Sprintf("=?UTF-8?Q?=C3=84-%d?=", i)
			if err != nil {
				errs <- err
				return
			}
			if got := response.Headers.Get("x-amz-meta-value"); got != want {
				errs <- fmt.Errorf("metadata %d: %q", i, got)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestEncryptedMultipartCompletionFailurePreservesUpload(t *testing.T) {
	deps := spitest.Deps(t)
	blobs := &failBlobs{BlobStore: deps.Blobs}
	deps.Blobs = blobs
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body []byte) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != nil {
			stream = io.NopCloser(bytes.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	_, _ = call("CreateBucket", map[string]any{"Bucket": "multipart-encryption"}, nil)
	keyID := "arn:aws:kms:us-east-1:000000000000:key/multipart-chaos"
	spitest.SeedKMSKey(t, deps, id, keyID, "Enabled")
	created, err := call("CreateMultipartUpload", map[string]any{"Bucket": "multipart-encryption", "Key": "object", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID, "BucketKeyEnabled": true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadID := created.Output["UploadId"].(string)
	part, err := call("UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1}, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	complete := map[string]any{"UploadId": uploadID, "MultipartUpload": map[string]any{"Parts": []any{map[string]any{"PartNumber": 1, "ETag": part.Headers.Get("ETag")}}}}
	blobs.fail = true
	if _, err := call("CompleteMultipartUpload", complete, nil); err == nil {
		t.Fatal("expected injected completion failure")
	}
	if _, err := call("HeadObject", map[string]any{"Bucket": "multipart-encryption", "Key": "object"}, nil); err == nil {
		t.Fatal("failed encrypted completion left object metadata")
	}
	blobs.fail = false
	completed, err := call("CompleteMultipartUpload", complete, nil)
	if err != nil || completed.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || completed.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
		t.Fatalf("recovered completion: %#v %v", completed, err)
	}
}

func TestCustomerEncryptedMultipartCompletionFailurePreservesUpload(t *testing.T) {
	deps := spitest.Deps(t)
	blobs := &failBlobs{BlobStore: deps.Blobs}
	deps.Blobs = blobs
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(operation string, input map[string]any, body []byte) (*spi.Response, error) {
		var stream io.ReadCloser
		if body != nil {
			stream = io.NopCloser(bytes.NewReader(body))
		}
		return p.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input, Body: stream})
	}
	_, _ = call("CreateBucket", map[string]any{"Bucket": "multipart-sse-c"}, nil)
	rawKey := []byte("0123456789abcdef0123456789abcdef")
	digest := md5.Sum(rawKey)
	encryption := map[string]any{"SSECustomerAlgorithm": "AES256", "SSECustomerKey": base64.StdEncoding.EncodeToString(rawKey), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(digest[:])}
	create := map[string]any{"Bucket": "multipart-sse-c", "Key": "object"}
	for key, value := range encryption {
		create[key] = value
	}
	created, err := call("CreateMultipartUpload", create, nil)
	if err != nil {
		t.Fatal(err)
	}
	uploadID := created.Output["UploadId"].(string)
	partInput := map[string]any{"UploadId": uploadID, "PartNumber": 1}
	for key, value := range encryption {
		partInput[key] = value
	}
	part, err := call("UploadPart", partInput, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	complete := map[string]any{"UploadId": uploadID, "MultipartUpload": map[string]any{"Parts": []any{map[string]any{"PartNumber": 1, "ETag": part.Headers.Get("ETag")}}}}
	blobs.fail = true
	if _, err := call("CompleteMultipartUpload", complete, nil); err == nil {
		t.Fatal("expected injected completion failure")
	}
	blobs.fail = false
	completed, err := call("CompleteMultipartUpload", complete, nil)
	if err != nil || completed.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != encryption["SSECustomerKeyMD5"] {
		t.Fatalf("recovered customer completion: %#v %v", completed, err)
	}
}

func TestPostObjectBlobFailureLeavesNoObject(t *testing.T) {
	deps := spitest.Deps(t)
	deps.Blobs = failBlobs{BlobStore: deps.Blobs, fail: true}
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "post-failure"}}); err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	_ = writer.WriteField("key", "object")
	_ = writer.WriteField("tagging", "<Tagging><TagSet><Tag><Key>source</Key><Value>browser</Value></Tag></TagSet></Tagging>")
	_ = writer.WriteField("Expires", "Thu, 27 Aug 2026 12:00:00 GMT")
	_ = writer.WriteField("x-amz-checksum-algorithm", "CRC32")
	file, _ := writer.CreateFormFile("file", "object.txt")
	_, _ = file.Write([]byte("body"))
	_ = writer.Close()
	httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-failure", &payload)
	httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PostObject", Input: map[string]any{"Bucket": "post-failure"}, Body: httpRequest.Body, HTTP: httpRequest}); err == nil {
		t.Fatal("expected injected POST storage failure")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{"Bucket": "post-failure", "Key": "object"}}); err == nil {
		t.Fatal("failed POST left object metadata")
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObjectTagging", Input: map[string]any{"Bucket": "post-failure", "Key": "object"}}); err == nil {
		t.Fatal("failed POST left object tags")
	}
}

func TestClockAdvanceDoesNotPanic(t *testing.T) {
	deps := spitest.Deps(t)
	if err := deps.Clock.Advance(24 * 3600 * 1e9); err != nil {
		t.Fatal(err)
	}
	p := s3.New(deps)
	_, err := p.Invoke(context.Background(), &spi.Request{
		Identity:  spi.Identity{Account: "000000000000", Region: "us-east-1"},
		Operation: "ListBuckets", Input: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBusSubscriberPanicIsolated(t *testing.T) {
	deps := spitest.Deps(t)
	defer func() {
		if recover() != nil {
			t.Fatal("bus panic escaped Publish")
		}
	}()
	deps.Bus.Subscribe("t", func(context.Context, []byte) { panic("boom") })
	// Current bus delivers synchronously; a panicking subscriber is a process
	// fault. Document that: Publish must not swallow it. We assert it *does*
	// panic so a future isolation change is visible.
	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		_ = deps.Bus.Publish(context.Background(), "t", []byte("x"))
	}()
	if !panicked {
		t.Fatal("expected subscriber panic to propagate (current contract)")
	}
}
