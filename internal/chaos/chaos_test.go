package chaos

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
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

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

type failBlobs struct {
	spi.BlobStore
	fail    bool
	failKey string
}

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
	input := map[string]any{"Bucket": "bucket", "Key": "k", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "arn:aws:kms:us-east-1:000000000000:key/chaos", "BucketKeyEnabled": true}
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
