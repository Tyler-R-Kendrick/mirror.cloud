package chaos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

type failBlobs struct {
	spi.BlobStore
	fail bool
}

func (f failBlobs) Put(ctx context.Context, key string, r io.Reader) (spi.BlobInfo, error) {
	if f.fail {
		return spi.BlobInfo{}, errors.New("injected blob failure")
	}
	return f.BlobStore.Put(ctx, key, r)
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
			input["VersionId"] = newer.Headers.Get("x-amz-version-id")
			if _, err = call("DeleteObject", input, ""); err != nil {
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

func TestBlobFailureSurfaces(t *testing.T) {
	deps := spitest.Deps(t)
	deps.Blobs = failBlobs{BlobStore: deps.Blobs, fail: true}
	p := s3.New(deps)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "bucket"}})
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "bucket", "Key": "k"}, Body: io.NopCloser(bytes.NewReader([]byte("x")))})
	if err == nil {
		t.Fatal("expected injected failure")
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
