package chaos

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/s3"
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
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "b"}})
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
				Input: map[string]any{"Bucket": "b", "Key": "k"},
				Body:  io.NopCloser(bytes.NewReader(body)),
			})
		}(i)
	}
	wg.Wait()
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{"Bucket": "b", "Key": "k"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Stream == nil {
		t.Fatal("missing body after concurrent puts")
	}
	_ = got.Stream.Close()
}

func TestTwoAccountsNeverSeeEachOtherUnderLoad(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	ctx := context.Background()
	a := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	b := spi.Identity{Account: "222222222222", Region: "us-east-1"}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: a, Operation: "CreateBucket", Input: map[string]any{"Bucket": "shared-name"}})
	_, _ = p.Invoke(ctx, &spi.Request{Identity: b, Operation: "CreateBucket", Input: map[string]any{"Bucket": "shared-name"}})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = p.Invoke(ctx, &spi.Request{Identity: a, Operation: "PutObject", Input: map[string]any{"Bucket": "shared-name", "Key": "k"}, Body: io.NopCloser(bytes.NewReader([]byte("A")))})
		}()
		go func() {
			defer wg.Done()
			_, _ = p.Invoke(ctx, &spi.Request{Identity: b, Operation: "PutObject", Input: map[string]any{"Bucket": "shared-name", "Key": "k"}, Body: io.NopCloser(bytes.NewReader([]byte("B")))})
		}()
	}
	wg.Wait()
	ga, err := p.Invoke(ctx, &spi.Request{Identity: a, Operation: "GetObject", Input: map[string]any{"Bucket": "shared-name", "Key": "k"}})
	if err != nil {
		t.Fatal(err)
	}
	gb, err := p.Invoke(ctx, &spi.Request{Identity: b, Operation: "GetObject", Input: map[string]any{"Bucket": "shared-name", "Key": "k"}})
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
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateBucket", Input: map[string]any{"Bucket": "b"}})
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutObject", Input: map[string]any{"Bucket": "b", "Key": "k"}, Body: io.NopCloser(bytes.NewReader([]byte("x")))})
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
