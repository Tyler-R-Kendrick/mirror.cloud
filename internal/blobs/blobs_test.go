package blobs_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/blobs"
)

func TestMemoryPutGetStatDelete(t *testing.T) {
	ctx := context.Background()
	b := blobs.NewMemory()
	body := []byte("hello blob")
	info, err := b.Put(ctx, "k", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum(body)
	wantMD5 := hex.EncodeToString(sum[:])
	if info.Size != int64(len(body)) || info.MD5 != wantMD5 {
		t.Fatalf("put info: %+v want md5 %s", info, wantMD5)
	}

	rc, got, err := b.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, body) {
		t.Fatalf("get bytes: %q", raw)
	}
	if got.MD5 != wantMD5 || got.Size != int64(len(body)) {
		t.Fatalf("get info: %+v", got)
	}

	st, err := b.Stat(ctx, "k")
	if err != nil || st.MD5 != wantMD5 {
		t.Fatalf("stat: %+v err=%v", st, err)
	}
	if err := b.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Get(ctx, "k"); err == nil {
		t.Fatal("expected missing after delete")
	}
	if _, err := b.Stat(ctx, "missing"); err == nil {
		t.Fatal("expected missing stat")
	}
}

func TestMemorySnapshotRestore(t *testing.T) {
	ctx := context.Background()
	src := blobs.NewMemory()
	if _, err := src.Put(ctx, "k", bytes.NewReader([]byte("snap"))); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := src.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	dst := blobs.NewMemory()
	if err := dst.Restore(ctx, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	rc, _, err := dst.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "snap" {
		t.Fatalf("restored %q", raw)
	}
}
