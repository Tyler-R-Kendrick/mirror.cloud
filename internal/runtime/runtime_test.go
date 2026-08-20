package runtime

import (
	"bytes"
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/sts"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	d := spitest.Deps(t)
	ctx := context.Background()
	if err := d.Store.Scope("000000000000", "us-east-1").Collection("t").Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Snapshot(d, "lock-a", &buf); err != nil {
		t.Fatal(err)
	}
	d2 := spitest.Deps(t)
	if err := Restore(d2, "lock-b", bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("expected lock mismatch")
	}
	if err := Restore(d2, "lock-a", bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	got, ok, err := d2.Store.Scope("000000000000", "us-east-1").Collection("t").Get(ctx, "k")
	if err != nil || !ok || string(got) != "v" {
		t.Fatalf("got %q ok=%v err=%v", got, ok, err)
	}
}

func TestBootS3Only(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	rt, err := Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.Reg.Resolve("aws.s3"); !ok {
		t.Fatal("s3 missing")
	}
}
