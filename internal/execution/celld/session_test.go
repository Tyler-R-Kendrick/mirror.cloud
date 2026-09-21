package celld_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/celld"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestDescribeUnset(t *testing.T) {
	s := &celld.Session{}
	id, caps, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id.Name != "celld" {
		t.Fatalf("backend: got %q", id.Name)
	}
	if id.Version != "" {
		t.Fatalf("want empty version before Start, got %q", id.Version)
	}
	if len(caps) != 0 {
		t.Fatalf("want empty descriptors, got %d", len(caps))
	}
}

func TestCallUnsetUnavailable(t *testing.T) {
	s := &celld.Session{}
	_, err := s.Call(context.Background(), spi.CapabilityCall{Action: "do.fetch"})
	ce, ok := err.(*spi.CapabilityError)
	if !ok || ce.Kind != spi.CapErrUnavailable {
		t.Fatalf("want CapErrUnavailable, got %T %v", err, err)
	}
}

func TestStartUnsetNoDownload(t *testing.T) {
	s := &celld.Session{}
	err := s.Start(context.Background())
	ce, ok := err.(*spi.CapabilityError)
	if !ok || ce.Kind != spi.CapErrUnavailable {
		t.Fatalf("want CapErrUnavailable, got %T %v", err, err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	s := &celld.Session{}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func pinnedBinary(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// internal/execution/celld -> repo root
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	bin := filepath.Join(root, "tools", "celld-runtime", "bin", "celld")
	if st, err := os.Stat(bin); err != nil || st.IsDir() {
		t.Skip("pinned celld binary absent; see tools/celld-runtime/README.md")
	}
	return bin
}

func TestStartRecordsIdentity(t *testing.T) {
	bin := pinnedBinary(t)
	s := &celld.Session{Binary: bin}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id, caps, err := s.Describe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id.Name != "celld" {
		t.Fatalf("name: got %q", id.Name)
	}
	if id.Version == "" {
		t.Fatal("want non-empty version from --version")
	}
	if len(caps) != 0 {
		t.Fatalf("must not advertise DO caps yet, got %d", len(caps))
	}
}

func TestCallProvisionedUnsupported(t *testing.T) {
	bin := pinnedBinary(t)
	s := &celld.Session{Binary: bin}
	_, err := s.Call(context.Background(), spi.CapabilityCall{Action: "do.fetch"})
	ce, ok := err.(*spi.CapabilityError)
	if !ok || ce.Kind != spi.CapErrUnsupported {
		t.Fatalf("want CapErrUnsupported, got %T %v", err, err)
	}
}
