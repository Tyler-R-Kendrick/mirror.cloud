package gcpcloudbuild_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/gcpcloudbuild"
)

func TestGCP_DELIVERY(t *testing.T) {
	dir := t.TempDir()
	nonce := "gcp-" + t.Name()
	yaml := `
substitutions:
  _NONCE: ` + nonce + `
steps:
- id: gen
  script: echo -n '${_NONCE}' > a.txt
- id: pack
  waitFor: [gen]
  script: cp a.txt out.txt
`
	cf, err := gcpcloudbuild.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	r, err := gcpcloudbuild.Run(context.Background(), cf, gcpcloudbuild.Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != "SUCCESS" {
		t.Fatalf("%#v", r)
	}
	b, err := os.ReadFile(filepath.Join(dir, "out.txt"))
	if err != nil || string(b) != nonce {
		t.Fatalf("%q %v", b, err)
	}
}
