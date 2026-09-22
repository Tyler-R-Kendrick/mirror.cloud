package azurepipelines_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/azurepipelines"
)

func TestAZ_DELIVERY(t *testing.T) {
	dir := t.TempDir()
	nonce := "az-" + t.Name()
	yaml := `
stages:
- stage: build
  jobs:
  - job: b
    steps:
    - script: echo -n '` + nonce + `' > art.txt
- stage: deploy
  jobs:
  - job: d
    environment:
      name: production
    steps:
    - script: cp art.txt deployed.txt
`
	p, err := azurepipelines.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	r1, err := azurepipelines.Run(context.Background(), p, azurepipelines.Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != "blocked" {
		t.Fatalf("want blocked got %#v", r1)
	}
	dir2 := t.TempDir()
	// need art from build — run with approval and both stages in one workdir after copying
	// Re-run full pipeline with approval in fresh dir that includes prior build via single run:
	r2, err := azurepipelines.Run(context.Background(), p, azurepipelines.Config{
		WorkDir:            dir2,
		ApproveEnvironment: func(name string) bool { return name == "production" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Status != "succeeded" {
		t.Fatalf("%#v", r2)
	}
	b, err := os.ReadFile(filepath.Join(dir2, "deployed.txt"))
	if err != nil || string(b) != nonce {
		t.Fatalf("%q %v", b, err)
	}
}
