package githubactions_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/githubactions"
)

func TestGH_DELIVERY(t *testing.T) {
	dir := t.TempDir()
	nonce := "gh-" + t.Name()
	yaml := `
on:
  push:
    branches: [main]
jobs:
  build:
    strategy:
      matrix:
        os: [linux]
        exclude:
          - os: windows
    steps:
      - run: echo -n '` + nonce + `' > out.txt
      - run: mkdir -p "$MIRROR_ARTIFACTS" && cp out.txt "$MIRROR_ARTIFACTS/out.txt"
  deploy:
    needs: [build]
    steps:
      - run: cp "$MIRROR_ARTIFACTS/out.txt" deployed.txt
`
	wf, err := githubactions.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	res, err := githubactions.Run(context.Background(), wf, githubactions.Config{WorkDir: dir, Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "succeeded" {
		t.Fatalf("%#v", res)
	}
	b, err := os.ReadFile(filepath.Join(dir, "deployed.txt"))
	if err != nil || string(b) != nonce {
		t.Fatalf("deployed %q err %v", b, err)
	}
	// fail without running deploy
	bad := `
on: push
jobs:
  build:
    steps:
      - run: false
  deploy:
    needs: [build]
    steps:
      - run: echo should-not > bad.txt
`
	wf2, _ := githubactions.Parse([]byte(bad))
	r2, err := githubactions.Run(context.Background(), wf2, githubactions.Config{WorkDir: t.TempDir(), Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Status == "succeeded" {
		t.Fatal("want failed")
	}
}
