package gitlabci_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/gitlabci"
)

func TestGL_DELIVERY(t *testing.T) {
	dir := t.TempDir()
	nonce := "gl-" + t.Name()
	yaml := `
stages: [build, deploy]
build:
  stage: build
  script:
    - echo -n '` + nonce + `' > art.txt
  artifacts:
    paths: [art.txt]
deploy:
  stage: deploy
  when: manual
  script:
    - cp art.txt deployed.txt
`
	p, err := gitlabci.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	// without approval → blocked, no deploy file
	r1, err := gitlabci.Run(context.Background(), p, gitlabci.Config{WorkDir: dir, Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status == "succeeded" {
		t.Fatalf("want blocked/failed without manual: %#v", r1)
	}
	if _, err := os.Stat(filepath.Join(dir, "deployed.txt")); !os.IsNotExist(err) {
		t.Fatal("deploy must not run")
	}
	dir2 := t.TempDir()
	r2, err := gitlabci.Run(context.Background(), p, gitlabci.Config{
		WorkDir: dir2, Branch: "main",
		Manual: func(job string) bool { return job == "deploy" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if r2.Status != "succeeded" {
		t.Fatalf("%#v", r2)
	}
	b, _ := os.ReadFile(filepath.Join(dir2, "deployed.txt"))
	if string(b) != nonce {
		t.Fatalf("got %q", b)
	}
}
