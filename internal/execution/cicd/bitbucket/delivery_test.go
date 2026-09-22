package bitbucket_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/bitbucket"
)

func TestBB_DELIVERY(t *testing.T) {
	dir := t.TempDir()
	nonce := "bb-" + t.Name()
	yaml := `
pipelines:
  default:
    - step:
        name: build
        script:
          - echo -n '` + nonce + `' > art.txt
        artifacts:
          - art.txt
    - step:
        name: deploy
        trigger: manual
        script:
          - cp art.txt deployed.txt
`
	f, err := bitbucket.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	r1, err := bitbucket.Run(context.Background(), f, bitbucket.Config{WorkDir: dir, Branch: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status == "succeeded" {
		t.Fatalf("want blocked: %#v", r1)
	}
	dir2 := t.TempDir()
	r2, err := bitbucket.Run(context.Background(), f, bitbucket.Config{
		WorkDir: dir2, Branch: "main",
		Manual: func(step string) bool { return step == "deploy" },
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
