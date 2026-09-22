package githubactions_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/githubactions"
)

func TestGH_ACTIONS(t *testing.T) {
	dir := t.TempDir()
	nonce := "gha-" + t.Name()
	// composite action
	comp := filepath.Join(dir, "comp")
	mustMk(t, comp)
	mustWrite(t, filepath.Join(comp, "action.yml"), "name: c\nruns:\n  using: composite\n  steps:\n    - run: echo -n '"+nonce+"' > comp.txt\n")
	// js action
	js := filepath.Join(dir, "jsact")
	mustMk(t, js)
	mustWrite(t, filepath.Join(js, "action.yml"), "name: j\nruns:\n  using: node20\n  main: index.js\n")
	mustWrite(t, filepath.Join(js, "index.js"),
		"const fs=require('fs'); fs.appendFileSync(process.env.GITHUB_OUTPUT, 'msg="+nonce+"\\n');\n")

	yaml := `
on: push
jobs:
  build:
    steps:
      - uses: ./comp
      - id: j
        uses: ./jsact
      - run: test -f comp.txt && test "$(cat comp.txt)" = "` + nonce + `"
      - run: test "${{ steps.j.outputs.msg }}" = "` + nonce + `"
      - run: mkdir -p "$MIRROR_ARTIFACTS" && printf '%s' "${{ steps.j.outputs.msg }}" > "$MIRROR_ARTIFACTS/nonce.txt"
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
	got, err := os.ReadFile(filepath.Join(dir, ".mirror-gha-artifacts", "nonce.txt"))
	if err != nil || string(got) != nonce {
		t.Fatalf("nonce artifact %q err %v", got, err)
	}
}

func TestADV_TOLERATED_FAILURE(t *testing.T) {
	dir := t.TempDir()
	yaml := `
on: push
jobs:
  build:
    steps:
      - run: false
        continue-on-error: true
      - run: echo ok > ok.txt
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
		t.Fatalf("want succeeded with continue-on-error %#v", res)
	}
	if _, err := os.Stat(filepath.Join(dir, "ok.txt")); err != nil {
		t.Fatal(err)
	}
}

func mustMk(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatal(err)
	}
}
func mustWrite(t *testing.T, p, body string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
