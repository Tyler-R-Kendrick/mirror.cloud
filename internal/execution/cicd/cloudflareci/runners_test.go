package cloudflareci_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/cloudflareci"
)

// CF-CI-RUNNERS: @cloudflare/ci 0.2 Artifacts push → runners + cache + fail stops deploy.
func TestCF_CI_RUNNERS(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "package.json"), `{"name":"app"}`)
	mustWrite(t, filepath.Join(dir, "bun.lock"), "lock-v1\n")

	ev := map[string]any{
		"type": cloudflareci.EventArtifactsPushed,
		"source": map[string]any{
			"namespace": "CI",
			"repoName":  "demo",
		},
		"payload": map[string]any{
			"ref":    "refs/heads/main",
			"before": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"after":  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			"commits": []any{
				map[string]any{
					"id":      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"message": "ship it",
					"author":  map[string]any{"name": "dev", "email": "dev@example.com"},
				},
			},
		},
	}
	body, _ := json.Marshal(ev)
	parsed, err := cloudflareci.ParseArtifactsPushEvent(body)
	if err != nil || parsed == nil {
		t.Fatalf("parse: %v %#v", err, parsed)
	}
	params, err := cloudflareci.MapPushEventToCiParams(parsed)
	if err != nil || params == nil {
		t.Fatalf("map: %v %#v", err, params)
	}
	if params.Provider != "cloudflare-artifacts" || params.Branch != "main" || params.SHA != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("params %#v", params)
	}

	nonce := "nonce-" + t.Name()
	pipe := cloudflareci.Pipeline{
		Roots: []cloudflareci.RunnerNode{{
			RunnerOptions: cloudflareci.RunnerOptions{
				Name:    "install",
				Command: "mkdir -p .deps && echo installed > .deps/ok",
				Cache:   &struct{ Inputs []string `json:"inputs"` }{Inputs: []string{"package.json", "bun.lock"}},
			},
			Children: []cloudflareci.RunnerNode{
				{RunnerOptions: cloudflareci.RunnerOptions{Name: "lint", Command: "test -f .deps/ok"}},
				{RunnerOptions: cloudflareci.RunnerOptions{Name: "build", Command: "echo -n '" + nonce + "' > dist.txt"}},
			},
			Then: []cloudflareci.RunnerNode{
				{RunnerOptions: cloudflareci.RunnerOptions{Name: "deploy", Command: "cp dist.txt deployed.txt"}},
			},
		}},
	}

	ctx := context.Background()
	r1, err := cloudflareci.Run(ctx, *params, pipe, cloudflareci.Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != "succeeded" || r1.Package != cloudflareci.PackagePin {
		t.Fatalf("run1 %#v", r1)
	}
	if got := read(t, filepath.Join(dir, "deployed.txt")); got != nonce {
		t.Fatalf("deployed %q want %q", got, nonce)
	}
	if findStep(r1, "install").CacheHit {
		t.Fatal("first install must be cache miss")
	}

	_ = os.Remove(filepath.Join(dir, "deployed.txt"))
	_ = os.Remove(filepath.Join(dir, "dist.txt"))
	r2, err := cloudflareci.Run(ctx, *params, pipe, cloudflareci.Config{WorkDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if !findStep(r2, "install").CacheHit {
		t.Fatal("second install want cache hit")
	}
	if read(t, filepath.Join(dir, "deployed.txt")) != nonce {
		t.Fatal("deploy after cache hit install")
	}

	failPipe := cloudflareci.Pipeline{
		Roots: []cloudflareci.RunnerNode{{
			RunnerOptions: cloudflareci.RunnerOptions{Name: "install", Command: "true"},
			Children: []cloudflareci.RunnerNode{
				{RunnerOptions: cloudflareci.RunnerOptions{Name: "lint", Command: "false"}},
			},
			Then: []cloudflareci.RunnerNode{
				{RunnerOptions: cloudflareci.RunnerOptions{Name: "deploy", Command: "echo bad > deployed-bad.txt"}},
			},
		}},
	}
	dir2 := t.TempDir()
	rf, err := cloudflareci.Run(ctx, *params, failPipe, cloudflareci.Config{WorkDir: dir2})
	if err != nil {
		t.Fatal(err)
	}
	if rf.Status != "errored" {
		t.Fatalf("want errored, got %s", rf.Status)
	}
	if !findStep(rf, "deploy").Skipped {
		t.Fatal("deploy must skip after lint fail")
	}
	if _, err := os.Stat(filepath.Join(dir2, "deployed-bad.txt")); !os.IsNotExist(err) {
		t.Fatal("deploy must not run after failed check")
	}

	other, err := cloudflareci.ParseArtifactsPushEvent([]byte(`{"type":"cf.other"}`))
	if err != nil || other != nil {
		t.Fatalf("wrong type: %v %#v", err, other)
	}
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func findStep(r *cloudflareci.RunResult, name string) cloudflareci.StepResult {
	for _, s := range r.Steps {
		if s.Name == name {
			return s
		}
	}
	return cloudflareci.StepResult{}
}
