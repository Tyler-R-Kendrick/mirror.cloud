package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

func TestExpandServices(t *testing.T) {
	got := runtime.ExpandServices([]string{"s3,sqs"}, "", false)
	if len(got) != 2 || got[0] != "aws.s3" || got[1] != "aws.sqs" {
		t.Fatalf("got %v", got)
	}
	if runtime.ExpandServices(nil, "", true) != nil {
		t.Fatal("all should be nil (enable every catalog service)")
	}
	got = runtime.ExpandServices(nil, "aws-core", false)
	if len(got) != 151 {
		t.Fatalf("aws-core: %v", got)
	}
}

func TestEnvOutput(t *testing.T) {
	t.Setenv("MIRROR_BIND", "127.0.0.1:4566")
	var buf bytes.Buffer
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	if err := cmdEnv(nil); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	os.Stdout = old
	_, _ = buf.ReadFrom(r)
	out := buf.String()
	for _, want := range []string{
		"AWS_ENDPOINT_URL=http://127.0.0.1:4566",
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		"STORAGE_EMULATOR_HOST=",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
}

func TestSplitFlagsAllowsBindAfterService(t *testing.T) {
	flags, pos := splitFlags([]string{"s3", "--bind", "127.0.0.1:9", "--seed", "x"}, map[string]bool{"all": true, "strict": true})
	if len(pos) != 1 || pos[0] != "s3" {
		t.Fatalf("pos %v", pos)
	}
	joined := strings.Join(flags, " ")
	if !strings.Contains(joined, "--bind 127.0.0.1:9") || !strings.Contains(joined, "--seed x") {
		t.Fatalf("flags %v", flags)
	}
}

func TestUsageNonEmpty(t *testing.T) {
	var b bytes.Buffer
	usage(&b)
	if !strings.Contains(b.String(), "mirror up") {
		t.Fatal(b.String())
	}
}

func TestCommandDiagnosticsAndCatalog(t *testing.T) {
	t.Setenv("MIRROR_BIND", "127.0.0.1:0")
	t.Setenv("AWS_ENDPOINT_URL", "https://amazonaws.com")
	t.Setenv("AWS_ENDPOINT_URL_S3", "https://s3.amazonaws.com")
	t.Setenv("AWS_DEFAULT_REGION", "wrong")
	t.Setenv("AWS_S3_FORCE_PATH_STYLE", "false")
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	out, err := captureStdout(t, func() error { return cmdDoctor([]string{"--bind", "127.0.0.1:0"}) })
	if err == nil || !strings.Contains(out, "FIX") || !strings.Contains(err.Error(), "finding") {
		t.Fatalf("doctor output %q error %v", out, err)
	}
	if out, err := captureStdout(t, cmdServices); err != nil || !strings.Contains(out, "aws.s3") || !strings.Contains(out, "emulate") {
		t.Fatalf("services output %q error %v", out, err)
	}
	if err := cmdDoctor([]string{"--unknown"}); err == nil {
		t.Fatal("doctor accepted unknown flag")
	}
	if err := cmdSupportMatrix([]string{"--unknown"}); err == nil {
		t.Fatal("support matrix accepted unknown flag")
	}
	dir := t.TempDir()
	outPath := filepath.Join(dir, "nested", "SUPPORT.md")
	if _, err := captureStdout(t, func() error { return cmdSupportMatrix([]string{"--out", outPath}) }); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(outPath); err != nil || !bytes.Contains(body, []byte("aws.s3")) {
		t.Fatalf("support matrix %s %v", body, err)
	}
}

func TestCommandSpecSnapshotAndDrift(t *testing.T) {
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(old)
	if err := cmdSpec(nil); err == nil {
		t.Fatal("spec accepted missing subcommand")
	}
	for _, args := range [][]string{{"sync"}, {"pin"}, {"update"}, {"diff"}} {
		if _, err := captureStdout(t, func() error { return cmdSpec(args) }); err != nil {
			t.Fatalf("spec %v: %v", args, err)
		}
	}
	if err := cmdSpec([]string{"add"}); err == nil {
		t.Fatal("spec add accepted missing service")
	}
	if err := os.MkdirAll("specs", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("specs", "mirror.lock"), []byte("lock"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := captureStdout(t, func() error { return cmdSpec([]string{"add", "aws.s3"}) }); err != nil {
		t.Fatal(err)
	}
	if out, err := captureStdout(t, func() error { return cmdSpec([]string{"add", "aws.s3"}) }); err != nil || !strings.Contains(out, "already") {
		t.Fatalf("duplicate spec %q %v", out, err)
	}
	if err := cmdSpec([]string{"unknown"}); err == nil {
		t.Fatal("unknown spec subcommand succeeded")
	}
	if lockSHA() == "bootstrap" {
		t.Fatal("existing mirror.set was not hashed")
	}
	if err := cmdSnapshot(nil); err == nil {
		t.Fatal("snapshot accepted missing command")
	}
	if err := cmdSnapshot([]string{"unknown"}); err == nil {
		t.Fatal("snapshot accepted unknown command")
	}
	if err := cmdSnapshot([]string{"save", "--persist", "persist"}); err == nil {
		t.Fatal("snapshot saved missing state")
	}
	if err := os.MkdirAll("persist", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("persist", "state.tar"), []byte("state"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := captureStdout(t, func() error { return cmdSnapshot([]string{"save", "--name", "one", "--persist", "persist"}) }); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("persist", "state.tar"), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := captureStdout(t, func() error { return cmdSnapshot([]string{"load", "--name", "one", "--persist", "persist"}) }); err != nil {
		t.Fatal(err)
	}
	if body, _ := os.ReadFile(filepath.Join("persist", "state.tar")); string(body) != "state" {
		t.Fatalf("loaded snapshot %q", body)
	}
	if err := cmdDrift(nil); err == nil {
		t.Fatal("drift accepted missing paths")
	}
	if err := os.WriteFile("a", []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("b", []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := captureStdout(t, func() error { return cmdDrift([]string{"--emulated", "a", "--recorded", "b"}) }); err != nil || !strings.Contains(out, "identical") {
		t.Fatalf("identical drift %q %v", out, err)
	}
	if err := os.WriteFile("b", []byte("different"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := captureStdout(t, func() error { return cmdDrift([]string{"--emulated", "a", "--recorded", "b"}) }); err == nil || !strings.Contains(out, "divergence") {
		t.Fatalf("divergent drift %q %v", out, err)
	}
	if bytesEqual([]byte("a"), []byte("b")) || bytesEqual([]byte("a"), []byte("aa")) || !bytesEqual([]byte("a"), []byte("a")) || len(shortSHA([]byte("x"))) != 16 {
		t.Fatal("drift helpers")
	}
}

func TestCommandUpValidation(t *testing.T) {
	if err := cmdUp([]string{"--tier", "invalid"}); err == nil {
		t.Fatal("accepted invalid tier")
	}
	if err := cmdUp([]string{"s3", "--bind", "bad::addr"}); err == nil {
		t.Fatal("invalid listen address succeeded")
	}
	flags, positional := splitFlags([]string{"--seed=x", "s3", "--", "--literal"}, map[string]bool{})
	if len(flags) != 1 || len(positional) != 2 || positional[1] != "--literal" {
		t.Fatalf("split flags %v %v", flags, positional)
	}
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	err = fn()
	_ = w.Close()
	os.Stdout = old
	var out bytes.Buffer
	_, _ = out.ReadFrom(r)
	_ = r.Close()
	return out.String(), err
}
