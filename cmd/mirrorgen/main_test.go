package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestEmitRoundTrip(t *testing.T) {
	dir := t.TempDir()
	svc := catalog.Bundle().ServiceByID("aws.s3")
	if svc == nil {
		t.Fatal("catalog missing aws.s3")
	}
	if err := emitService(dir, *svc); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "aws", "s3", "model.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	var got model.Service
	if err := json.NewDecoder(zr).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ID != "aws.s3" || len(got.Operations) == 0 {
		t.Fatalf("got %+v", got)
	}
	goSrc, err := os.ReadFile(filepath.Join(dir, "aws", "s3", "model.go"))
	if err != nil {
		t.Fatal(err)
	}
	if string(goSrc)[:len("// Code generated")] != "// Code generated" {
		t.Fatalf("header %s", goSrc[:40])
	}

	// Regenerating must produce identical bytes, including the gzip container:
	// the committed models are only trustworthy if they follow deterministically
	// from the pinned specs.
	first, err := os.ReadFile(filepath.Join(dir, "aws", "s3", "model.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if err := emitService(dir, *svc); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(dir, "aws", "s3", "model.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("generation is not byte-idempotent")
	}
}

func TestSanitizePkg(t *testing.T) {
	if got := sanitizePkg("resource-groups-tagging-api"); got != "resourcegroupstaggingapi" {
		t.Fatal(got)
	}
	p, pkg := splitID("gcp.storage")
	if p != "gcp" || pkg != "storage" {
		t.Fatalf("%s %s", p, pkg)
	}
}

func TestLoadSet(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "mirror.set")
	if err := os.WriteFile(p, []byte("# c\naws.s3 emulate\naws.lambda\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := loadSet(p)
	if err != nil || len(got) != 2 || got[0].Tier != model.TierEmulate || got[1].ID != "aws.lambda" {
		t.Fatalf("%v %v", got, err)
	}
}

func TestLoadBundleAndSpecIngestion(t *testing.T) {
	ctx := context.Background()
	if bundle, note, err := loadBundle(ctx, "missing", true); err != nil || len(bundle.Services) == 0 || !strings.Contains(note, "bootstrap") {
		t.Fatalf("catalog bundle %d %q %v", len(bundle.Services), note, err)
	}
	if bundle, note, err := loadBundle(ctx, filepath.Join(t.TempDir(), "missing"), false); err != nil || len(bundle.Services) == 0 || !strings.Contains(note, "no vendored") {
		t.Fatalf("fallback bundle %d %q %v", len(bundle.Services), note, err)
	}
	dir := t.TempDir()
	aws := `{"smithy":"2.0","shapes":{"com.example#Demo":{"type":"service","operations":[{"target":"com.example#Ping"}],"traits":{"aws.protocols#awsJson1_0":{},"aws.api#service":{"endpointPrefix":"demo","sdkId":"Demo"}}},"com.example#Ping":{"type":"operation","input":{"target":"com.example#Input"},"output":{"target":"com.example#Output"}},"com.example#Input":{"type":"structure","members":{}},"com.example#Output":{"type":"structure","members":{}}}}`
	gcp := `{"name":"storage","version":"v1","discoveryVersion":"v1","resources":{"buckets":{"methods":{"insert":{"id":"storage.buckets.insert","httpMethod":"POST","path":"b"}}}}}`
	for name, body := range map[string]string{"aws.json": aws, "gcp.JSON": gcp, "unknown.json": `{}`, "mirror.lock": `{}`, "notes.txt": "ignored"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	groups, count, err := ingestSpecs(ctx, dir)
	if err != nil || count != 2 || len(groups) != 2 {
		t.Fatalf("ingested groups=%d count=%d err=%v", len(groups), count, err)
	}
	bundle, note, err := loadBundle(ctx, dir, false)
	if err != nil || len(bundle.Services) != 2 || !strings.Contains(note, "2 spec file") {
		t.Fatalf("fused bundle %#v %q %v", bundle.Services, note, err)
	}
}

func TestDiffFilteringAndEmission(t *testing.T) {
	old := model.Bundle{SchemaVersion: "1", Services: []model.Service{{ID: "aws.a", Operations: []model.Operation{{Name: "Old"}}}}}
	neu := model.Bundle{SchemaVersion: "1", Services: []model.Service{{ID: "aws.a", Operations: []model.Operation{{Name: "New"}}}}}
	dir := t.TempDir()
	oldPath, newPath := filepath.Join(dir, "old.json"), filepath.Join(dir, "new.json")
	for path, bundle := range map[string]model.Bundle{oldPath: old, newPath: neu} {
		raw, _ := json.Marshal(bundle)
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := runDiff([]string{oldPath, newPath}, neu, false); err != nil {
		t.Fatal(err)
	}
	if err := runDiff([]string{oldPath, newPath}, neu, true); err != nil {
		t.Fatal(err)
	}
	if err := runDiff(nil, neu, false); err != nil {
		t.Fatal(err)
	}
	if err := runDiff([]string{"one"}, neu, false); err == nil {
		t.Fatal("diff accepted one path")
	}
	if _, err := readBundle(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("read missing bundle")
	}
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readBundle(bad); err == nil {
		t.Fatal("read malformed bundle")
	}
	services := []model.Service{{ID: "aws.keep"}, {ID: "gcp.keep"}, {ID: "aws.drop"}}
	filtered := filterSet(services, []setEntry{{ID: "aws.keep"}, {ID: "gcp.keep"}, {ID: "aws.missing"}})
	if len(filtered) != 2 || filtered[0].ID != "aws.keep" {
		t.Fatalf("filtered %#v", filtered)
	}
	out := filepath.Join(dir, "generated")
	if err := emitAll(out, filtered); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, "gcp", "keep", "model.go")); err != nil {
		t.Fatal(err)
	}
	provider, pkg := splitID("123-service")
	if provider != "unknown" || pkg != "s123service" {
		t.Fatalf("split %q %q", provider, pkg)
	}
	provider, pkg = splitID("aws.---")
	if provider != "aws" || pkg != "service" {
		t.Fatalf("empty package %q %q", provider, pkg)
	}
	svc := model.Service{ID: "aws.sorted", Operations: []model.Operation{{Name: "Z"}, {Name: "A"}}}
	if raw, err := marshalService(svc); err != nil || !strings.HasSuffix(string(raw), "\n") || strings.Index(string(raw), `"A"`) > strings.Index(string(raw), `"Z"`) {
		t.Fatalf("marshaled %s %v", raw, err)
	}
	if len(sha256Hex([]byte("value"))) != 64 {
		t.Fatal("SHA-256 length")
	}
}
