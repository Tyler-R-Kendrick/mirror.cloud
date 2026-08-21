package main

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	raw, err := os.ReadFile(filepath.Join(dir, "aws", "s3", "model.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got model.Service
	if err := json.Unmarshal(raw, &got); err != nil {
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

	if err := emitService(dir, *svc); err != nil {
		t.Fatal(err)
	}
	raw2, err := os.ReadFile(filepath.Join(dir, "aws", "s3", "model.json"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(raw2) {
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
