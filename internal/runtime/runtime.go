// Package runtime assembles SPI dependencies and the HTTP edge.
package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/blobs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bus"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/journal"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/store"
)

// Version is the binary version, overridable by the linker.
var Version = "0.1.0-dev"

// Runtime is a booted emulator process.
type Runtime struct {
	Cfg  config.Config
	Deps spi.Deps
	Reg  registry.Registry
	HTTP *edge.Server
}

// Aliases maps CLI names onto canonical service IDs.
var Aliases = map[string]string{
	"s3":             "aws.s3",
	"aws.s3":         "aws.s3",
	"dynamodb":       "aws.dynamodb",
	"ddb":            "aws.dynamodb",
	"aws.dynamodb":   "aws.dynamodb",
	"sqs":            "aws.sqs",
	"aws.sqs":        "aws.sqs",
	"sns":            "aws.sns",
	"aws.sns":        "aws.sns",
	"sts":            "aws.sts",
	"aws.sts":        "aws.sts",
	"iam":            "aws.iam",
	"aws.iam":        "aws.iam",
	"ssm":            "aws.ssm",
	"aws.ssm":        "aws.ssm",
	"secretsmanager": "aws.secretsmanager",
	"secrets":        "aws.secretsmanager",
	"sm":             "aws.secretsmanager",
	"gcs":            "gcp.storage",
	"storage":        "gcp.storage",
	"gcp.storage":    "gcp.storage",
}

// Profiles are named service sets.
var Profiles = map[string][]string{
	"aws-core": {"aws.s3", "aws.dynamodb", "aws.sqs", "aws.sns", "aws.sts", "aws.iam", "aws.ssm", "aws.secretsmanager"},
	"gcp-core": {"gcp.storage"},
}

// CanonicalServiceID maps a CLI token to a service ID.
func CanonicalServiceID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if id, ok := Aliases[s]; ok {
		return id
	}
	return s
}

// ExpandServices resolves CLI tokens, comma lists, and profiles.
func ExpandServices(args []string, profile string, all bool) []string {
	if all {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	add := func(id string) {
		id = CanonicalServiceID(id)
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	if profile != "" {
		if ids, ok := Profiles[profile]; ok {
			for _, id := range ids {
				add(id)
			}
		}
	}
	for _, a := range args {
		for _, p := range strings.Split(a, ",") {
			add(p)
		}
	}
	return out
}

// Boot constructs dependencies, registry, and HTTP server.
func Boot(cfg config.Config) (*Runtime, error) {
	clk := spi.Clock(clock.Real{})
	if strings.EqualFold(os.Getenv("MIRROR_CLOCK"), "controllable") {
		c := clock.NewControllable()
		_ = c.Advance(1577836800 * 1e9) // 2020-01-01 UTC
		clk = c
	}
	deps := spi.Deps{
		Store:   store.NewMemory(cfg.LockSHA),
		Blobs:   blobs.NewMemory(),
		Bus:     bus.New(),
		Clock:   clk,
		Rand:    rand.New(cfg.Seed),
		Journal: journal.New(),
		Model:   catalog.Bundle(),
	}
	if cfg.PersistDir != "" {
		if err := os.MkdirAll(cfg.PersistDir, 0o755); err != nil {
			return nil, err
		}
		if b, err := os.ReadFile(filepath.Join(cfg.PersistDir, "state.tar")); err == nil {
			if err := Restore(deps, cfg.LockSHA, bytes.NewReader(b)); err != nil {
				return nil, fmt.Errorf("restore persist: %w", err)
			}
		}
	}
	reg, err := registry.New(deps, cfg.Services, cfg.Tiers)
	if err != nil {
		return nil, err
	}
	return &Runtime{Cfg: cfg, Deps: deps, Reg: reg, HTTP: edge.New(cfg, deps, reg, Version)}, nil
}

// Handler is the HTTP handler.
func (rt *Runtime) Handler() http.Handler { return rt.HTTP }

// SavePersist writes process state to cfg.PersistDir when set.
func (rt *Runtime) SavePersist() error {
	if rt.Cfg.PersistDir == "" {
		return nil
	}
	var buf bytes.Buffer
	if err := Snapshot(rt.Deps, rt.Cfg.LockSHA, &buf); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(rt.Cfg.PersistDir, "state.tar"), buf.Bytes(), 0o644)
}

// Snapshot writes store + blobs + lock into one tar archive.
func Snapshot(deps spi.Deps, lockSHA string, w io.Writer) error {
	var storeBuf, blobBuf bytes.Buffer
	if err := deps.Store.Snapshot(context.Background(), &storeBuf); err != nil {
		return err
	}
	if err := deps.Blobs.Snapshot(context.Background(), &blobBuf); err != nil {
		return err
	}
	man, _ := json.Marshal(map[string]string{
		"lockSHA": lockSHA,
		"seed":    "",
	})
	tw := tar.NewWriter(w)
	defer tw.Close()
	for _, f := range []struct {
		name string
		body []byte
	}{
		{"manifest.json", man},
		{"store.tar", storeBuf.Bytes()},
		{"blobs.json", blobBuf.Bytes()},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.body))}); err != nil {
			return err
		}
		if _, err := tw.Write(f.body); err != nil {
			return err
		}
	}
	return nil
}

// Restore loads a Snapshot archive.
func Restore(deps spi.Deps, lockSHA string, r io.Reader) error {
	tr := tar.NewReader(r)
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return err
		}
		files[hdr.Name] = b
	}
	var man struct {
		LockSHA string `json:"lockSHA"`
	}
	if b, ok := files["manifest.json"]; ok {
		_ = json.Unmarshal(b, &man)
	}
	want := lockSHA
	if want != "" && man.LockSHA != "" && man.LockSHA != want {
		return fmt.Errorf("snapshot spec-lock hash %s does not match current %s", man.LockSHA, want)
	}
	if b := files["store.tar"]; len(b) > 0 {
		if err := deps.Store.Restore(context.Background(), bytes.NewReader(b)); err != nil {
			return err
		}
	}
	if b := files["blobs.json"]; len(b) > 0 {
		if err := deps.Blobs.Restore(context.Background(), bytes.NewReader(b)); err != nil {
			return err
		}
	}
	return nil
}

// SupportMatrix renders docs/SUPPORT.md from the catalog and registered factories.
func SupportMatrix() string {
	b := catalog.Bundle()
	emu := map[string]bool{}
	for _, f := range registry.Factories() {
		if f.Tier == model.TierEmulate {
			emu[f.ServiceID] = true
		}
	}
	var sb strings.Builder
	sb.WriteString("# Support matrix\n\n")
	sb.WriteString("Generated by `mirror support-matrix`. Do not hand-edit.\n\n")
	sb.WriteString("Catalog is a bootstrap model until `make specs-sync` + `make generate` replace it with vendored specs.\n\n")
	sb.WriteString("| Service | Protocol | Tier | Operations |\n|---|---|---|---|\n")
	for _, svc := range b.Services {
		tier := "mock"
		if emu[svc.ID] {
			tier = "emulate"
		}
		fmt.Fprintf(&sb, "| `%s` | `%s` | `%s` | %d |\n", svc.ID, svc.Protocol, tier, len(svc.Operations))
	}
	sb.WriteString("\nIAM policy documents are stored and returned verbatim and are **never evaluated** (AllowAll).\n")
	sb.WriteString("SSM `SecureString` uses a reversible local encoding, not real encryption.\n")
	sb.WriteString("Lambda and other compute-execution services return `MirrorNotImplemented`.\n")
	return sb.String()
}
