package check

import (
	"compress/gzip"
	"encoding/json"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// These tests guard the committed generated models. They need no network and
// no vendored specs: specs/ is reproducible from specs/mirror.lock, but
// internal/generated is the artifact the rest of the system consumes, so it is
// committed and checked here.

func generatedRoot(t *testing.T) string {
	t.Helper()
	root := findMod(t)
	dir := filepath.Join(root, "internal", "generated")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("internal/generated is missing; run: make specs-sync && make generate (%v)", err)
	}
	return dir
}

// loadGenerated returns every committed service model, keyed by service ID.
func loadGenerated(t *testing.T) map[string]*model.Service {
	t.Helper()
	dir := generatedRoot(t)
	out := map[string]*model.Service{}
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Base(path) != "model.json.gz" {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer zr.Close()
		var svc model.Service
		if err := json.NewDecoder(zr).Decode(&svc); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if prev, dup := out[svc.ID]; dup {
			t.Errorf("service %s generated twice (%s and this one)", svc.ID, prev.Source.Path)
		}
		out[svc.ID] = &svc
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestGeneratedShapesAreNotEmpty is the gate that would have caught the
// bootstrap catalog: it shipped `Shapes: map[string]model.Shape{}` for every
// service, which silently disabled required-member validation and made
// schema-driven synthesis impossible. A model without shapes is not a model.
func TestGeneratedShapesAreNotEmpty(t *testing.T) {
	svcs := loadGenerated(t)
	if len(svcs) == 0 {
		t.Fatal("no generated services found")
	}
	var empty []string
	for id, svc := range svcs {
		if len(svc.Shapes) == 0 {
			empty = append(empty, id)
		}
		if len(svc.Operations) == 0 {
			t.Errorf("%s has no operations", id)
		}
	}
	sort.Strings(empty)
	if len(empty) > 0 {
		t.Errorf("%d generated service(s) carry no shapes: %s", len(empty), strings.Join(empty, ", "))
	}
}

// TestGeneratedCoversDeclaredSet checks that every service in specs/mirror.set
// was generated, except those specs/aws-dirs.json records as having no
// upstream model at all. Before this gate, mirror.set declared 152 services
// while the sync script could reach 28 and reported success anyway.
func TestGeneratedCoversDeclaredSet(t *testing.T) {
	root := findMod(t)
	svcs := loadGenerated(t)

	setBytes, err := os.ReadFile(filepath.Join(root, "specs", "mirror.set"))
	if err != nil {
		t.Fatal(err)
	}
	dirsBytes, err := os.ReadFile(filepath.Join(root, "specs", "aws-dirs.json"))
	if err != nil {
		t.Fatal(err)
	}
	var dirs struct {
		Unavailable map[string]string `json:"unavailable"`
	}
	if err := json.Unmarshal(dirsBytes, &dirs); err != nil {
		t.Fatal(err)
	}

	var missing []string
	for _, line := range strings.Split(string(setBytes), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		id := strings.Fields(line)[0]
		if _, ok := svcs[id]; ok {
			continue
		}
		if reason, excused := dirs.Unavailable[id]; excused {
			t.Logf("%s: no generated model — %s", id, reason)
			continue
		}
		missing = append(missing, id)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("%d service(s) in specs/mirror.set have no generated model: %s\n"+
			"Run `make specs-sync && make generate`, or record the service in the "+
			"\"unavailable\" map of specs/aws-dirs.json with a reason.",
			len(missing), strings.Join(missing, ", "))
	}
}

// TestGeneratedIDsAreCanonical checks that the ID inside each model matches
// the package path it was generated into, so a model cannot be served under
// one identity and validated under another.
func TestGeneratedIDsAreCanonical(t *testing.T) {
	dir := generatedRoot(t)
	svcs := loadGenerated(t)
	for id, svc := range svcs {
		provider, rest, ok := strings.Cut(id, ".")
		if !ok {
			t.Errorf("service ID %q has no provider prefix", id)
			continue
		}
		var pkg strings.Builder
		for _, r := range strings.ToLower(rest) {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				pkg.WriteRune(r)
			}
		}
		want := filepath.Join(dir, provider, pkg.String(), "model.json.gz")
		if _, err := os.Stat(want); err != nil {
			t.Errorf("%s: expected model at %s: %v", id, want, err)
		}
		if svc.Protocol == "" {
			t.Errorf("%s: no protocol", id)
		}
	}
}

// TestEveryGeneratedModelIsEmbedded catches the failure with no symptom, the
// mirror image of TestEveryBundleOnDiskIsEmbedded.
//
// internal/generated embeds its models with a //go:embed pattern. That pattern
// was `all:aws all:gcp` -- every provider that existed when it was written --
// so a model mirrorgen generated for a third provider was written to disk and
// committed, and generated.Model still answered "no model for hostinger.api"
// with the file sitting in the tree. Nothing failed at build time, because an
// unembedded directory is indistinguishable from a provider with no models.
func TestEveryGeneratedModelIsEmbedded(t *testing.T) {
	root := generatedRoot(t)
	providers, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, provider := range providers {
		if !provider.IsDir() {
			continue
		}
		services, err := os.ReadDir(filepath.Join(root, provider.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, service := range services {
			if !service.IsDir() {
				continue
			}
			rel := path.Join(provider.Name(), service.Name(), "model.json.gz")
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
				continue
			}
			found++
			if _, err := fs.Stat(generated.FS(), rel); err != nil {
				t.Errorf("internal/generated/%s is committed but not embedded; "+
					"the //go:embed pattern in index.go does not match it", rel)
			}
		}
	}
	if found == 0 {
		t.Fatal("no generated models found on disk; this test would pass vacuously")
	}
}
