package behaviors_test

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// TestEveryBundleLoads is the gate that keeps behavior data honest: every
// committed bundle must load and validate against the generated model for its
// service. A bundle that names a member the wire protocol cannot carry, or an
// operation the service does not have, fails here rather than at a request.
func TestEveryBundleLoads(t *testing.T) {
	ids := behaviors.ServiceIDs()
	if len(ids) == 0 {
		t.Fatal("no behavior bundles found")
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			svc := generatedModel(t, id)
			b, err := behaviors.Load(id, svc)
			if err != nil {
				t.Fatalf("bundle failed to load:\n%v", err)
			}
			if b.ServiceID != id {
				t.Fatalf("bundle declares %q, loaded as %q", b.ServiceID, id)
			}
			if b.Compiled == nil {
				t.Fatal("bundle loaded without compiled expressions")
			}
			for name := range b.Operations {
				if !hasOperation(svc, name) {
					t.Errorf("operation %s is not in the generated model", name)
				}
			}
		})
	}
}

// generatedModel reads the committed model for a service straight from
// internal/generated, so this test exercises the same artifact the runtime
// will consume rather than a fixture.
func generatedModel(t *testing.T, serviceID string) *model.Service {
	t.Helper()
	provider, service := split(serviceID)
	var pkg []rune
	for _, r := range service {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			pkg = append(pkg, r)
		}
	}
	path := filepath.Join("..", "internal", "generated", provider, string(pkg), "model.json.gz")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("no generated model for %s at %s: %v\n"+
			"Run: make specs-sync && make generate", serviceID, path, err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	var svc model.Service
	if err := json.NewDecoder(zr).Decode(&svc); err != nil {
		t.Fatal(err)
	}
	return &svc
}

func hasOperation(svc *model.Service, name string) bool {
	for _, op := range svc.Operations {
		if op.Name == name {
			return true
		}
	}
	return false
}

func split(id string) (string, string) {
	for i := 0; i < len(id); i++ {
		if id[i] == '.' {
			return id[:i], id[i+1:]
		}
	}
	return "", id
}
