// Package behaviors embeds the Behavior IR bundles under behavior/ and loads
// them against the generated models.
//
// Bundles are data, so they ship in the binary the same way the models do:
// nothing is read from disk at runtime, and a bundle that fails validation
// fails the build's tests rather than the first request that touches it.
package behaviors

import (
	"embed"
	"io/fs"
	"path"
	"sort"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

//go:embed all:aws all:gcp
var files embed.FS

// FS exposes the embedded bundles for tools that want to walk them.
func FS() fs.FS { return files }

// ServiceIDs returns every service that has a bundle, sorted.
//
// The directory layout is behavior/<provider>/<service>/, and the service ID
// is <provider>.<service> — the same identity the generated models use, so a
// bundle and its model cannot be mismatched by accident.
func ServiceIDs() []string {
	var out []string
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil
	}
	for _, provider := range entries {
		if !provider.IsDir() {
			continue
		}
		svcs, err := fs.ReadDir(files, provider.Name())
		if err != nil {
			continue
		}
		for _, svc := range svcs {
			if svc.IsDir() {
				out = append(out, provider.Name()+"."+svc.Name())
			}
		}
	}
	sort.Strings(out)
	return out
}

// Dir returns the embedded directory holding a service's bundle.
func Dir(serviceID string) string {
	provider, service, ok := cut(serviceID)
	if !ok {
		return ""
	}
	return path.Join(provider, service)
}

// Load reads and validates one service's bundle against its generated model.
func Load(serviceID string, svc *model.Service) (*bir.Service, error) {
	return bir.Load(files, Dir(serviceID), svc)
}

// Has reports whether a bundle exists for the service.
func Has(serviceID string) bool {
	dir := Dir(serviceID)
	if dir == "" {
		return false
	}
	info, err := fs.Stat(files, dir)
	return err == nil && info.IsDir()
}

func cut(id string) (provider, service string, ok bool) {
	for i := 0; i < len(id); i++ {
		if id[i] == '.' {
			return id[:i], id[i+1:], true
		}
	}
	return "", "", false
}
