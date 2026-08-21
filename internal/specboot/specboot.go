// Package specboot loads the process model from vendored specs when
// present, otherwise the bootstrap catalog.
package specboot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/fusion"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver/aws/smithy"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver/gcp/discovery"
)

var (
	once sync.Once
	got  *model.Bundle
)

// Bundle returns ingested specs when specs/*.json exist, else catalog.Bundle.
func Bundle() *model.Bundle {
	once.Do(func() {
		root := findSpecs()
		b, n := ingest(root)
		if n == 0 || b == nil || len(b.Services) == 0 {
			got = catalog.Bundle()
			return
		}
		got = b
		for _, s := range catalog.Bundle().Services {
			existing := got.ServiceByID(s.ID)
			if existing == nil {
				got.Services = append(got.Services, s)
				continue
			}
			have := map[string]bool{}
			for _, op := range existing.Operations {
				have[op.Name] = true
			}
			for _, op := range s.Operations {
				if !have[op.Name] {
					existing.Operations = append(existing.Operations, op)
				}
			}
		}
	})
	return got
}

func findSpecs() string {
	wd, _ := os.Getwd()
	for p := wd; p != "/" && p != ""; p = filepath.Dir(p) {
		cand := filepath.Join(p, "specs")
		if st, err := os.Stat(cand); err == nil && st.IsDir() {
			return cand
		}
		if _, err := os.Stat(filepath.Join(p, "go.mod")); err == nil {
			return filepath.Join(p, "specs")
		}
	}
	return "specs"
}

func ingest(root string) (*model.Bundle, int) {
	var aws, gcp [][]model.Service
	n := 0
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		if strings.HasSuffix(path, "mirror.lock") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil || len(raw) == 0 {
			return nil
		}
		src := model.SourceRef{Path: path}
		head := raw
		if len(head) > 4096 {
			head = head[:4096]
		}
		var got []model.Service
		switch {
		case (smithy.Receiver{}).Detect(path, head):
			got, _ = (smithy.Receiver{}).Ingest(context.Background(), src, raw)
		case (discovery.Receiver{}).Detect(path, head):
			got, _ = (discovery.Receiver{}).Ingest(context.Background(), src, raw)
		}
		if len(got) == 0 {
			return nil
		}
		n++
		if strings.HasPrefix(got[0].ID, "gcp.") {
			gcp = append(gcp, got)
		} else {
			aws = append(aws, got)
		}
		return nil
	})
	if n == 0 {
		return nil, 0
	}
	var svcs []model.Service
	if len(aws) > 0 {
		fused, _, err := fusion.Fuse(context.Background(), model.ProviderAWS, aws)
		if err == nil {
			svcs = append(svcs, fused.Services...)
		}
	}
	if len(gcp) > 0 {
		fused, _, err := fusion.Fuse(context.Background(), model.ProviderGCP, gcp)
		if err == nil {
			svcs = append(svcs, fused.Services...)
		}
	}
	return &model.Bundle{SchemaVersion: "1", Services: svcs}, n
}
