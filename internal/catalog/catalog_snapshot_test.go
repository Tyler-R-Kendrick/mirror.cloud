package catalog

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
)

func TestBundleCharacterization(t *testing.T) {
	b := Bundle()
	type row struct {
		ID       string
		Protocol string
		Ops      []string
	}
	rows := make([]row, 0, len(b.Services))
	for _, s := range b.Services {
		ops := make([]string, len(s.Operations))
		for i, o := range s.Operations {
			ops[i] = o.Name
		}
		rows = append(rows, row{ID: s.ID, Protocol: string(s.Protocol), Ops: ops})
	}
	golden.AssertJSON(t, rows)
}
