// Package specdiff reports semantic API-surface differences between Bundles.
package specdiff

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Report is a machine-readable API-surface diff.
type Report struct {
	AddedOps      []string `json:"added_operations"`
	RemovedOps    []string `json:"removed_operations"`
	ChangedOps    []string `json:"changed_operations"`
	AddedShapes   []string `json:"added_shapes"`
	RemovedShapes []string `json:"removed_shapes"`
	ChangedShapes []string `json:"changed_shapes"`
}

// Diff compares a and b.
func Diff(a, b model.Bundle) Report {
	var r Report
	opsA := ops(a)
	opsB := ops(b)
	for k := range opsB {
		if _, ok := opsA[k]; !ok {
			r.AddedOps = append(r.AddedOps, k)
		}
	}
	for k, oa := range opsA {
		ob, ok := opsB[k]
		if !ok {
			r.RemovedOps = append(r.RemovedOps, k)
			continue
		}
		if oa.Input != ob.Input || oa.Output != ob.Output || strings.Join(oa.Errors, ",") != strings.Join(ob.Errors, ",") {
			r.ChangedOps = append(r.ChangedOps, k)
		}
	}
	shA := shapes(a)
	shB := shapes(b)
	for k := range shB {
		if _, ok := shA[k]; !ok {
			r.AddedShapes = append(r.AddedShapes, k)
		}
	}
	for k, sa := range shA {
		sb, ok := shB[k]
		if !ok {
			r.RemovedShapes = append(r.RemovedShapes, k)
			continue
		}
		if shapeChanged(sa, sb) {
			r.ChangedShapes = append(r.ChangedShapes, k)
		}
	}
	sort.Strings(r.AddedOps)
	sort.Strings(r.RemovedOps)
	sort.Strings(r.ChangedOps)
	sort.Strings(r.AddedShapes)
	sort.Strings(r.RemovedShapes)
	sort.Strings(r.ChangedShapes)
	return r
}

// String is the human-readable form.
func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "added ops: %s\nremoved ops: %s\nchanged ops: %s\nadded shapes: %s\nremoved shapes: %s\nchanged shapes: %s\n",
		strings.Join(r.AddedOps, ", "), strings.Join(r.RemovedOps, ", "), strings.Join(r.ChangedOps, ", "),
		strings.Join(r.AddedShapes, ", "), strings.Join(r.RemovedShapes, ", "), strings.Join(r.ChangedShapes, ", "))
	return b.String()
}

// JSON is the machine-readable form.
func (r Report) JSON() string {
	b, _ := json.MarshalIndent(r, "", "  ")
	return string(b)
}

// WriteJSON writes the machine-readable form.
func (r Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func ops(b model.Bundle) map[string]model.Operation {
	m := map[string]model.Operation{}
	for _, s := range b.Services {
		for _, op := range s.Operations {
			m[s.ID+"."+op.Name] = op
		}
	}
	return m
}

func shapes(b model.Bundle) map[string]model.Shape {
	m := map[string]model.Shape{}
	for _, s := range b.Services {
		for id, sh := range s.Shapes {
			m[s.ID+"."+id] = sh
		}
	}
	return m
}

func shapeChanged(a, b model.Shape) bool {
	if a.Kind != b.Kind {
		return true
	}
	if len(a.Members) != len(b.Members) {
		return true
	}
	for n, ma := range a.Members {
		mb, ok := b.Members[n]
		if !ok || ma.Required != mb.Required || ma.Shape != mb.Shape {
			return true
		}
	}
	return false
}
