package equivalence

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"sort"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// TraceSchema versions the on-disk format.
const TraceSchema = "trace/1"

// File is a recorded trace as it is committed to the repository.
//
// A trace outlives the handler it was recorded from, which is the point: the
// hand-written pack is the only description of its own semantics, so before it
// is deleted its answers are frozen here. Afterwards the engine is replayed
// against the recording rather than against the pack, and the gate keeps
// working when the pack is gone.
//
// Source and Note carry provenance. A recording is evidence about a pack, not
// about the real service — when probed evidence later contradicts it, the
// corpus wins and this file is updated deliberately, citing the recording.
type File struct {
	Schema  string      `json:"schema"`
	Service string      `json:"service"`
	Source  string      `json:"source"`
	Note    string      `json:"note,omitempty"`
	Steps   []StepEntry `json:"steps"`
}

// StepEntry pairs one request with the outcome the reference produced.
type StepEntry struct {
	Operation string         `json:"operation"`
	Input     map[string]any `json:"input"`
	Identity  spi.Identity   `json:"identity"`
	Output    map[string]any `json:"output,omitempty"`
	Fault     *FaultEntry    `json:"fault,omitempty"`
}

// FaultEntry is the comparable part of a fault. The message is deliberately
// excluded: wording is not behavior, and holding it fixed would block
// legitimate improvements to diagnostics.
type FaultEntry struct {
	Code       string `json:"code"`
	HTTPStatus int    `json:"http_status"`
	Fault      string `json:"fault"`
}

// NewFile builds a committable recording from a trace.
func NewFile(service, source, note string, t *Trace) *File {
	f := &File{Schema: TraceSchema, Service: service, Source: source, Note: note}
	for i, s := range t.Steps {
		e := StepEntry{Operation: s.Operation, Input: s.Input, Identity: s.Identity}
		if i < len(t.Outcomes) {
			out := t.Outcomes[i]
			e.Output = out.Output
			if out.Fault != nil {
				e.Fault = &FaultEntry{
					Code:       out.Fault.Code,
					HTTPStatus: out.Fault.HTTPStatus,
					Fault:      out.Fault.Fault,
				}
			}
		}
		f.Steps = append(f.Steps, e)
	}
	return f
}

// Trace converts a recording back into the in-memory form Replay consumes.
func (f *File) Trace() *Trace {
	t := &Trace{}
	for _, e := range f.Steps {
		t.Steps = append(t.Steps, Step{Operation: e.Operation, Input: e.Input, Identity: e.Identity})
		out := Outcome{Output: e.Output}
		if e.Fault != nil {
			out.Fault = &spi.Fault{
				Code:       e.Fault.Code,
				HTTPStatus: e.Fault.HTTPStatus,
				Fault:      e.Fault.Fault,
			}
		}
		t.Outcomes = append(t.Outcomes, out)
	}
	return t
}

// Marshal renders a recording as the bytes to commit: stable, indented and
// newline-terminated, so a re-recording produces a reviewable diff.
func (f *File) Marshal() ([]byte, error) {
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// LoadFile reads one recording.
func LoadFile(fsys fs.FS, name string) (*File, error) {
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, err
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if f.Schema != TraceSchema {
		return nil, fmt.Errorf("%s: schema %q, want %q", name, f.Schema, TraceSchema)
	}
	if f.Service == "" {
		return nil, fmt.Errorf("%s: no service", name)
	}
	if len(f.Steps) == 0 {
		return nil, fmt.Errorf("%s: no steps; a recording that asserts nothing gates nothing", name)
	}
	return &f, nil
}

// LoadDir reads every recording in a directory, sorted by service ID.
func LoadDir(fsys fs.FS, dir string) ([]*File, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	var out []*File
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".json" {
			continue
		}
		f, err := LoadFile(fsys, path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out, nil
}
