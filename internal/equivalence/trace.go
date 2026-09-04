package equivalence

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

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
	// Superseded states why this step's recorded output is known to be wrong
	// and is deliberately not matched.
	//
	// The recording is what the pack did, and it stays that way: rewriting it
	// to whatever the bundle produces would turn the gate into a mirror. When
	// better evidence contradicts the pack -- most often the operation's own
	// output shape, which is `declared` and outranks a pack's `authored`
	// behavior -- the disagreement is recorded here instead, with the reason,
	// and the step's outcome class (success or fault) is still compared.
	//
	// This is a hole in the gate by construction. It is a visible, reviewable,
	// individually-justified hole, which is the most a two-tier oracle can
	// offer: equivalence gates the migration, evidence gates the truth.
	Superseded string `json:"superseded,omitempty"`
	// SupersededMembers states, per output path, why that one member is not
	// compared. It is the narrow form of Superseded and the one to reach for
	// first: a pack that answers a member its response shape does not declare
	// diverges on that member alone, and excusing the whole step to excuse it
	// throws away every other assertion in the body.
	SupersededMembers map[string]string `json:"superseded_members,omitempty"`
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
	// Identifiers a step produced and a later step consumed become references,
	// so the replay asks each side about the identifiers it issued itself.
	linkInputs(f.Steps)
	return f
}

// Trace converts a recording back into the in-memory form Replay consumes.
func (f *File) Trace() *Trace {
	t := &Trace{}
	for _, e := range f.Steps {
		t.Steps = append(t.Steps, Step{
			Operation: e.Operation, Input: e.Input, Identity: e.Identity,
			Superseded:        e.Superseded != "",
			SupersededMembers: e.SupersededMembers,
		})
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

// Superseded lists the steps whose recorded output is deliberately not
// matched, with the reason for each. A recording with many of these is a
// recording that has stopped gating much, which is why the replay test reports
// the count rather than letting it accumulate quietly.
func (f *File) Superseded() map[int]string {
	out := map[int]string{}
	for i, e := range f.Steps {
		if e.Superseded != "" {
			out[i] = e.Superseded
		}
		// A per-member exemption is a hole in the gate too, a smaller one, and
		// it is reported the same way for the same reason: the only defence
		// against a recording quietly accumulating them is having to read them
		// on every run.
		for _, path := range sortedKeys(e.SupersededMembers) {
			out[i] = strings.TrimSpace(out[i] + "\n  " + path + ": " + e.SupersededMembers[path])
		}
	}
	return out
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// A recorded input may carry a reference to a value an earlier step produced,
// instead of that value:
//
//	{"JobId": {"$fromStep": 2, "$fromPath": "JobId"}}
//
// Without this, a trace cannot gate read-after-create for any resource whose
// identifier is generated: the recording holds the identifier the reference
// produced, the candidate produces a different one, and feeding the recorded
// value back in reads a record that does not exist. Since read-after-create is
// the most common shape there is, a gate that cannot express it is not gating
// much.
//
// References are resolved against the *candidate's* own earlier answers, so
// each side is asked about the identifiers it actually issued.
const (
	fromStepKey = "$fromStep"
	fromPathKey = "$fromPath"
)

// linkInputs rewrites a recording's inputs, replacing any string that an
// earlier step returned with a reference to where it came from. It is applied
// once, at recording time.
//
// A value the caller supplied itself is never linked, however often the service
// echoes it back. The point of a reference is to follow an identifier the
// service issued, and a name the caller chose is not one: linking it makes a
// literal depend on some earlier answer happening to repeat it. That is how a
// step comes to reference an output member a bundle deliberately drops, and the
// input then resolves to nothing -- a failure that reads as though the bundle
// were wrong about the step it broke, several steps away from the echo.
func linkInputs(steps []StepEntry) {
	// Where each produced value was first seen.
	origin := map[string][2]any{}
	// Values the caller has already supplied, which are literals, not
	// identifiers, no matter where they later appear.
	supplied := map[string]bool{}
	for i := range steps {
		steps[i].Input = linkValue(steps[i].Input, origin, supplied).(map[string]any)
		indexSupplied(steps[i].Input, supplied)
		indexOutput(i, "", steps[i].Output, origin)
	}
}

// indexSupplied records every literal string a step's input carries.
func indexSupplied(v any, supplied map[string]bool) {
	switch t := v.(type) {
	case map[string]any:
		if _, _, isRef := asRef(t); isRef {
			return // a reference carries no literal of its own
		}
		for _, vv := range t {
			indexSupplied(vv, supplied)
		}
	case []any:
		for _, vv := range t {
			indexSupplied(vv, supplied)
		}
	case string:
		supplied[t] = true
	}
}

// From names a value an earlier step produced, for use in a recorded input.
func From(step int, path string) map[string]any {
	return map[string]any{fromStepKey: step, fromPathKey: path}
}

// indexOutput records where each identifier-shaped value in an answer came
// from, by dotted path, so a later step can name a nested one.
func indexOutput(step int, prefix string, v any, origin map[string][2]any) {
	switch t := v.(type) {
	case map[string]any:
		for k, vv := range t {
			indexOutput(step, join(prefix, k), vv, origin)
		}
	case []any:
		// An identifier is not always answered at the top of a response:
		// CreateWorkspaces answers the workspace it created inside
		// PendingRequests, and a later step that names that workspace has to
		// be able to point at it.
		for i, vv := range t {
			indexOutput(step, join(prefix, strconv.Itoa(i)), vv, origin)
		}
	case string:
		// Short values are caller-chosen names, not issued identifiers, and
		// linking them would turn a literal into a reference for no reason.
		if len(t) >= 8 && prefix != "" {
			if _, seen := origin[t]; !seen {
				origin[t] = [2]any{step, prefix}
			}
		}
	}
}

func linkValue(v any, origin map[string][2]any, supplied map[string]bool) any {
	switch t := v.(type) {
	case map[string]any:
		if _, _, isRef := asRef(t); isRef {
			return t // already a reference; its contents are not data
		}
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = linkValue(vv, origin, supplied)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = linkValue(vv, origin, supplied)
		}
		return out
	case string:
		if supplied[t] {
			return v
		}
		if from, ok := origin[t]; ok {
			return map[string]any{fromStepKey: from[0], fromPathKey: from[1]}
		}
	}
	return v
}

// resolveInputs replaces references with what the candidate answered earlier.
// A reference that cannot be resolved is left as it is, so the resulting
// divergence names the step rather than disappearing.
func resolveInputs(v any, outcomes []Outcome) any {
	switch t := v.(type) {
	case map[string]any:
		if step, path, ok := asRef(t); ok {
			if step >= 0 && step < len(outcomes) {
				if got, ok := lookupPath(outcomes[step].Output, path); ok {
					return got
				}
			}
			return nil
		}
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = resolveInputs(vv, outcomes)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, vv := range t {
			out[i] = resolveInputs(vv, outcomes)
		}
		return out
	}
	return v
}

func asRef(m map[string]any) (step int, path string, ok bool) {
	if len(m) != 2 {
		return 0, "", false
	}
	raw, hasStep := m[fromStepKey]
	p, hasPath := m[fromPathKey]
	if !hasStep || !hasPath {
		return 0, "", false
	}
	switch n := raw.(type) {
	case int:
		step = n
	case float64:
		step = int(n)
	default:
		return 0, "", false
	}
	s, isStr := p.(string)
	return step, s, isStr
}

// lookupPath follows a dotted path into an answer.
func lookupPath(out map[string]any, path string) (any, bool) {
	var cur any = out
	for _, part := range strings.Split(path, ".") {
		switch c := cur.(type) {
		case map[string]any:
			v, ok := c[part]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			// A numeric segment indexes a list, which is how a reference
			// reaches an identifier answered inside one.
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			cur = c[i]
		default:
			return nil, false
		}
	}
	return cur, true
}
