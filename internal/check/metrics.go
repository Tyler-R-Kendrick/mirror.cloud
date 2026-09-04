// Package check holds repository-level invariant checks. Most run as tests;
// this file is the measurement half of the anti-drift ratchet, shared by
// ratchet_test.go and cmd/ratchet.
//
// The ratchet exists because the hand-written service surface is scheduled for
// deletion, not growth: docs/MASTER_PROMPT_V2.md §5 requires every metric here
// to move in one direction only. Behavior arrives as data under behavior/, not
// as another Go pack.
package check

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Metrics counts the hand-written service surface. Every field is
// monotonically non-increasing across commits; see Compare.
type Metrics struct {
	// Packs is the number of directories under internal/services that
	// register a behavior pack.
	Packs int `json:"packs"`
	// CaseLabels counts `case "Op":` dispatch arms — the clearest signal of
	// hand-written per-operation behavior.
	CaseLabels int `json:"case_labels"`
	// ServicesFiles and ServicesLOC size the non-test Go under
	// internal/services.
	ServicesFiles int `json:"services_files"`
	ServicesLOC   int `json:"services_loc"`
	// FaultSites counts inline &spi.Fault{} constructions. These must
	// collapse into the model-seeded error table, so the count only falls.
	FaultSites int `json:"fault_sites"`
	// RegisterSites counts registry.Register call sites.
	RegisterSites int `json:"register_sites"`
	// PackDirs is the sorted allowlist of pack directories, relative to
	// internal/services. A directory absent here is a new hand-written pack.
	PackDirs []string `json:"pack_dirs"`

	// RoutingMismatches counts behavior bundles the runtime would route
	// differently from what the vendored specification says -- a different
	// protocol, a different X-Amz-Target prefix, or both.
	//
	// This is not a style metric like the others; it is a live defect with a
	// number attached. A bundle is validated against the generated model --
	// aws.guardduty is restJson1 with ninety operations -- and served through
	// the booted catalog, which describes it as awsJson1_1 with twenty. The
	// consequence is that no real SDK can reach those services: it sends
	// `GET /detector` and the edge answers only `POST /` with an X-Amz-Target.
	//
	// It is a ratchet rather than a failing test because fifty-three services
	// are in this state and fixing them is an edge change, not a bundle
	// change. What the ratchet buys is that the number cannot grow while that
	// work is outstanding, and that a service leaving the list cannot silently
	// rejoin it.
	RoutingMismatches int `json:"routing_mismatches"`
	// RoutingMismatchServices is the sorted list behind that count. A service
	// absent here that mismatches is a new one, which is the case worth
	// failing on: the count alone would let a fix and a regression cancel.
	RoutingMismatchServices []string `json:"routing_mismatch_services"`
}

const (
	servicesRel  = "internal/services"
	ratchetFile  = "ratchet.json"
	caseNeedle   = `case "`
	faultNeedle  = "&spi.Fault{"
	regNeedle    = "registry.Register("
	goSuffix     = ".go"
	testSuffix   = "_test.go"
	modMarker    = "go.mod"
	pathSepSlash = "/"
)

// ModRoot walks up from dir to the directory holding go.mod.
func ModRoot(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(abs, modMarker)); err == nil {
			return abs, nil
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", errors.New("check: no go.mod found above " + dir)
		}
		abs = parent
	}
}

// Measure counts the hand-written service surface rooted at the module root.
// Only non-test Go files under internal/services are counted: tests are the
// migration oracle and are expected to grow, not shrink.
func Measure(root string) (Metrics, error) {
	var m Metrics
	svcRoot := filepath.Join(root, filepath.FromSlash(servicesRel))
	packs := map[string]bool{}

	err := filepath.WalkDir(svcRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // no packs left: the goal state
			}
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, goSuffix) || strings.HasSuffix(path, testSuffix) {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		src := string(b)

		m.ServicesFiles++
		m.ServicesLOC += strings.Count(src, "\n")
		m.FaultSites += strings.Count(src, faultNeedle)
		regs := strings.Count(src, regNeedle)
		m.RegisterSites += regs
		for _, line := range strings.Split(src, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), caseNeedle) {
				m.CaseLabels++
			}
		}
		if regs > 0 {
			rel, relErr := filepath.Rel(svcRoot, filepath.Dir(path))
			if relErr != nil {
				return relErr
			}
			packs[filepath.ToSlash(rel)] = true
		}
		return nil
	})
	if err != nil {
		return Metrics{}, err
	}

	m.PackDirs = make([]string, 0, len(packs))
	for p := range packs {
		m.PackDirs = append(m.PackDirs, p)
	}
	sort.Strings(m.PackDirs)
	m.Packs = len(m.PackDirs)

	// The one metric that is not a count of source text: how many bundles the
	// runtime serves in a protocol the specification disagrees with.
	served, spec := ServedAndSpecRouting()
	m.RoutingMismatches, m.RoutingMismatchServices = MeasureRouting(served, spec)
	return m, nil
}

// LoadBaseline reads ratchet.json from the module root.
func LoadBaseline(root string) (Metrics, error) {
	b, err := os.ReadFile(filepath.Join(root, ratchetFile))
	if err != nil {
		return Metrics{}, err
	}
	var m Metrics
	if err := json.Unmarshal(b, &m); err != nil {
		return Metrics{}, err
	}
	return m, nil
}

// WriteBaseline writes ratchet.json to the module root, formatted stably.
func WriteBaseline(root string, m Metrics) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, ratchetFile), append(b, '\n'), 0o644)
}

// Regression describes one metric that moved the wrong way.
type Regression struct {
	Metric   string
	Baseline int
	Current  int
}

func (r Regression) String() string {
	return r.Metric + ": baseline " + itoa(r.Baseline) + ", now " + itoa(r.Current)
}

// Compare reports every counted metric where current exceeds baseline, plus
// any pack directory that is not in the baseline allowlist.
// NotComparable marks a metric absent from the baseline being compared
// against, which happens exactly once per metric: on the commit that adds it.
const NotComparable = -1

// comparable pairs every ratcheted metric with its name, so both Compare and
// the baseline guard work from one list rather than each carrying its own.
func comparable(baseline, current Metrics) []struct {
	name string
	b, c int
} {
	return []struct {
		name string
		b, c int
	}{
		{"packs", baseline.Packs, current.Packs},
		{"case_labels", baseline.CaseLabels, current.CaseLabels},
		{"services_files", baseline.ServicesFiles, current.ServicesFiles},
		{"services_loc", baseline.ServicesLOC, current.ServicesLOC},
		{"fault_sites", baseline.FaultSites, current.FaultSites},
		{"register_sites", baseline.RegisterSites, current.RegisterSites},
		{"routing_mismatches", baseline.RoutingMismatches, current.RoutingMismatches},
	}
}

// MetricNames lists the ratcheted metrics by their JSON key. The baseline
// guard uses it to tell a metric this commit *introduces* from one it raised:
// a metric absent from the base's ratchet.json has nothing to have regressed
// from, and treating its first value as a regression would mean the only
// metrics that can ever be added are the ones that start at zero -- exactly
// the ones not worth adding.
func MetricNames() []string {
	var out []string
	for _, p := range comparable(Metrics{}, Metrics{}) {
		out = append(out, p.name)
	}
	return out
}

// ClearAbsent marks every metric the base's ratchet.json did not carry as
// NotComparable, so Compare skips it. Driven by the raw JSON keys rather than
// by a per-metric special case, so adding a metric needs no change here.
func ClearAbsent(m *Metrics, present map[string]json.RawMessage) {
	for _, name := range MetricNames() {
		if _, ok := present[name]; ok {
			continue
		}
		switch name {
		case "packs":
			m.Packs = NotComparable
		case "case_labels":
			m.CaseLabels = NotComparable
		case "services_files":
			m.ServicesFiles = NotComparable
		case "services_loc":
			m.ServicesLOC = NotComparable
		case "fault_sites":
			m.FaultSites = NotComparable
		case "register_sites":
			m.RegisterSites = NotComparable
		case "routing_mismatches":
			m.RoutingMismatches, m.RoutingMismatchServices = NotComparable, nil
		}
	}
}

func Compare(baseline, current Metrics) (regressions []Regression, newPacks []string) {
	for _, p := range comparable(baseline, current) {
		// A baseline of NotComparable marks a metric the base's ratchet.json
		// did not carry, so this commit introduces it and there is nothing
		// to compare against.
		if p.b == NotComparable {
			continue
		}
		if p.c > p.b {
			regressions = append(regressions, Regression{Metric: p.name, Baseline: p.b, Current: p.c})
		}
	}
	allowed := make(map[string]bool, len(baseline.PackDirs))
	for _, d := range baseline.PackDirs {
		allowed[d] = true
	}
	for _, d := range current.PackDirs {
		if !allowed[d] {
			newPacks = append(newPacks, d)
		}
	}
	if baseline.RoutingMismatches == NotComparable {
		return regressions, newPacks
	}
	known := make(map[string]bool, len(baseline.RoutingMismatchServices))
	for _, id := range baseline.RoutingMismatchServices {
		known[id] = true
	}
	for _, id := range current.RoutingMismatchServices {
		if !known[id] {
			regressions = append(regressions, Regression{
				Metric:   "routing_mismatch/" + id,
				Baseline: 0, Current: 1,
			})
		}
	}
	return regressions, newPacks
}

// MeasureRouting compares, for every behavior bundle, how the runtime would
// route a request to it against how the vendored specification says it should
// be routed. It is separate from Measure because it reads the built packages
// rather than the source tree.
func MeasureRouting(served map[string]string, spec map[string]string) (int, []string) {
	var differ []string
	for id, got := range served {
		want, ok := spec[id]
		if !ok || got == want {
			continue
		}
		differ = append(differ, id)
	}
	sort.Strings(differ)
	return len(differ), differ
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
