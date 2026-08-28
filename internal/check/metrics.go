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
func Compare(baseline, current Metrics) (regressions []Regression, newPacks []string) {
	pairs := []struct {
		name string
		b, c int
	}{
		{"packs", baseline.Packs, current.Packs},
		{"case_labels", baseline.CaseLabels, current.CaseLabels},
		{"services_files", baseline.ServicesFiles, current.ServicesFiles},
		{"services_loc", baseline.ServicesLOC, current.ServicesLOC},
		{"fault_sites", baseline.FaultSites, current.FaultSites},
		{"register_sites", baseline.RegisterSites, current.RegisterSites},
	}
	for _, p := range pairs {
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
	return regressions, newPacks
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
