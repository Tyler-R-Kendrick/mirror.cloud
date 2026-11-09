package testinputs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// What the cache sees is the test log; the property that matters is that
// Command leaves the directory out of it and Record and Packages put the
// inputs in. -test.testlogfile is what go test passes to read it.
func TestTheLogHoldsTheInputsAndNotTheRoot(t *testing.T) {
	if os.Getenv("TESTINPUTS_CHILD") == "1" {
		root, _ := filepath.Abs(filepath.Join("..", ".."))
		Record(t, root, "LICENSE", "scripts")
		Packages(t, root, "./internal/clock")
		if err := Command(root, "true").Run(); err != nil {
			t.Fatal(err)
		}
		return
	}
	log := filepath.Join(t.TempDir(), "log")
	cmd := Command(".", os.Args[0], "-test.run=^TestTheLogHoldsTheInputsAndNotTheRoot$", "-test.testlogfile="+log)
	cmd.Env = append(os.Environ(), "TESTINPUTS_CHILD=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	got := string(raw)
	for _, want := range []string{"open " + root + "/LICENSE\n", "open " + root + "/scripts\n", "open " + root + "/go.sum\n", "open " + root + "/internal/clock\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("the log lacks %q:\n%s", want, got)
		}
	}
	for _, op := range []string{"stat ", "open "} {
		if strings.Contains(got, op+root+"\n") {
			t.Errorf("the log holds %q, which changes with every commit:\n%s", op+root, got)
		}
	}
}

func TestMissingInputsFail(t *testing.T) {
	for _, f := range []func(testing.TB){
		func(tb testing.TB) { Record(tb, t.TempDir(), "absent") },
		func(tb testing.TB) { Packages(tb, t.TempDir(), "./...") },
	} {
		ft := &fatalT{TB: t}
		func() {
			defer func() { recover() }()
			f(ft)
		}()
		if !ft.failed {
			t.Error("a missing input was accepted")
		}
	}
}

type fatalT struct {
	testing.TB
	failed bool
}

func (f *fatalT) Helper() {}
func (f *fatalT) Fatalf(string, ...any) {
	f.failed = true
	panic("fatal")
}

func TestRecordsARealTree(t *testing.T) {
	root, _ := filepath.Abs(filepath.Join("..", ".."))
	Record(t, root, "LICENSE", "scripts")
	Packages(t, root, "./internal/clock")
	pkgs := PackageSet(t, root)
	if len(pkgs) < 50 {
		t.Errorf("PackageSet found %d packages", len(pkgs))
	}
}
