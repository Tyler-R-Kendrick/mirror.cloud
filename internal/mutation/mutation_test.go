package mutation

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestMutantsAreKilled is a tiny in-tree mutation suite. Each mutant
// rewrites one production token via `go test -overlay` and must make
// the targeted tests fail. A surviving mutant means the tests are blind.
func TestMutantsAreKilled(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))

	type mutant struct {
		name string
		file string
		old  string
		new  string
		pkg  string
		run  string
	}
	mutants := []mutant{
		{
			name: "expr-and-to-or",
			file: filepath.Join("internal", "services", "dynamodb", "expr", "expr.go"),
			old:  `case "AND":`,
			new:  `case "AND_MUTATED":`,
			pkg:  "./internal/services/dynamodb/expr",
			run:  "TestANDNotOR|TestEvalBool",
		},
		{
			name: "expr-eq-to-neq",
			file: filepath.Join("internal", "services", "dynamodb", "expr", "expr.go"),
			old:  "return cmp == 0, nil",
			new:  "return cmp != 0, nil",
			pkg:  "./internal/services/dynamodb/expr",
			run:  "TestEquals",
		},
		{
			name: "fault-501-to-500",
			file: filepath.Join("internal", "spi", "spi.go"),
			old:  "HTTPStatus: 501,",
			new:  "HTTPStatus: 500,",
			pkg:  "./internal/conformance",
			run:  "TestFaultErrorString|TestCodecRoundTripAndFaultEnvelope",
		},
	}

	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			src := filepath.Join(root, m.file)
			body, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), m.old) {
				t.Fatalf("needle not found in %s: %q", m.file, m.old)
			}
			mutated := strings.Replace(string(body), m.old, m.new, 1)
			dir := t.TempDir()
			dst := filepath.Join(dir, filepath.Base(m.file))
			if err := os.WriteFile(dst, []byte(mutated), 0o644); err != nil {
				t.Fatal(err)
			}
			overlay := filepath.Join(dir, "overlay.json")
			enc, _ := json.Marshal(map[string]any{"Replace": map[string]string{src: dst}})
			if err := os.WriteFile(overlay, enc, 0o644); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("go", "test", m.pkg, "-count=1", "-run", m.run, "-overlay", overlay)
			cmd.Dir = root
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("mutant survived (tests still passed)\n%s", out)
			}
		})
	}
}
