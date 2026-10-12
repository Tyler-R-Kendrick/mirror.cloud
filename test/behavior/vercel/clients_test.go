package vercel

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"net/http/httptest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

// clientDirEnv names the directory of an installed client toolchain. This suite
// does not fetch one: installing a package is not something a test should do, and
// the machines that run it unattended have no registry. Where the variable is
// not set the test says what to set it to and skips.
const clientDirEnv = "MIRROR_VERCEL_CLIENT_DIR"

// TestVercelsOwnClientsDeploy points Vercel's CLI and its SDK at a booted
// emulator and has each deploy: the clients people actually use, as against the
// curl script and the conformance run that stand in for them everywhere else.
//
// It needs a directory with node_modules holding the `vercel` package (for the
// CLI journey) and `@vercel/sdk` (for the SDK one), and `node` on the PATH.
// test/vercel-clients holds the two scripts; docs/VERCEL.md says how to
// install the toolchain.
func TestVercelsOwnClientsDeploy(t *testing.T) {
	dir := os.Getenv(clientDirEnv)
	if dir == "" {
		t.Skipf("%s is not set: point it at a directory with the vercel package and @vercel/sdk installed under node_modules (see docs/VERCEL.md, \"Real clients\"); these journeys are not run without one", clientDirEnv)
	}
	cfg := config.Default()
	cfg.Services = []string{"vercel.api", "aws.s3"}
	cfg.Seed = "vercel-clients"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	env := append(os.Environ(), "MIRROR_URL="+ts.URL, clientDirEnv+"="+dir)

	scripts, err := filepath.Abs(filepath.Join("..", "..", "vercel-clients"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("sdk", func(t *testing.T) {
		if _, err := os.Stat(filepath.Join(dir, "node_modules", "@vercel", "sdk")); err != nil {
			t.Skipf("%s has no node_modules/@vercel/sdk: install it there first", dir)
		}
		node, err := exec.LookPath("node")
		if err != nil {
			t.Skip("node is not on the PATH")
		}
		// The module resolves @vercel/sdk from the toolchain, so the script runs
		// from there and not from where it lives.
		script := filepath.Join(scripts, "sdk-journey.mjs")
		cp := filepath.Join(dir, "mirror-sdk-journey.mjs")
		body, err := os.ReadFile(script)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cp, body, 0o644); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(cp)
		cmd := exec.Command(node, cp)
		cmd.Dir, cmd.Env = dir, env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("the SDK journey failed: %v\n%s", err, out)
		}
	})

	t.Run("cli", func(t *testing.T) {
		if _, err := os.Stat(filepath.Join(dir, "node_modules", ".bin", "vercel")); err != nil {
			t.Skipf("%s has no node_modules/.bin/vercel: install the vercel package there first", dir)
		}
		cmd := exec.Command("bash", filepath.Join(scripts, "cli-journey.sh"))
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("the CLI journey failed: %v\n%s", err, out)
		}
	})
}
