package bundled_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/identity"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestEveryBundleBuilds fails the build rather than the first request when a
// bundle has no generated model, disagrees with the one it has, or is
// otherwise unservable. A data-defined service that cannot start is a CI
// failure by construction, which is the whole reason bundles are validated
// against models instead of interpreted hopefully.
func TestEveryBundleBuilds(t *testing.T) {
	ids := allBundles()
	if len(ids) == 0 {
		t.Fatal("no bundles; behavior/ should hold at least the extracted services")
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			pack, err := bundled.New(id, spitest.Deps(t))
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if pack.ServiceID() != id {
				t.Errorf("bundle reports %q", pack.ServiceID())
			}
			if len(pack.Operations()) == 0 {
				t.Error("serves no operations")
			}
		})
	}
}

// TestBundlesAreRegistered checks the init side: a bundle that loads but never
// reaches the registry would be dead data, and the service it replaced would
// simply be gone.
func TestBundlesAreRegistered(t *testing.T) {
	registered := map[string]int{}
	for _, f := range registry.Factories() {
		registered[f.ServiceID]++
	}
	shadow := bundled.ShadowIDs()
	for _, id := range allBundles() {
		if reason, isShadow := shadow[id]; isShadow {
			// A shadow bundle is gated but not serving, so the pack it
			// shadows is the one in the registry.
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is shadowed with no reason; a shadow bundle that "+
					"does not say what is missing is how a half-migration becomes permanent", id)
			}
			continue
		}
		switch registered[id] {
		case 0:
			t.Errorf("%s has a bundle but is not registered", id)
		case 1:
		default:
			t.Errorf("%s is registered %d times; a service is served by a pack "+
				"or by its bundle, not both", id, registered[id])
		}
	}
}

// allBundles is every bundle under behavior/, serving or shadowed. Both kinds
// must load and build; only the serving ones reach the registry.
func allBundles() []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range bundled.ServiceIDs() {
		seen[id] = true
		out = append(out, id)
	}
	for id := range bundled.ShadowIDs() {
		if !seen[id] {
			out = append(out, id)
		}
	}
	return out
}

// TestShadowBundlesAreStillGated is the rule that keeps shadow honest: a
// bundle that is not serving must still be replayed against the recording of
// the pack that is. Otherwise "proven but not serving" decays into "written
// and unchecked".
func TestShadowBundlesAreStillGated(t *testing.T) {
	for id := range bundled.ShadowIDs() {
		path := filepath.Join("..", "equivalence", "traces", id+".json")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s is shadowed but has no recording at %s", id, path)
		}
	}
}

// TestRegistryRunsDeclaredWorkers holds the worker lifecycle: the registry
// starts a service's worker when it builds the service and stops it on
// Close, and a cross-service call building the bundle starts none.
func TestRegistryRunsDeclaredWorkers(t *testing.T) {
	started, stopped := 0, 0
	bundled.RegisterWorker("aws.sqs", func(spi.Deps) func() error {
		started++
		return func() error { stopped++; return nil }
	})
	deps := spitest.Deps(t)
	if _, err := bundled.New("aws.sqs", deps); err != nil || started != 0 {
		t.Fatalf("bundled.New started a worker (started=%d, err=%v)", started, err)
	}
	r, err := registry.New(deps, []string{"aws.sqs"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if started != 1 {
		t.Fatalf("registry started %d workers, want 1", started)
	}
	if err := r.Close(); err != nil || stopped != 1 {
		t.Fatalf("registry Close stopped %d workers (err=%v), want 1", stopped, err)
	}
}

// TestGlobalResourcesAreSharedAcrossAccounts: an STS credential lives in the
// scope every account shares, so the edge can verify a request signed with it
// knowing only the key, and GetAccessKeyInfo answers the issuer's account to
// a caller in another.
func TestGlobalResourcesAreSharedAcrossAccounts(t *testing.T) {
	deps := spitest.Deps(t)
	sts, err := bundled.New("aws.sts", deps)
	if err != nil {
		t.Fatal(err)
	}
	issuer := spi.Identity{Account: "111111111111", Region: "us-east-1"}
	out, err := sts.Invoke(context.Background(), &spi.Request{Identity: issuer, Operation: "AssumeRole", Input: map[string]any{
		"RoleArn": "arn:aws:iam::111111111111:role/Admin", "RoleSessionName": "s",
	}})
	if err != nil {
		t.Fatal(err)
	}
	creds := out.Output["Credentials"].(map[string]any)
	ak := creds["AccessKeyId"].(string)
	secret, token, temporary := identity.S3Credential(context.Background(), deps.Store, deps.Rand, ak)
	if !temporary || secret != creds["SecretAccessKey"] || token != creds["SessionToken"] {
		t.Fatalf("the edge would not verify the issued credential: secret=%q token=%q temporary=%v", secret, token, temporary)
	}
	info, err := sts.Invoke(context.Background(), &spi.Request{Identity: spi.Identity{Account: "222222222222", Region: "eu-west-1"},
		Operation: "GetAccessKeyInfo", Input: map[string]any{"AccessKeyId": ak}})
	if err != nil || info.Output["Account"] != issuer.Account {
		t.Fatalf("GetAccessKeyInfo from another account = %v, %v; want %s", info, err, issuer.Account)
	}
}
