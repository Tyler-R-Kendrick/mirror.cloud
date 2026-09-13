package check

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDemuxGuessesAreTheOnesC34Names pins what the metric counts against the
// eight branches the critique enumerates.
//
// The list matters more than the number. A count alone lets one provider's
// guess be deleted while another's is added, which is the shape of the drift
// this measures: every provider so far has entered the demux as a substring
// guess, so the count grows by one per provider unless something is watching.
func TestDemuxGuessesAreTheOnesC34Names(t *testing.T) {
	root, err := ModRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	n, names, err := MeasureDemuxGuesses(root)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(names) {
		t.Fatalf("count %d does not match the %d names returned", n, len(names))
	}
	got := map[string]bool{}
	for _, name := range names {
		got[name] = true
	}
	// Every branch C34 enumerates, by name.
	for _, want := range []string{
		"azureRequest", "cloudflareRequest", "digitaloceanRequest", "flyRequest",
		"hetznerRequest", "hostingerRequest", "railwayRequest", "vercelService",
	} {
		if !got[want] {
			t.Errorf("%s is a provider guess in internal/edge but is not counted; have %v", want, names)
		}
	}
	// The demux's own helpers are not provider guesses and must not be
	// counted. Each of these ends in Request or Service, so they are exactly
	// what a suffix test alone would over-count.
	for _, never := range []string{
		"claimingRequest", "sdkRequest", "credentialScopeService",
		"streamingSignatureRequest", "streamingTrailerSignatureRequest",
		"streamingUnsignedTrailerRequest", "streamingUnsignedV4ARequest",
		"streamingV4ARequest",
	} {
		if got[never] {
			t.Errorf("%s is the demux doing its own job, not a provider guess, but it was counted", never)
		}
	}
}

// TestDemuxGuessIsCountedForEveryKnownProvider adds a branch for a provider
// that has none today and checks it is seen.
//
// gcp is the provider used because it is real -- a directory under
// internal/generated -- and has no guess in the baseline, so a new one is
// exactly the regression this metric exists to catch. Writing the file into the
// real package directory and removing it again is deliberate: the measurement
// reads the tree, so a fixture somewhere else would test a different thing.
func TestDemuxGuessIsCountedForEveryKnownProvider(t *testing.T) {
	root, err := ModRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	before, _, err := MeasureDemuxGuesses(root)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "internal", "edge", "zz_demux_fixture.go")
	const body = `package edge

import "net/http"

// gcpRequest is a fixture written by TestDemuxGuessIsCountedForEveryKnownProvider.
func gcpRequest(r *http.Request) bool { return r != nil }

// notAProviderRequest must not be counted: its stem names no provider.
func notAProviderRequest(r *http.Request) bool { return r != nil }
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })

	after, names, err := MeasureDemuxGuesses(root)
	if err != nil {
		t.Fatal(err)
	}
	if after != before+1 {
		t.Fatalf("adding one provider guess moved the count %d -> %d, want %d; names %v",
			before, after, before+1, names)
	}
	var sawGCP, sawOther bool
	for _, name := range names {
		switch name {
		case "gcpRequest":
			sawGCP = true
		case "notAProviderRequest":
			sawOther = true
		}
	}
	if !sawGCP {
		t.Errorf("gcpRequest was not counted; names %v", names)
	}
	if sawOther {
		t.Errorf("notAProviderRequest was counted, but its stem names no provider")
	}
}

// TestDemuxGuessBaselineIsDeclared keeps ratchet.json's list honest against the
// tree, so the baseline cannot drift away from what is actually there.
func TestDemuxGuessBaselineIsDeclared(t *testing.T) {
	root, err := ModRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := LoadBaseline(root)
	if err != nil {
		t.Fatal(err)
	}
	_, names, err := MeasureDemuxGuesses(root)
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	for _, name := range baseline.DemuxGuessNames {
		declared[name] = true
	}
	for _, name := range names {
		if !declared[name] {
			t.Errorf("%s is a provider guess in the tree but not in ratchet.json's demux_guess_names", name)
		}
	}
}
