package engine

import "testing"

// TestLowerFoldsASCIIOnly pins the helper a bundle uses to make a name a key.
//
// ASCII only is the point, not a shortcut: Unicode case folding is
// locale-dependent -- Turkish dotless i folds `I` to `ı`, not `i` -- and an
// identifier that folds differently by locale is a store key that does not
// round-trip. A domain name is exactly such an identifier.
func TestLowerFoldsASCIIOnly(t *testing.T) {
	for in, want := range map[string]string{
		"Example.COM": "example.com",
		"example.com": "example.com",
		"":            "",
		"ALLCAPS":     "allcaps",
		"MiXeD-123":   "mixed-123",
		// Non-ASCII is left alone while its ASCII neighbours fold. Turkish
		// dotless i is the standard example of why: a Unicode fold would turn
		// `I` into `ı` under a Turkish locale and into `i` everywhere else, so
		// the same name would produce two different store keys.
		"İSTANBUL": "İstanbul",
		"ÄÖÜ":      "ÄÖÜ",
	} {
		if got := asciiLower(in); got != want {
			t.Errorf("asciiLower(%q) = %q, want %q", in, got, want)
		}
	}
	// Byte length is preserved, which is what keeps a folded key the same key.
	for _, s := range []string{"Example.COM", "ÄÖÜ", "MiXeD-123"} {
		if len(asciiLower(s)) != len(s) {
			t.Errorf("asciiLower(%q) changed length", s)
		}
	}
}
