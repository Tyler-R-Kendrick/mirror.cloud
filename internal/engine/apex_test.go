package engine

import "testing"

// TestApexOfIsTheRegistrablePart pins what Vercel's apexName is for the names
// a project is given, including the second-level registrations under a country
// code and the names that are already apexes.
func TestApexOfIsTheRegistrablePart(t *testing.T) {
	for in, want := range map[string]string{
		"example.com":         "example.com",
		"www.example.com":     "example.com",
		"a.b.c.example.com":   "example.com",
		"example.co.uk":       "example.co.uk",
		"www.example.co.uk":   "example.co.uk",
		"shop.example.com.au": "example.com.au",
		"my-app.vercel.app":   "vercel.app",
		"localhost":           "localhost",
		"co.uk":               "co.uk",
		"example.com.":        "example.com",
		"":                    "",
	} {
		if got := apexOf(in); got != want {
			t.Errorf("apexOf(%q) = %q, want %q", in, got, want)
		}
	}
	if got := evalCEL(t, `apexOf('www.example.co.uk')`, nil); got != "example.co.uk" {
		t.Errorf("apexOf from CEL = %v", got)
	}
}
