package engine

import "strings"

// secondLevel are the labels that sit directly under a two-letter country
// code and are registered under it, so `example.co.uk` is a registrable name
// and `co.uk` is not. This is not the public suffix list -- that is a 200 KB
// file that changes weekly, and an emulator has no business pinning it -- but
// it is right for the suffixes a project is actually given, and wrong in the
// way a short list is wrong: a name under a suffix it does not know is taken to
// be registered one label higher up.
var secondLevel = map[string]bool{
	"co": true, "com": true, "org": true, "net": true, "gov": true, "edu": true,
	"ac": true, "ne": true, "or": true, "go": true, "nom": true, "ltd": true, "plc": true,
}

// apexOf is the registrable part of a hostname: example.com for
// www.example.com, example.co.uk for www.example.co.uk. A name with fewer
// labels than that is its own apex.
func apexOf(host string) string {
	labels := strings.Split(strings.TrimSuffix(host, "."), ".")
	n := len(labels)
	if n <= 2 {
		return strings.Join(labels, ".")
	}
	take := 2
	if len(labels[n-1]) == 2 && secondLevel[labels[n-2]] {
		take = 3
	}
	return strings.Join(labels[n-take:], ".")
}
