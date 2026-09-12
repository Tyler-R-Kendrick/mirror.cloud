package engine

import (
	"sort"
	"strings"
)

// hierList folds blob names into a flat-or-hierarchical listing: names under
// prefix that still contain the delimiter collapse to one entry per leading
// segment (returned with the delimiter attached, e.g. "dir/"); the rest pass
// through. Entries come back sorted, like Azurite's EnumerationResults.
func hierList(names []string, prefix, delim string) []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range names {
		if !strings.HasPrefix(n, prefix) {
			continue
		}
		entry := n
		if delim != "" {
			rest := n[len(prefix):]
			if i := strings.Index(rest, delim); i >= 0 {
				entry = prefix + rest[:i+len(delim)]
			}
		}
		if !seen[entry] {
			seen[entry] = true
			out = append(out, entry)
		}
	}
	sort.Strings(out)
	return out
}
