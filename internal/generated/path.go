// Package generated: this file is hand-written, not mirrorgen output.
package generated

import "strings"

// ServicePath maps a service ID onto the directory its generated model lives
// in: the provider prefix, then the service name stripped to lowercase
// alphanumerics. This is the one place the layout rule lives. The registry
// reads by it and mirrorgen writes by it, so the two cannot drift -- the
// disagreement that once answered "no model for hostinger.api" with the file
// committed beside the others.
func ServicePath(serviceID string) (provider, pkg string) {
	provider, rest, ok := strings.Cut(serviceID, ".")
	if !ok {
		provider, rest = "unknown", serviceID
	}
	var b strings.Builder
	for _, r := range strings.ToLower(rest) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	if pkg = b.String(); pkg == "" {
		pkg = "service"
	}
	return provider, pkg
}
