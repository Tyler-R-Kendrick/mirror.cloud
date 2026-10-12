package engine

import "testing"

// TestMentionsMatchesWholeIdentifiersOnly pins what counts as reading a
// binding: the name on its own or as the head of a call or field, never a
// member of something else and never part of a longer name.
func TestMentionsMatchesWholeIdentifiersOnly(t *testing.T) {
	for _, c := range []struct {
		src, name string
		want      bool
	}{
		{"size(page)", "page", true},
		{"page.map(c, c.Name)", "page", true},
		{"rec.page", "page", false},
		{"pages", "page", false},
		{"my_page", "page", false},
		{"page2", "page", false},
		{"a + page", "page", true},
		{"'page'.size() + page.size()", "page", true},
	} {
		if got := mentions(c.src, c.name); got != c.want {
			t.Errorf("mentions(%q, %q) = %v, want %v", c.src, c.name, got, c.want)
		}
	}
}
