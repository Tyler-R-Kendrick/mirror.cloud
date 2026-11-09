package bir

import (
	"testing"
	"testing/fstest"
)

// ShadowOf reads the field from the header of whichever file of a split bundle
// carries it, and says "" for a bundle that serves.
func TestShadowOfReadsAnyFileOfTheBundle(t *testing.T) {
	fsys := fstest.MapFS{
		"p/serves/service.yaml": {Data: []byte("schema: bir/1\nservice: p.serves\n")},
		"p/inline/service.yaml": {Data: []byte("schema: bir/1\nservice: p.inline\nshadow: gap\n")},
		"p/split/service.yaml":  {Data: []byte("schema: bir/1\nservice: p.split\n")},
		"p/split/ops/a.yaml":    {Data: []byte("shadow: in an ops file\noperations: {}\n")},
		"p/nested/service.yaml": {Data: []byte("schema: bir/1\nservice: p.nested\nresources:\n  shadow:\n    collection: x\n")},
	}
	for dir, want := range map[string]string{"p/serves": "", "p/inline": "gap", "p/split": "in an ops file", "p/nested": ""} {
		if got, err := ShadowOf(fsys, dir); err != nil || got != want {
			t.Errorf("ShadowOf(%s) = %q, %v; want %q", dir, got, err, want)
		}
	}
}
