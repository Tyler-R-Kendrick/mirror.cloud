package bir

import (
	"reflect"
	"testing"
)

func TestTagExpr(t *testing.T) {
	tags := map[string]any{"tag1": "val1", "tag2": "val2", "key1": "1a", " key 1 +-.:=_/": "x"}
	for _, test := range []struct {
		expr string
		want bool
	}{
		{"tag1='val1'", true},
		{"tag1='val11'", false},
		{"tag1 <> 'v0'", true},
		{"tag1 <> 'val1'", false},
		{"missing='v'", false},
		{"missing<>'v'", false}, // SQL NULL semantics
		{"key1>'1 a'", true},
		{"key1>'1a'", false},
		{"key1>='1a'", true},
		{"key1<'1b'", true},
		{"tag1='val1' and tag2='val2'", true},
		{"tag1='val1' AND tag2='nope'", false},
		{"tag1='nope' or tag2='val2'", true},
		{"not tag1='nope'", true},
		{"(tag1='nope' or tag2='val2') and tag1='val1'", true},
		{"@container='ctr'", true},
		{"@container='other'", false},
		{`" key 1 +-.:=_/"='x'`, true},
		{"tag1='it''s'", false},
	} {
		ev, err := ParseTagExpr(test.expr)
		if err != nil {
			t.Fatalf("%s: %v", test.expr, err)
		}
		if got := ev(tags, "ctr"); got != test.want {
			t.Errorf("%s: got %v, want %v", test.expr, got, test.want)
		}
	}
	for _, bad := range []string{"key111==value1", "key+1='value1'", "tag1=", "(tag1='v'", "tag1 'v'", "tag1='v", "'tag1'='v'", "@container11='1'", "@foo='1'"} {
		if _, err := ParseTagExpr(bad); err == nil {
			t.Errorf("%s: want parse error", bad)
		}
	}
}

func TestTagExprKeys(t *testing.T) {
	got := TagExprKeys(`tag1='a' and ("k 2"='b' or @container='c') and not tag1<>'z'`)
	if !reflect.DeepEqual(got, []string{"tag1", "k 2"}) {
		t.Fatalf("keys %#v", got)
	}
}

