package engine

import (
	"reflect"
	"testing"
)

func TestHierList(t *testing.T) {
	got := hierList([]string{"dir/b", "dir/a", "top", "dir/sub/c", "other/x"}, "", "/")
	if !reflect.DeepEqual(got, []string{"dir/", "other/", "top"}) {
		t.Fatalf("hier %#v", got)
	}
	got = hierList([]string{"dir/b", "dir/a", "top"}, "dir/", "/")
	if !reflect.DeepEqual(got, []string{"dir/a", "dir/b"}) {
		t.Fatalf("hier scoped %#v", got)
	}
	got = hierList([]string{"b", "a"}, "", "")
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("flat %#v", got)
	}
}
