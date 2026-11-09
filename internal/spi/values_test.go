package spi

import (
	"reflect"
	"testing"
)

// Members reads name.member.1, name.member.2 and on to the first index
// missing, as the query decoder does: a gap ends the list. Before, it
// counted the members and read that many indices, so member.1 and member.3
// answered [a] and member.2 and member.3 answered [b], neither the decoder's.
func TestMembersEndsAtAGapAsTheDecoderDoes(t *testing.T) {
	for _, c := range []struct {
		in   map[string]any
		want []any
	}{
		{map[string]any{"L.member.1": "a", "L.member.2": "b", "L.member.3": "c"}, []any{"a", "b", "c"}},
		{map[string]any{"L.member.1": "a", "L.member.3": "c"}, []any{"a"}},
		{map[string]any{"L.member.2": "b", "L.member.3": "c"}, []any{}},
		{map[string]any{"L.member.x": "a", "Other.member.1": "o"}, []any{}},
	} {
		if got := Members(c.in, "L"); !reflect.DeepEqual(got, c.want) {
			t.Errorf("Members(%v) = %#v, want %#v", c.in, got, c.want)
		}
	}
}
