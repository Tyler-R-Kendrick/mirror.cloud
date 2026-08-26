package eventhttp

import (
	"bytes"
	"testing"
)

func TestEncodeFormArrayFormats(t *testing.T) {
	body := []byte(`{"array":["a","b"],"metadata":{"order":"monthly"}}`)
	for format, want := range map[string]string{
		"INDICES":  "array%5B0%5D=a&array%5B1%5D=b&metadata%5Border%5D=monthly",
		"REPEAT":   "array=a&array=b&metadata%5Border%5D=monthly",
		"COMMAS":   "array=a%2Cb&metadata%5Border%5D=monthly",
		"BRACKETS": "array%5B%5D=a&array%5B%5D=b&metadata%5Border%5D=monthly",
	} {
		encoded, err := EncodeForm(body, format)
		if err != nil || string(encoded) != want {
			t.Fatalf("%s = %q, %v; want %q", format, encoded, err, want)
		}
	}
	if _, err := EncodeForm([]byte(`[]`), "INDICES"); err == nil {
		t.Fatal("encoded non-object form body")
	}
}

func TestMergeBodyLimitWithoutConnectionParameters(t *testing.T) {
	if _, err := MergeBody(bytes.Repeat([]byte{'x'}, 5), nil, 4); err == nil {
		t.Fatal("accepted oversized unmerged HTTP body")
	}
	if body, err := MergeBody(bytes.Repeat([]byte{'x'}, 4), nil, 4); err != nil || len(body) != 4 {
		t.Fatalf("rejected body at limit: %d, %v", len(body), err)
	}
}
