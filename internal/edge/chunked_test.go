package edge

import (
	"bytes"
	"testing"
)

func TestDeframeAWSChunkedStoresPayloadNotFraming(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "signed chunks",
			in:   "5;chunk-signature=aaa\r\nhello\r\n6;chunk-signature=bbb\r\n world\r\n0;chunk-signature=ccc\r\n\r\n",
			want: "hello world",
		},
		{
			name: "unsigned chunks",
			in:   "b\r\nhello world\r\n0\r\n\r\n",
			want: "hello world",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := deframeAWSChunked(bytes.NewReader([]byte(tc.in)))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
			if bytes.Contains(got, []byte("chunk-signature")) || bytes.Contains(got, []byte("\r\n")) {
				t.Fatalf("framing leaked: %q", got)
			}
		})
	}
}

func TestParseAWSChunkedSignatures(t *testing.T) {
	body, chunks, signatures, err := parseAWSChunked(bytes.NewBufferString("5;chunk-signature=aaa\r\nhello\r\n0;chunk-signature=bbb\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" || len(chunks) != 2 || string(chunks[0]) != "hello" || len(chunks[1]) != 0 || len(signatures) != 2 || signatures[0] != "aaa" || signatures[1] != "bbb" {
		t.Fatalf("body=%q chunks=%q signatures=%q", body, chunks, signatures)
	}
	if _, _, _, err := parseAWSChunked(bytes.NewBufferString("5;chunk-signature=aaa\r\nhello!!")); err == nil {
		t.Fatal("missing chunk terminator accepted")
	}
}
