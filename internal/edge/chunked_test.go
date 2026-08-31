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
		{
			name: "terminal header only",
			in:   "b\r\nhello world\r\n0\r\n",
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
	body, chunks, signatures, trailers, err := parseAWSChunked(bytes.NewBufferString("5;chunk-signature=aaa\r\nhello\r\n0;chunk-signature=bbb\r\nx-amz-checksum-crc32c:sOO8/Q==\r\nx-amz-trailer-signature:signed\r\n\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" || len(chunks) != 2 || string(chunks[0]) != "hello" || len(chunks[1]) != 0 || len(signatures) != 2 || signatures[0] != "aaa" || signatures[1] != "bbb" || trailers.Get("X-Amz-Checksum-Crc32c") != "sOO8/Q==" || trailers.Get("X-Amz-Trailer-Signature") != "signed" {
		t.Fatalf("body=%q chunks=%q signatures=%q trailers=%q", body, chunks, signatures, trailers)
	}
	if _, _, _, _, err := parseAWSChunked(bytes.NewBufferString("5;chunk-signature=aaa\r\nhello!!0;chunk-signature=bbb\r\n\r\n")); err == nil {
		t.Fatal("missing chunk terminator accepted")
	}
}
