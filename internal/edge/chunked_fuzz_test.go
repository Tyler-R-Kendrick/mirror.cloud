package edge

import (
	"bytes"
	"testing"
)

func FuzzDeframeAWSChunked(f *testing.F) {
	f.Add([]byte("5;chunk-signature=abc\r\nhello\r\n0;chunk-signature=abc\r\n\r\n"))
	f.Add([]byte("a;chunk-signature=first\r\nHello Blob\r\n0;chunk-signature=last\r\n"))
	f.Add([]byte("\r\nHello Blob\r\n0;chunk-signature=invalid\r\n"))
	f.Add([]byte("0\r\n\r\n"))
	f.Add([]byte(""))
	f.Add([]byte("zzzz\r\n"))
	f.Add([]byte("-100\r\n"))
	f.Add([]byte("5\r\nhello\r\n"))
	f.Add([]byte("5\r\nhello"))
	f.Fuzz(func(t *testing.T, in []byte) {
		_, _ = deframeAWSChunked(bytes.NewReader(in))
	})
}
