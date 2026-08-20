package edge

import (
	"bytes"
	"testing"
)

func FuzzDeframeAWSChunked(f *testing.F) {
	f.Add([]byte("5;chunk-signature=abc\r\nhello\r\n0;chunk-signature=abc\r\n\r\n"))
	f.Add([]byte("0\r\n\r\n"))
	f.Add([]byte(""))
	f.Add([]byte("zzzz\r\n"))
	f.Fuzz(func(t *testing.T, in []byte) {
		_, _ = deframeAWSChunked(bytes.NewReader(in))
	})
}
