package edge

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// deframeAWSChunked unwraps Content-Encoding: aws-chunked bodies.
// Format: <hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n … trailing 0 chunk.
func deframeAWSChunked(r io.Reader) ([]byte, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	rest := raw
	for len(rest) > 0 {
		nl := bytes.Index(rest, []byte("\r\n"))
		if nl < 0 {
			return nil, fmt.Errorf("aws-chunked: missing chunk header")
		}
		header := string(rest[:nl])
		rest = rest[nl+2:]
		sizePart := header
		if i := strings.IndexByte(header, ';'); i >= 0 {
			sizePart = header[:i]
		}
		sizePart = strings.TrimSpace(sizePart)
		if sizePart == "" {
			continue
		}
		n, err := strconv.ParseInt(sizePart, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("aws-chunked: bad size %q: %w", sizePart, err)
		}
		if n < 0 {
			return nil, fmt.Errorf("aws-chunked: bad size %q", sizePart)
		}
		if n == 0 {
			break
		}
		if len(rest) < 2 || n > int64(len(rest)-2) {
			return nil, fmt.Errorf("aws-chunked: truncated chunk")
		}
		out.Write(rest[:n])
		rest = rest[n:]
		if bytes.HasPrefix(rest, []byte("\r\n")) {
			rest = rest[2:]
		}
	}
	return out.Bytes(), nil
}
