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
	body, _, _, err := parseAWSChunked(r)
	return body, err
}

func parseAWSChunked(r io.Reader) ([]byte, [][]byte, []string, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, nil, err
	}
	var out bytes.Buffer
	var chunks [][]byte
	var signatures []string
	rest := raw
	for len(rest) > 0 {
		nl := bytes.Index(rest, []byte("\r\n"))
		if nl < 0 {
			return nil, nil, nil, fmt.Errorf("aws-chunked: missing chunk header")
		}
		header := string(rest[:nl])
		rest = rest[nl+2:]
		sizePart := header
		signature := ""
		if i := strings.IndexByte(header, ';'); i >= 0 {
			sizePart = header[:i]
			for _, extension := range strings.Split(header[i+1:], ";") {
				name, value, ok := strings.Cut(extension, "=")
				if ok && strings.EqualFold(strings.TrimSpace(name), "chunk-signature") {
					if signature != "" {
						return nil, nil, nil, fmt.Errorf("aws-chunked: duplicate chunk signature")
					}
					signature = strings.TrimSpace(value)
				}
			}
		}
		sizePart = strings.TrimSpace(sizePart)
		if sizePart == "" {
			continue
		}
		n, err := strconv.ParseInt(sizePart, 16, 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("aws-chunked: bad size %q: %w", sizePart, err)
		}
		if n < 0 {
			return nil, nil, nil, fmt.Errorf("aws-chunked: bad size %q", sizePart)
		}
		if n == 0 {
			chunks = append(chunks, nil)
			signatures = append(signatures, signature)
			break
		}
		if len(rest) < 2 || n > int64(len(rest)-2) {
			return nil, nil, nil, fmt.Errorf("aws-chunked: truncated chunk")
		}
		chunk := bytes.Clone(rest[:n])
		out.Write(chunk)
		chunks = append(chunks, chunk)
		signatures = append(signatures, signature)
		rest = rest[n:]
		if !bytes.HasPrefix(rest, []byte("\r\n")) {
			return nil, nil, nil, fmt.Errorf("aws-chunked: missing chunk terminator")
		}
		rest = rest[2:]
	}
	return out.Bytes(), chunks, signatures, nil
}
