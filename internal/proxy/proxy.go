// Package proxy implements the proxy fidelity tier: pass-through to an
// explicit cloud endpoint with record, replay, and hybrid cassette modes.
// Secrets are scrubbed at write time. The proxy is off by default.
package proxy

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Mode is the proxy operating mode.
type Mode string

const (
	ModeOff         Mode = "off" // default
	ModePassthrough Mode = "passthrough"
	ModeRecord      Mode = "record"
	ModeReplay      Mode = "replay"
	ModeHybrid      Mode = "hybrid" // replay if present, else record
)

// Cassette is a recorded request/response corpus. Secrets are scrubbed at
// write time, never at read time.
type Cassette interface {
	Lookup(key string) (*Interaction, bool)
	Append(i *Interaction) error
	Flush() error
}

// Interaction is one recorded HTTP request/response pair.
type Interaction struct {
	Key             string
	Method          string
	Path            string
	Query           string
	RequestHeaders  http.Header
	RequestBody     []byte
	Status          int
	ResponseHeaders http.Header
	ResponseBody    []byte
}

// FileCassette is a plain-text, diffable cassette stored at Path.
// One interaction per record; Flush writes records in key order.
type FileCassette struct {
	// Scrub holds extra write-time secret patterns (regular expressions).
	Scrub Scrub

	path  string
	mu    sync.Mutex
	byKey map[string]*Interaction
}

// NewFileCassette loads a cassette from path, or starts empty if missing.
func NewFileCassette(path string) (*FileCassette, error) {
	c := &FileCassette{path: path, byKey: map[string]*Interaction{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return c, nil
		}
		return nil, err
	}
	m, err := parseCassette(data)
	if err != nil {
		return nil, err
	}
	c.byKey = m
	return c, nil
}

// Lookup returns the interaction for key, if present.
func (c *FileCassette) Lookup(key string) (*Interaction, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	i, ok := c.byKey[key]
	return i, ok
}

// Append stores i after write-time secret scrubbing. Same key overwrites.
func (c *FileCassette) Append(i *Interaction) error {
	if i == nil {
		return errors.New("proxy: nil interaction")
	}
	stored := applyScrub(cloneInteraction(i), c.Scrub.Patterns)
	if stored.Key == "" {
		stored.Key = lookupKey(stored.Method, stored.Path, stored.Query, stored.RequestBody, nil)
	}
	c.mu.Lock()
	c.byKey[stored.Key] = stored
	c.mu.Unlock()
	return nil
}

// Flush writes the cassette to disk, sorted by key.
func (c *FileCassette) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.byKey))
	for k := range c.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	buf.WriteString("# mirror.cloud cassette v1\n")
	for _, k := range keys {
		writeInteraction(&buf, c.byKey[k])
	}
	if dir := filepath.Dir(c.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}

var _ Cassette = (*FileCassette)(nil)

func cloneInteraction(i *Interaction) *Interaction {
	if i == nil {
		return nil
	}
	out := *i
	out.RequestBody = bytes.Clone(i.RequestBody)
	out.ResponseBody = bytes.Clone(i.ResponseBody)
	if i.RequestHeaders != nil {
		out.RequestHeaders = i.RequestHeaders.Clone()
	}
	if i.ResponseHeaders != nil {
		out.ResponseHeaders = i.ResponseHeaders.Clone()
	}
	return &out
}

func lookupKey(method, path, rawQuery string, body []byte, patterns []string) string {
	method = strings.ToUpper(method)
	if method == "" {
		method = http.MethodGet
	}
	res := compilePatterns(patterns)
	path = redactString(path, res)
	q := scrubQuery(rawQuery, res)
	var b strings.Builder
	b.WriteString(method)
	b.WriteByte(' ')
	b.WriteString(path)
	if q != "" {
		b.WriteByte('?')
		b.WriteString(q)
	}
	if len(body) > 0 {
		sum := sha256.Sum256(body)
		b.WriteByte(' ')
		b.WriteString(hex.EncodeToString(sum[:]))
	}
	return b.String()
}

func sortQuery(raw string) string {
	if raw == "" {
		return ""
	}
	v, err := url.ParseQuery(raw)
	if err != nil {
		return raw
	}
	return v.Encode()
}

const cassettePrefix = "=== "

func writeInteraction(w *bytes.Buffer, i *Interaction) {
	w.WriteByte('\n')
	w.WriteString(cassettePrefix)
	w.WriteString(i.Key)
	w.WriteByte('\n')
	fmt.Fprintf(w, "method: %s\n", i.Method)
	fmt.Fprintf(w, "path: %s\n", i.Path)
	fmt.Fprintf(w, "query: %s\n", i.Query)
	writeHeaders(w, "req-header", i.RequestHeaders)
	fmt.Fprintf(w, "req-body: %d\n", len(i.RequestBody))
	w.Write(i.RequestBody)
	w.WriteByte('\n')
	fmt.Fprintf(w, "status: %d\n", i.Status)
	writeHeaders(w, "resp-header", i.ResponseHeaders)
	fmt.Fprintf(w, "resp-body: %d\n", len(i.ResponseBody))
	w.Write(i.ResponseBody)
	w.WriteByte('\n')
}

func writeHeaders(w *bytes.Buffer, prefix string, h http.Header) {
	if h == nil {
		return
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		if skipHeader(k) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range h[k] {
			fmt.Fprintf(w, "%s: %s: %s\n", prefix, k, v)
		}
	}
}

func skipHeader(k string) bool {
	switch http.CanonicalHeaderKey(k) {
	case "Content-Length", "Transfer-Encoding", "Connection", "Keep-Alive", "Trailer", "Upgrade", "Proxy-Connection":
		return true
	}
	return false
}

func parseCassette(data []byte) (map[string]*Interaction, error) {
	r := bufio.NewReader(bytes.NewReader(data))
	out := map[string]*Interaction{}
	var cur *Interaction
	flush := func() {
		if cur != nil && cur.Key != "" {
			out[cur.Key] = cur
		}
		cur = nil
	}
	for {
		line, err := r.ReadString('\n')
		eof := err == io.EOF
		if err != nil && !eof {
			return nil, err
		}
		trim := strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(trim, "#") || trim == "":
			// comments and blanks
		case strings.HasPrefix(trim, cassettePrefix):
			flush()
			cur = &Interaction{
				Key:             strings.TrimPrefix(trim, cassettePrefix),
				RequestHeaders:  make(http.Header),
				ResponseHeaders: make(http.Header),
			}
		case cur == nil:
			// stray line before first record
		default:
			name, val, ok := splitField(trim)
			if !ok {
				break
			}
			switch name {
			case "method":
				cur.Method = val
			case "path":
				cur.Path = val
			case "query":
				cur.Query = val
			case "status":
				cur.Status, _ = strconv.Atoi(val)
			case "req-header":
				hk, hv := splitHeader(val)
				cur.RequestHeaders.Add(hk, hv)
			case "resp-header":
				hk, hv := splitHeader(val)
				cur.ResponseHeaders.Add(hk, hv)
			case "req-body":
				n, _ := strconv.Atoi(val)
				cur.RequestBody, err = readBlock(r, n)
				if err != nil {
					return nil, err
				}
			case "resp-body":
				n, _ := strconv.Atoi(val)
				cur.ResponseBody, err = readBlock(r, n)
				if err != nil {
					return nil, err
				}
			}
		}
		if eof {
			break
		}
	}
	flush()
	return out, nil
}

func splitField(line string) (name, val string, ok bool) {
	i := strings.IndexByte(line, ':')
	if i < 0 {
		return "", "", false
	}
	return line[:i], strings.TrimPrefix(line[i+1:], " "), true
}

func splitHeader(v string) (string, string) {
	i := strings.IndexByte(v, ':')
	if i < 0 {
		return v, ""
	}
	return v[:i], strings.TrimPrefix(v[i+1:], " ")
}

func readBlock(r *bufio.Reader, n int) ([]byte, error) {
	if n < 0 {
		n = 0
	}
	buf := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
	}
	b, err := r.ReadByte()
	if err == io.EOF {
		return buf, nil
	}
	if err != nil {
		return nil, err
	}
	if b == '\r' {
		_, _ = r.ReadByte()
	} else if b != '\n' {
		if err := r.UnreadByte(); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// Transport is an http.RoundTripper that proxies according to Mode.
type Transport struct {
	// Scrub holds extra write-time secret patterns (regular expressions).
	Scrub Scrub

	mode     Mode
	cassette Cassette
	ep       *url.URL
	base     http.RoundTripper
}

var errModeOff = errors.New("proxy: mode is off")

// NewTransport returns a RoundTripper for mode. endpoint is required for
// passthrough, record, and hybrid. cassette is required for record, replay,
// and hybrid.
func NewTransport(mode Mode, cassette Cassette, endpoint string) (*Transport, error) {
	t := &Transport{
		mode:     mode,
		cassette: cassette,
		base:     newBase(),
	}
	switch mode {
	case ModeOff, "":
		return t, nil
	case ModeReplay:
		if cassette == nil {
			return nil, errors.New("proxy: cassette required for replay")
		}
		if endpoint != "" {
			u, err := url.Parse(endpoint)
			if err != nil {
				return nil, fmt.Errorf("proxy: endpoint: %w", err)
			}
			t.ep = u
		}
		return t, nil
	case ModePassthrough, ModeRecord, ModeHybrid:
		if endpoint == "" {
			return nil, errors.New("proxy: endpoint required")
		}
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, fmt.Errorf("proxy: endpoint: %w", err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, errors.New("proxy: endpoint must include scheme and host")
		}
		t.ep = u
		if mode != ModePassthrough && cassette == nil {
			return nil, fmt.Errorf("proxy: cassette required for %s", mode)
		}
		return t, nil
	default:
		return nil, fmt.Errorf("proxy: unknown mode %q", mode)
	}
}

func newBase() http.RoundTripper {
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t := dt.Clone()
		t.Proxy = nil
		return t
	}
	return http.DefaultTransport
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, errors.New("proxy: nil request")
	}
	switch t.mode {
	case ModeOff, "":
		return nil, errModeOff
	}

	body, err := peekBody(req)
	if err != nil {
		return nil, err
	}
	key := lookupKey(req.Method, req.URL.Path, req.URL.RawQuery, body, t.Scrub.Patterns)

	switch t.mode {
	case ModeReplay:
		return t.replay(req, key)
	case ModeHybrid:
		if t.cassette != nil {
			if i, ok := t.cassette.Lookup(key); ok {
				return responseFrom(req, i)
			}
		}
		return t.record(req, body, key)
	case ModeRecord:
		return t.record(req, body, key)
	case ModePassthrough:
		return t.forward(req, body)
	default:
		return nil, fmt.Errorf("proxy: unknown mode %q", t.mode)
	}
}

var _ http.RoundTripper = (*Transport)(nil)

func (t *Transport) replay(req *http.Request, key string) (*http.Response, error) {
	if t.cassette == nil {
		return nil, errors.New("proxy: cassette required for replay")
	}
	i, ok := t.cassette.Lookup(key)
	if !ok {
		return nil, fmt.Errorf("proxy: no recorded interaction for %q", key)
	}
	return responseFrom(req, i)
}

func (t *Transport) record(req *http.Request, body []byte, key string) (*http.Response, error) {
	resp, err := t.forward(req, body)
	if err != nil {
		return nil, err
	}
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	i := applyScrub(&Interaction{
		Key:             key,
		Method:          strings.ToUpper(req.Method),
		Path:            req.URL.Path,
		Query:           sortQuery(req.URL.RawQuery),
		RequestHeaders:  req.Header.Clone(),
		RequestBody:     bytes.Clone(body),
		Status:          resp.StatusCode,
		ResponseHeaders: resp.Header.Clone(),
		ResponseBody:    respBody,
	}, t.Scrub.Patterns)
	if t.cassette != nil {
		if err := t.cassette.Append(i); err != nil {
			return nil, err
		}
	}
	resp.Body = io.NopCloser(bytes.NewReader(respBody))
	resp.ContentLength = int64(len(respBody))
	return resp, nil
}

func (t *Transport) forward(req *http.Request, body []byte) (*http.Response, error) {
	if t.ep == nil {
		return nil, errors.New("proxy: endpoint required")
	}
	out := req.Clone(req.Context())
	out.URL.Scheme = t.ep.Scheme
	out.URL.Host = t.ep.Host
	if p := strings.TrimSuffix(t.ep.Path, "/"); p != "" && p != "/" {
		out.URL.Path = p + out.URL.Path
		out.URL.RawPath = ""
	}
	out.Host = t.ep.Host
	out.RequestURI = ""
	if body != nil {
		out.Body = io.NopCloser(bytes.NewReader(body))
		out.ContentLength = int64(len(body))
		out.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
	}
	return t.base.RoundTrip(out)
}

func peekBody(req *http.Request) ([]byte, error) {
	if req.Body == nil {
		return nil, nil
	}
	defer req.Body.Close()
	b, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(b))
	return b, nil
}

func responseFrom(req *http.Request, i *Interaction) (*http.Response, error) {
	hdr := http.Header{}
	if i.ResponseHeaders != nil {
		hdr = i.ResponseHeaders.Clone()
	}
	hdr.Del("Transfer-Encoding")
	hdr.Set("Content-Length", strconv.Itoa(len(i.ResponseBody)))
	status := i.Status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		Status:        strconv.Itoa(status) + " " + http.StatusText(status),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        hdr,
		Body:          io.NopCloser(bytes.NewReader(i.ResponseBody)),
		ContentLength: int64(len(i.ResponseBody)),
		Uncompressed:  true,
		Request:       req,
	}, nil
}

// Divergence is one difference between an emulated response and a recorded one.
type Divergence struct {
	Path     string
	Emulated string
	Recorded string
}

// Diff reports structured divergences between emulated and recorded bytes.
func Diff(emulated, recorded []byte) []Divergence {
	if bytes.Equal(emulated, recorded) {
		return nil
	}
	el := bytes.Split(emulated, []byte("\n"))
	rl := bytes.Split(recorded, []byte("\n"))
	n := len(el)
	if len(rl) > n {
		n = len(rl)
	}
	var out []Divergence
	for i := 0; i < n; i++ {
		var e, r string
		if i < len(el) {
			e = string(el[i])
		}
		if i < len(rl) {
			r = string(rl[i])
		}
		if e != r {
			out = append(out, Divergence{
				Path:     fmt.Sprintf("line[%d]", i+1),
				Emulated: e,
				Recorded: r,
			})
		}
	}
	return out
}
