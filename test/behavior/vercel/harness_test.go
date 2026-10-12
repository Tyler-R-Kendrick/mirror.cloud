package vercel

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// vx is a booted emulator with a controllable clock, addressed the way a real
// client addresses api.vercel.com: bearer token, JSON bodies, the production
// host. Tests built on it speak HTTP and nothing else, so what they assert is
// what a client sees.
type vx struct {
	t  *testing.T
	rt *rtpkg.Runtime
	ts *httptest.Server
}

// newVX boots the named services (vercel.api when none) on a controllable
// clock. The clock starts at the epoch the other Vercel tests use, so
// timestamps are the same across suites.
func newVX(t *testing.T, seed string, services ...string) *vx {
	t.Helper()
	return newVXWith(t, seed, services)
}

// bootOptions are the ways a test changes the configuration it boots with.
type bootOptions struct{ cfg []func(*config.Config) }

// withConfig edits the configuration before the runtime boots.
func withConfig(f func(*config.Config)) func(*bootOptions) {
	return func(o *bootOptions) { o.cfg = append(o.cfg, f) }
}

// newVXWith is newVX with configuration edits.
func newVXWith(t *testing.T, seed string, services []string, opts ...func(*bootOptions)) *vx {
	t.Helper()
	t.Setenv("MIRROR_CLOCK", "controllable")
	cfg := config.Default()
	if len(services) == 0 {
		services = []string{"vercel.api"}
	}
	cfg.Services = services
	cfg.Seed = seed
	var o bootOptions
	for _, opt := range opts {
		opt(&o)
	}
	for _, f := range o.cfg {
		f(&cfg)
	}
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	t.Cleanup(ts.Close)
	return &vx{t: t, rt: rt, ts: ts}
}

// reply is one answer: the status, the decoded body when it is a JSON object,
// the raw bytes, and the headers.
type reply struct {
	status int
	body   map[string]any
	raw    []byte
	header http.Header
}

// do sends one request. body may be nil, a string, []byte or anything JSON
// encodes; headers come in key, value pairs and replace the defaults.
func (v *vx) do(method, path string, body any, headers ...string) reply {
	v.t.Helper()
	var rdr io.Reader
	contentType := ""
	switch b := body.(type) {
	case nil:
	case string:
		rdr, contentType = bytes.NewReader([]byte(b)), "application/json"
	case []byte:
		rdr, contentType = bytes.NewReader(b), "application/octet-stream"
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			v.t.Fatal(err)
		}
		rdr, contentType = bytes.NewReader(raw), "application/json"
	}
	req, err := http.NewRequest(method, v.ts.URL+path, rdr)
	if err != nil {
		v.t.Fatal(err)
	}
	req.Host = "api.vercel.com"
	req.Header.Set("Authorization", "Bearer test_token")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		if headers[i+1] == "" {
			req.Header.Del(headers[i])
			continue
		}
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		v.t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	out := reply{status: res.StatusCode, raw: raw, header: res.Header}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

// advance moves the emulator's clock, which is what settles a deployment's
// build timers.
func (v *vx) advance(d time.Duration) {
	v.t.Helper()
	if err := v.rt.Deps.Clock.Advance(d); err != nil {
		v.t.Fatal(err)
	}
}

// str reads a string member, "" when it is absent or not a string.
func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// unmarshalJSON decodes a body that may be an array.
func unmarshalJSON(raw []byte, into any) error { return json.Unmarshal(raw, into) }
