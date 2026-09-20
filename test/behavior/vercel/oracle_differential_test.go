package vercel

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestVercelOracleDifferential replays the canonical corpus (oracle/corpus.json)
// against mirror's booted edge and compares with the answers recorded from the
// vendor's own emulator (oracle/answers.json, captured by
// scripts/capture-oracle.py --service vercel against the pin in
// specs/vercel/emulate-inventory.json). The oracle's behavior gates mirror's
// without CI needing node: re-capture when the pin moves.
//
// Both sides are normalized with the same rewrites the capture script applies
// (the two implementations must stay in lockstep): ephemeral ids and tokens,
// timestamps, and absolute URLs are replaced before comparison.
func TestVercelOracleDifferential(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"vercel.api", "vercel.kv", "vercel.blob"}
	cfg.Seed = "vercel-oracle"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	corpus := readOracleJSON[oracleCorpus](t, "corpus.json")
	recorded := readOracleJSON[oracleAnswers](t, "answers.json")
	want := map[string]oracleAnswer{}
	for _, a := range recorded.Answers {
		want[a.Name] = a
	}

	const (
		apiToken  = "test_token_admin"
		blobToken = "vercel_blob_rw_corpusstore_zz"
	)
	got := map[string]any{} // step name -> decoded body, for $ref substitution
	for i, step := range corpus.Steps {
		name := step.Name
		if name == "" {
			name = fmt.Sprint(i)
		}
		t.Run(name, func(t *testing.T) {
			path := substituteRefs(t, step.Path, got).(string)
			body := substituteRefs(t, step.Body, got)

			var rdr io.Reader
			switch {
			case body != nil:
				raw, _ := json.Marshal(body)
				rdr = strings.NewReader(string(raw))
			case step.BodyText != "":
				rdr = strings.NewReader(step.BodyText)
			}
			req, err := http.NewRequest(step.Method, ts.URL+path, rdr)
			if err != nil {
				t.Fatal(err)
			}
			if step.Host != "" {
				req.Host = step.Host
			} else {
				req.Host = "api.vercel.com"
			}
			tok := apiToken
			if step.Token == "blob" {
				tok = blobToken
			}
			req.Header.Set("Authorization", "Bearer "+tok)
			if body != nil {
				req.Header.Set("Content-Type", "application/json")
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(res.Body)
			res.Body.Close()

			w, ok := want[name]
			if !ok {
				t.Fatalf("no recorded oracle answer for %s", name)
			}
			if res.StatusCode != w.Status {
				t.Fatalf("status %d, oracle answered %d: %s", res.StatusCode, w.Status, raw)
			}
			var decoded any
			if err := json.Unmarshal(raw, &decoded); err == nil {
				got[name] = decoded
			} else {
				decoded = string(raw)
				got[name] = decoded
			}
			switch step.Compare {
			case "status":
				// status compared above
			case "exact":
				if normalizeJSON(mustJSON(decoded)) != w.Normalized {
					t.Errorf("body differs from the oracle\nmirror: %s\noracle: %s", normalizeJSON(mustJSON(decoded)), w.Normalized)
				}
			case "fields":
				var wantBody any
				if err := json.Unmarshal([]byte(w.Raw), &wantBody); err != nil {
					t.Fatalf("recorded body for %s does not parse", name)
				}
				for _, f := range step.Fields {
					if strings.HasPrefix(f, "?") {
						_, gok2 := dig(decoded, f[1:])
						_, wok2 := dig(wantBody, f[1:])
						if gok2 != wok2 {
							t.Errorf("%s: field %s present=%v, oracle present=%v", name, f[1:], gok2, wok2)
						}
						continue
					}
					gv, gok := dig(decoded, f)
					wv, wok := dig(wantBody, f)
					if gok != wok {
						t.Errorf("%s: field %s present=%v, oracle present=%v", name, f, gok, wok)
						continue
					}
					if gok && normalizeJSON(mustJSON(gv)) != normalizeJSON(mustJSON(wv)) {
						t.Errorf("%s: field %s = %s, oracle answered %s", name, f, normalizeJSON(mustJSON(gv)), normalizeJSON(mustJSON(wv)))
					}
				}
			}
		})
	}
}

type oracleCorpus struct {
	Steps []struct {
		Name     string   `json:"name"`
		Service  string   `json:"service"`
		Method   string   `json:"method"`
		Path     string   `json:"path"`
		Host     string   `json:"host"`
		Token    string   `json:"token"`
		Body     any      `json:"body"`
		BodyText string   `json:"bodyText"`
		Compare  string   `json:"compare"`
		Fields   []string `json:"fields"`
	} `json:"steps"`
}

type oracleAnswers struct {
	Answers []oracleAnswer `json:"answers"`
}

type oracleAnswer struct {
	Name       string `json:"name"`
	Status     int    `json:"status"`
	Normalized string `json:"normalized"`
	Raw        string `json:"raw"`
}

func readOracleJSON[T any](t *testing.T, name string) T {
	t.Helper()
	raw, err := os.ReadFile("oracle/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// The rewrites of scripts/capture-oracle.py --service vercel, kept in the same order.
var oracleNormalizers = []struct {
	rx  *regexp.Regexp
	rep string
}{
	{regexp.MustCompile(`https?://[^\s"]+?/`), "http://HOST/"},
	{regexp.MustCompile(`\b(?:prj|dpl|team|ak|vc|dep)_[A-Za-z0-9]{8,}`), "ID"},
	{regexp.MustCompile(`vercel_api_[A-Za-z0-9_-]{16,}`), "TOKEN"},
	{regexp.MustCompile(`vercel_[A-Za-z0-9_-]{16,}`), "TOKEN"},
	{regexp.MustCompile(`\b[0-9a-f]{20,}\b`), "HEX"},
	{regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z"`), `"TS"`},
	{regexp.MustCompile(`\b1[0-9]{12}\b`), "EPOCHMS"},
}

func normalizeJSON(raw []byte) string {
	s := string(raw)
	for _, n := range oracleNormalizers {
		s = n.rx.ReplaceAllString(s, n.rep)
	}
	return s
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// dig reads a dotted path (with numeric list indices) out of decoded JSON.
func dig(v any, path string) (any, bool) {
	cur := v
	for _, part := range strings.Split(path, ".") {
		switch c := cur.(type) {
		case map[string]any:
			var ok bool
			cur, ok = c[part]
			if !ok {
				return nil, false
			}
		case []any:
			var i int
			if _, err := fmt.Sscanf(part, "%d", &i); err != nil || i >= len(c) {
				return nil, false
			}
			cur = c[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

var refRx = regexp.MustCompile(`\$ref:([a-z0-9-]+)\.([a-zA-Z0-9_.]+)`)

// substituteRefs replaces $ref:step.path in strings with the value mirror
// answered at that step, and resolves {"$ref": ...} objects in bodies.
func substituteRefs(t *testing.T, node any, got map[string]any) any {
	t.Helper()
	switch v := node.(type) {
	case nil:
		return nil
	case string:
		return refRx.ReplaceAllStringFunc(v, func(m string) string {
			parts := refRx.FindStringSubmatch(m)
			val, ok := dig(got[parts[1]], parts[2])
			if !ok {
				t.Fatalf("$ref %s: no value recorded from step %s", m, parts[1])
			}
			return fmt.Sprint(val)
		})
	case map[string]any:
		if len(v) == 1 {
			if ref, ok := v["$ref"].(string); ok {
				parts := refRx.FindStringSubmatch("$ref:" + ref)
				val, ok := dig(got[parts[1]], parts[2])
				if !ok {
					t.Fatalf("$ref %s: no value recorded", ref)
				}
				return val
			}
		}
		out := map[string]any{}
		for k, x := range v {
			out[k] = substituteRefs(t, x, got)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = substituteRefs(t, x, got)
		}
		return out
	default:
		return v
	}
}
