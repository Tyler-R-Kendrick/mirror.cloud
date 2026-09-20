package stripe

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

// TestStripeOracleDifferential replays the canonical corpus (oracle/corpus.json)
// against mirror's booted edge and compares with the answers recorded from the
// vendor's own emulator (oracle/answers.json, captured by
// scripts/capture-oracle.py --service stripe against the pin in
// specs/stripe/emulate-inventory.json). The oracle's behavior gates mirror's
// without CI needing node: re-capture when the pin moves.
//
// Both sides are normalized with the same rewrites the capture script applies
// (the two implementations must stay in lockstep): ephemeral ids, timestamps,
// and absolute URLs are replaced before comparison. The oracle seeds one
// customer, so every customer-linked list step filters to the corpus rows;
// products and prices seed nothing, so those lists compare unfiltered.
func TestStripeOracleDifferential(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"stripe.api"}
	cfg.Seed = "stripe-oracle"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	corpus := readStripeJSON[stripeCorpus](t, "corpus.json")
	recorded := readStripeJSON[stripeAnswers](t, "answers.json")
	want := map[string]stripeAnswer{}
	for _, a := range recorded.Answers {
		want[a.Name] = a
	}

	follow := http.DefaultClient
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	got := map[string]any{} // step name -> decoded body, for $ref substitution
	for i, step := range corpus.Steps {
		name := step.Name
		if name == "" {
			name = fmt.Sprint(i)
		}
		t.Run(name, func(t *testing.T) {
			path := stripeSubstituteRefs(t, step.Path, got).(string)
			body := stripeSubstituteRefs(t, step.Body, got)

			var rdr io.Reader
			contentType := ""
			switch {
			case body != nil:
				raw, _ := json.Marshal(body)
				rdr = strings.NewReader(string(raw))
				contentType = "application/json"
			case step.BodyText != "":
				rdr = strings.NewReader(step.BodyText)
				contentType = step.ContentType
				if contentType == "" {
					contentType = "text/plain"
				}
			}
			req, err := http.NewRequest(step.Method, ts.URL+path, rdr)
			if err != nil {
				t.Fatal(err)
			}
			if step.Host != "" {
				req.Host = step.Host
			} else {
				req.Host = "api.stripe.com"
			}
			// The oracle checks no authentication on any route, so neither
			// does the replay: requests carry none on either side.
			if contentType != "" {
				req.Header.Set("Content-Type", contentType)
			}
			client := follow
			if step.NoFollow {
				client = noFollow
			}
			res, err := client.Do(req)
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
			if step.Location != "" {
				wantLoc := stripeNormalize([]byte(stripeSubstituteRefs(t, step.Location, got).(string)))
				gotLoc := stripeNormalize([]byte(res.Header.Get("Location")))
				if wantLoc != gotLoc {
					t.Errorf("location %s, oracle answered %s", gotLoc, wantLoc)
				}
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
				// status compared above; the location above where asserted
			case "exact":
				if stripeNormalize(mustStripeJSON(decoded)) != w.Normalized {
					t.Errorf("body differs from the oracle\nmirror: %s\noracle: %s", stripeNormalize(mustStripeJSON(decoded)), w.Normalized)
				}
			case "fields":
				var wantBody any
				if err := json.Unmarshal([]byte(w.Raw), &wantBody); err != nil {
					t.Fatalf("recorded body for %s does not parse", name)
				}
				for _, f := range step.Fields {
					gv, gok := stripeDig(decoded, f)
					wv, wok := stripeDig(wantBody, f)
					if gok != wok {
						t.Errorf("%s: field %s present=%v, oracle present=%v", name, f, gok, wok)
						continue
					}
					if gok && stripeNormalize(mustStripeJSON(gv)) != stripeNormalize(mustStripeJSON(wv)) {
						t.Errorf("%s: field %s = %s, oracle answered %s", name, f, stripeNormalize(mustStripeJSON(gv)), stripeNormalize(mustStripeJSON(wv)))
					}
				}
			}
		})
	}
}

type stripeCorpus struct {
	Steps []struct {
		Name        string   `json:"name"`
		Service     string   `json:"service"`
		Method      string   `json:"method"`
		Path        string   `json:"path"`
		Host        string   `json:"host"`
		Body        any      `json:"body"`
		BodyText    string   `json:"bodyText"`
		ContentType string   `json:"contentType"`
		NoFollow    bool     `json:"noFollow"`
		Location    string   `json:"location"`
		Compare     string   `json:"compare"`
		Fields      []string `json:"fields"`
	} `json:"steps"`
}

type stripeAnswers struct {
	Answers []stripeAnswer `json:"answers"`
}

type stripeAnswer struct {
	Name       string `json:"name"`
	Status     int    `json:"status"`
	Normalized string `json:"normalized"`
	Raw        string `json:"raw"`
	Location   string `json:"location"`
}

func readStripeJSON[T any](t *testing.T, name string) T {
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

// The rewrites of scripts/capture-oracle.py --service stripe, kept in the
// same order.
var stripeNormalizers = []struct {
	rx  *regexp.Regexp
	rep string
}{
	{regexp.MustCompile(`https?://[^\s"]+?/`), "http://HOST/"},
	{regexp.MustCompile(`\b(?:cus|prod|price|pi|ch|cs|cuss_secret)_[A-Za-z0-9_-]{8,}`), "ID"},
	{regexp.MustCompile(`\b[0-9a-f]{20,}\b`), "HEX"},
	{regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z"`), `"TS"`},
	{regexp.MustCompile(`\b1[0-9]{9}\b`), "EPOCH"},
}

func stripeNormalize(raw []byte) string {
	s := string(raw)
	for _, n := range stripeNormalizers {
		s = n.rx.ReplaceAllString(s, n.rep)
	}
	return s
}

func mustStripeJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// stripeDig reads a dotted path (with numeric list indices) out of decoded JSON.
func stripeDig(v any, path string) (any, bool) {
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

var stripeRefRx = regexp.MustCompile(`\$ref:([a-z0-9-]+)\.([a-zA-Z0-9_.]+)`)

// stripeSubstituteRefs replaces $ref:step.path in strings with the value
// mirror answered at that step, and resolves {"$ref": ...} objects in bodies.
func stripeSubstituteRefs(t *testing.T, node any, got map[string]any) any {
	t.Helper()
	switch v := node.(type) {
	case nil:
		return nil
	case string:
		return stripeRefRx.ReplaceAllStringFunc(v, func(m string) string {
			parts := stripeRefRx.FindStringSubmatch(m)
			val, ok := stripeDig(got[parts[1]], parts[2])
			if !ok {
				t.Fatalf("$ref %s: no value recorded from step %s", m, parts[1])
			}
			return fmt.Sprint(val)
		})
	case map[string]any:
		if len(v) == 1 {
			if ref, ok := v["$ref"].(string); ok {
				parts := stripeRefRx.FindStringSubmatch("$ref:" + ref)
				val, ok := stripeDig(got[parts[1]], parts[2])
				if !ok {
					t.Fatalf("$ref %s: no value recorded", ref)
				}
				return val
			}
		}
		out := map[string]any{}
		for k, x := range v {
			out[k] = stripeSubstituteRefs(t, x, got)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = stripeSubstituteRefs(t, x, got)
		}
		return out
	default:
		return v
	}
}
