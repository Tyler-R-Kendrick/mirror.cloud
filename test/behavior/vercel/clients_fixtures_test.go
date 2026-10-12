package vercel

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// What Vercel's own clients sent, kept as they sent it.
//
// Each file under testdata/clients/<client>/ is one journey recorded by pointing
// the real client (the CLI, or the SDK) at a running emulator through a proxy
// that wrote down every request. A step is a request exactly as the client put
// it on the wire (method, address, the headers a server could key on, body), the
// answer the client then needed, and the members it read out of the answer to
// build the next request. The credential is a dummy; the host is the emulator's
// own, as a client pointed at http://127.0.0.1:PORT sends it, so a step whose
// address no model claims falls through to S3 and shows as NoSuchBucket instead
// of passing on a Host header a real client does not send.
//
// These are client-observed and nothing more: they record what a client asked
// for and what it needed back, never what api.vercel.com answered. A step's
// `want` is the emulator's contract with that client, and a fixture that fails
// means a real client would have been refused.
//
// Ids are the emulator's, so a step names the ones it needs with {{var}} and an
// earlier step captures them from its answer.

type fxRequest struct {
	Method     string            `json:"method"`
	Path       string            `json:"path"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
	BodyBase64 string            `json:"bodyBase64"`
}

type fxWant struct {
	Status int `json:"status"`
	// Members are dotted paths (a.b.0.c) that must be present and not null.
	Members []string `json:"members"`
	// Equals maps a dotted path to the string its value must print as.
	Equals map[string]string `json:"equals"`
	// Absent are dotted paths that must not be there.
	Absent []string `json:"absent"`
}

type fxStep struct {
	Name    string            `json:"name"`
	Note    string            `json:"note"`
	Advance string            `json:"advance"` // a Go duration the clock moves before the request
	Request fxRequest         `json:"request"`
	Want    fxWant            `json:"want"`
	Capture map[string]string `json:"capture"` // variable -> dotted path in the answer
}

type fxFile struct {
	Client   string   `json:"client"`
	Observed string   `json:"observed"`
	Steps    []fxStep `json:"steps"`
}

var fxVar = regexp.MustCompile(`\{\{([A-Za-z0-9_]+)\}\}`)

// fxGet reads a dotted path out of decoded JSON; numbers index arrays.
func fxGet(v any, path string) (any, bool) {
	for _, part := range strings.Split(path, ".") {
		switch node := v.(type) {
		case map[string]any:
			next, ok := node[part]
			if !ok {
				return nil, false
			}
			v = next
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false
			}
			v = node[i]
		default:
			return nil, false
		}
	}
	return v, true
}

func fxFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "clients", "*", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// TestRealClientFixturesReplay replays every recorded journey against a fresh
// emulator and holds each answer to what the client needed from it.
func TestRealClientFixturesReplay(t *testing.T) {
	files := fxFiles(t)
	if len(files) == 0 {
		t.Fatal("testdata/clients holds no recorded journey; the suite would pass by checking nothing")
	}
	for _, file := range files {
		file := file
		t.Run(filepath.Base(filepath.Dir(file))+"/"+strings.TrimSuffix(filepath.Base(file), ".json"), func(t *testing.T) {
			raw, err := os.ReadFile(file)
			if err != nil {
				t.Fatal(err)
			}
			var fx fxFile
			if err := json.Unmarshal(raw, &fx); err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			if fx.Client == "" || fx.Observed == "" || len(fx.Steps) == 0 {
				t.Fatalf("%s must name the client, say how it was observed and hold steps", file)
			}
			v := newVXWith(t, "vercel-client-fixtures", []string{"vercel.api", "aws.s3"})
			vars := map[string]string{}
			for i, step := range fx.Steps {
				t.Run(strconv.Itoa(i+1)+" "+step.Name, func(t *testing.T) {
					fxRun(t, v, step, vars)
				})
			}
		})
	}
}

func fxRun(t *testing.T, v *vx, step fxStep, vars map[string]string) {
	t.Helper()
	sub := func(s string) string {
		return fxVar.ReplaceAllStringFunc(s, func(m string) string {
			name := fxVar.FindStringSubmatch(m)[1]
			val, ok := vars[name]
			if !ok {
				t.Fatalf("%s uses {{%s}}, which no earlier step captured", step.Name, name)
			}
			return val
		})
	}
	if step.Advance != "" {
		d, err := time.ParseDuration(step.Advance)
		if err != nil {
			t.Fatal(err)
		}
		v.advance(d)
	}
	var body io.Reader
	switch {
	case step.Request.BodyBase64 != "":
		b, err := base64.StdEncoding.DecodeString(step.Request.BodyBase64)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(b)
	case step.Request.Body != "":
		body = strings.NewReader(sub(step.Request.Body))
	}
	req, err := http.NewRequest(step.Request.Method, v.ts.URL+sub(step.Request.Path), body)
	if err != nil {
		t.Fatal(err)
	}
	for k, val := range step.Request.Headers {
		req.Header.Set(k, val)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	label := step.Request.Method + " " + req.URL.RequestURI()
	if res.StatusCode != step.Want.Status {
		t.Fatalf("%s = %d, the client needed %d: %.400s", label, res.StatusCode, step.Want.Status, raw)
	}
	var doc any
	if len(step.Want.Members)+len(step.Want.Equals)+len(step.Want.Absent)+len(step.Capture) > 0 {
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s answered %q, which is not JSON: %v", label, clip(raw), err)
		}
	}
	for _, path := range step.Want.Members {
		if got, ok := fxGet(doc, path); !ok || got == nil {
			t.Errorf("%s: the answer has no %q, which the client reads: %.400s", label, path, raw)
		}
	}
	for path, want := range step.Want.Equals {
		got, ok := fxGet(doc, path)
		if want = sub(want); !ok || strings.TrimSpace(printJSON(got)) != want {
			t.Errorf("%s: %s = %v, want %q", label, path, got, want)
		}
	}
	for _, path := range step.Want.Absent {
		if _, ok := fxGet(doc, path); ok {
			t.Errorf("%s: the answer has %q, which it should not: %.400s", label, path, raw)
		}
	}
	for name, path := range step.Capture {
		got, ok := fxGet(doc, path)
		if !ok {
			t.Fatalf("%s: cannot capture %s from %q: %.400s", label, name, path, raw)
		}
		vars[name] = printJSON(got)
	}
}

// printJSON prints a decoded value the way a string compares: strings bare,
// everything else as JSON.
func printJSON(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}
