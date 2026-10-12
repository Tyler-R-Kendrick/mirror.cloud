package vercel

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
)

// exchange is one request the acceptance script made and the answer it got.
type exchange struct {
	method, path string
	status       int
	body         []byte
}

type capture struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (c *capture) WriteHeader(code int) { c.status = code; c.ResponseWriter.WriteHeader(code) }
func (c *capture) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	c.buf.Write(b)
	return c.ResponseWriter.Write(b)
}

// TestTheAcceptanceScriptDeploysAndItsAnswersAreTheDocuments runs
// scripts/vercel-acceptance.sh -- the deploy path of a Vercel client, in curl --
// against a booted emulator on a real clock, and then holds every answer it got
// to the schema the vendored document declares for the operation that gave it.
// The script asserts on what it needs; this asserts that what came back is what
// a client generated from the document would accept.
func TestTheAcceptanceScriptDeploysAndItsAnswersAreTheDocuments(t *testing.T) {
	for _, tool := range []string{"bash", "curl", "jq"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed; the acceptance script needs bash, curl and jq", tool)
		}
	}
	cfg := config.Default()
	cfg.Services = []string{"vercel.api", "aws.s3"}
	cfg.Seed = "vercel-acceptance"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu        sync.Mutex
		exchanges []exchange
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := &capture{ResponseWriter: w}
		rt.Handler().ServeHTTP(c, r)
		mu.Lock()
		exchanges = append(exchanges, exchange{r.Method, r.URL.Path, c.status, c.buf.Bytes()})
		mu.Unlock()
	}))
	defer ts.Close()

	script, err := filepath.Abs(filepath.Join("..", "..", "..", "scripts", "vercel-acceptance.sh"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script)
	cmd.Env = append(os.Environ(), "MIRROR_URL="+ts.URL, "ACCEPT_WAIT_SECONDS=20")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the acceptance script failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "accepted:") {
		t.Fatalf("the script did not say it accepted:\n%s", out)
	}

	c := newConf(t)
	validated := map[string]bool{}
	for _, ex := range exchanges {
		var op *confOp
		for _, o := range c.ops {
			if o.method != ex.method {
				continue
			}
			for _, tmpl := range append([]string{o.uri}, o.alts...) {
				if templateMatches(tmpl, ex.path) {
					op = o
				}
			}
		}
		if op == nil {
			t.Errorf("%s %s is served by no operation of the model", ex.method, ex.path)
			continue
		}
		if op.doc == nil || len(ex.body) == 0 || ex.status >= 400 {
			continue
		}
		schema, declared := op.doc.responseSchema(op.spec, ex.status)
		if !declared || schema == nil {
			continue
		}
		var inst any
		if err := json.Unmarshal(ex.body, &inst); err != nil {
			t.Errorf("%s %s answered %d with a body that is not JSON: %s", ex.method, ex.path, ex.status, clip(ex.body))
			continue
		}
		if errs := op.doc.validate(schema, inst); len(errs) > 0 {
			t.Errorf("%s %s (%s) %d does not satisfy %s:\n    %s\n  body: %s", ex.method, ex.path, op.name, ex.status, op.doc.name, strings.Join(clipList(errs, 8), "\n    "), clip(ex.body))
		}
		validated[op.name] = true
	}
	for _, want := range []string{"CreateProject", "CreateProjectEnv", "FilterProjectEnvs", "UploadFile", "CreateDeployment", "GetDeployment", "AssignAlias", "GetAlias", "GetDeployments", "GetProjects"} {
		if !validated[want] {
			t.Errorf("the script's answer from %s was not held to the document (validated: %v)", want, validated)
		}
	}
}
