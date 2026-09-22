package vercel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
)

// boaConfig is the subset of Build Output API v3 config.json we validate.
type boaConfig struct {
	Version   int                    `json:"version"`
	Routes    []boaRoute             `json:"routes"`
	Overrides map[string]boaOverride `json:"overrides"`
}

type boaRoute struct {
	Src  string `json:"src"`
	Dest string `json:"dest"`
}

type boaOverride struct {
	Path string `json:"path"`
}

type vcConfig struct {
	Runtime string `json:"runtime"`
	Handler string `json:"handler"`
}

// PublishBOA publishes `.vercel/output` static/ via delivery.PublishDir.
// Optional config.json (version 3) is validated: route dest / override keys
// that name local static files must exist. Node functions: see InvokeFunction.
func PublishBOA(outputDir string) (*delivery.Site, error) {
	staticDir := filepath.Join(outputDir, "static")
	st, err := os.Stat(staticDir)
	if err != nil {
		return nil, fmt.Errorf("vercel boa: static/: %w", err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("vercel boa: static/ not a directory")
	}
	cfgPath := filepath.Join(outputDir, "config.json")
	if raw, err := os.ReadFile(cfgPath); err == nil {
		var cfg boaConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("vercel boa: config.json: %w", err)
		}
		if cfg.Version != 0 && cfg.Version != 3 {
			return nil, fmt.Errorf("vercel boa: unsupported config version %d (want 3)", cfg.Version)
		}
		if err := validateBOARefs(staticDir, cfg); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("vercel boa: config.json: %w", err)
	}
	return delivery.PublishDir(staticDir)
}

// InvokeFunction runs a bounded Node Serverless Function from
// outputDir/functions/<name>.func/ (.vc-config.json runtime nodejs*, handler).
// Only handlers that export a default (req,res) function — ESM default or
// module.exports = (req,res)=>… — via a tiny node harness writing the body.
// path is the request URL passed to the handler (e.g. "/api/hello").
func InvokeFunction(outputDir, name, path string) ([]byte, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("vercel boa: empty function name")
	}
	if strings.Contains(name, "..") {
		return nil, fmt.Errorf("vercel boa: bad function name %q", name)
	}
	funcsRoot := filepath.Clean(filepath.Join(outputDir, "functions"))
	funcDir := filepath.Clean(filepath.Join(funcsRoot, name+".func"))
	if funcDir != funcsRoot && !strings.HasPrefix(funcDir, funcsRoot+string(os.PathSeparator)) {
		return nil, fmt.Errorf("vercel boa: function path escapes: %q", name)
	}
	raw, err := os.ReadFile(filepath.Join(funcDir, ".vc-config.json"))
	if err != nil {
		return nil, fmt.Errorf("vercel boa: .vc-config.json: %w", err)
	}
	var cfg vcConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("vercel boa: .vc-config.json: %w", err)
	}
	rt := strings.ToLower(strings.TrimSpace(cfg.Runtime))
	if !strings.HasPrefix(rt, "nodejs") {
		return nil, fmt.Errorf("vercel boa: unsupported runtime %q (want nodejs*)", cfg.Runtime)
	}
	handler := strings.TrimSpace(cfg.Handler)
	if handler == "" || strings.Contains(handler, "..") || filepath.IsAbs(handler) {
		return nil, fmt.Errorf("vercel boa: bad handler %q", cfg.Handler)
	}
	handlerPath := filepath.Join(funcDir, handler)
	if _, err := os.Stat(handlerPath); err != nil {
		return nil, fmt.Errorf("vercel boa: handler file: %w", err)
	}
	node, err := exec.LookPath("node")
	if err != nil {
		return nil, fmt.Errorf("vercel boa: node: %w", err)
	}
	if path == "" {
		path = "/"
	}
	// ponytail: one-shot req/res shim; full Vercel/Next/edge runtime later.
	script := `
const { pathToFileURL } = require('url');
const handlerPath = process.env.MIRROR_VC_HANDLER;
const reqPath = process.env.MIRROR_VC_PATH || '/';
(async () => {
  const m = await import(pathToFileURL(handlerPath).href);
  let fn = m.default;
  if (typeof fn !== 'function' && typeof m === 'function') fn = m;
  if (typeof fn !== 'function') {
    process.stderr.write('vercel boa: handler must export default (req,res) function\n');
    process.exit(2);
  }
  const req = { method: 'GET', url: reqPath, headers: {}, query: {} };
  let done = false;
  const res = {
    statusCode: 200,
    setHeader() { return this; },
    getHeader() { return undefined; },
    status(c) { this.statusCode = c; return this; },
    json(o) { return this.end(JSON.stringify(o)); },
    send(b) { return this.end(b); },
    end(b) {
      if (done) return this;
      done = true;
      if (b != null && b !== undefined) process.stdout.write(typeof b === 'string' ? b : Buffer.from(b));
      return this;
    },
  };
  await Promise.resolve(fn(req, res));
  if (!done) res.end('');
})().catch((e) => { process.stderr.write(String(e && e.stack || e)); process.exit(1); });
`
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node, "-e", script)
	cmd.Dir = funcDir
	cmd.Env = append(process.ScrubEnv(os.Environ()),
		"MIRROR_VC_HANDLER="+handlerPath,
		"MIRROR_VC_PATH="+path,
	)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("vercel boa: invoke %s: %w\n%s", name, err, ee.Stderr)
		}
		return nil, fmt.Errorf("vercel boa: invoke %s: %w", name, err)
	}
	return out, nil
}


func validateBOARefs(staticDir string, cfg boaConfig) error {
	for _, r := range cfg.Routes {
		if err := requireStaticFile(staticDir, r.Dest); err != nil {
			return err
		}
	}
	for name := range cfg.Overrides {
		if err := requireStaticFile(staticDir, name); err != nil {
			return err
		}
	}
	return nil
}

func requireStaticFile(staticDir, ref string) error {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	if strings.Contains(ref, "://") {
		return nil // absolute URL dest — not a static file
	}
	rel := strings.TrimPrefix(filepath.Clean("/"+ref), "/")
	if rel == "" || rel == "." {
		return nil
	}
	path := filepath.Join(staticDir, rel)
	if !strings.HasPrefix(path, filepath.Clean(staticDir)+string(os.PathSeparator)) {
		return fmt.Errorf("vercel boa: path escapes static/: %q", ref)
	}
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("vercel boa: missing static file %q: %w", ref, err)
	}
	if st.IsDir() {
		return fmt.Errorf("vercel boa: static ref %q is a directory", ref)
	}
	return nil
}
