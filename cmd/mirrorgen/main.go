// Command mirrorgen ingests vendored specs (or the bootstrap catalog) and
// emits per-service Go packages under internal/generated.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/catalog"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/fusion"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver/aws/smithy"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver/gcp/discovery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver/openapi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specdiff"
)

func main() {
	specsDir := flag.String("specs", "specs", "vendored specs directory")
	setPath := flag.String("set", "specs/mirror.set", "service set file")
	outDir := flag.String("out", "internal/generated", "output directory")
	diff := flag.Bool("diff", false, "print API-surface diff instead of generating")
	jsonOut := flag.Bool("json", false, "machine-readable specdiff JSON")
	forceCatalog := flag.Bool("catalog", false, "generate from the bootstrap catalog only")
	index := flag.String("index", "", "index a spec tree: print `service-id<TAB>path` for every model found, then exit")
	flag.Parse()

	ctx := context.Background()

	// -index exists so scripts/specs-sync.sh never has to guess which upstream
	// directory holds a service. Service IDs are derived by the same receivers
	// that generate the models, so the mapping cannot drift from the model.
	if *index != "" {
		if err := runIndex(ctx, *index, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	want, err := loadSet(*setPath)
	if err != nil && !*forceCatalog {
		fmt.Fprintf(os.Stderr, "mirrorgen: %v (continuing with all ingested services)\n", err)
	}

	ingested, srcNote, err := loadBundle(ctx, *specsDir, *forceCatalog)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, srcNote)

	if *diff {
		if err := runDiff(flag.Args(), ingested, *jsonOut); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	svcs := ingested.Services
	if len(want) > 0 {
		svcs = filterSet(svcs, want)
		// After the set decides which services are generated, and before they
		// are emitted: a service may also declare which of its operations it
		// wants, for the vendor that publishes one document per platform.
		narrowed, err := narrowAll(svcs, want)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		svcs = narrowed
	}
	if err := emitAll(*outDir, svcs); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "wrote %d service package(s) under %s\n", len(svcs), *outDir)
}

func loadBundle(ctx context.Context, specsDir string, forceCatalog bool) (model.Bundle, string, error) {
	if forceCatalog {
		b := catalog.Bundle()
		return *b, "mirrorgen: using bootstrap catalog", nil
	}
	groups, n, err := ingestSpecs(ctx, specsDir)
	if err != nil {
		return model.Bundle{}, "", err
	}
	if n == 0 {
		b := catalog.Bundle()
		return *b, "mirrorgen: no vendored specs; using bootstrap catalog", nil
	}
	// Fragments are fused per provider, and the provider is the first segment
	// of the service ID -- the same thing the rest of the system reads it as.
	//
	// This used to be `if strings.HasPrefix(id, "gcp.")` with everything else
	// falling to AWS, which was true while there were two providers and
	// silently wrong for a third: a `hostinger.api` document would have been
	// fused as AWS. `internal/model`'s own doc comment says nothing may branch
	// on a specific Provider value, and this was the branch.
	byProvider := map[model.Provider][][]model.Service{}
	var order []model.Provider
	for _, g := range groups {
		if len(g) == 0 {
			continue
		}
		provider := model.ProviderAWS
		if name, _, ok := strings.Cut(g[0].ID, "."); ok && name != "" {
			provider = model.Provider(name)
		}
		if _, seen := byProvider[provider]; !seen {
			order = append(order, provider)
		}
		byProvider[provider] = append(byProvider[provider], g)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	out := model.Bundle{SchemaVersion: "1"}
	for _, provider := range order {
		b, _, err := fusion.Fuse(ctx, provider, byProvider[provider])
		if err != nil {
			return model.Bundle{}, "", err
		}
		out.Services = append(out.Services, b.Services...)
		out.Sources = append(out.Sources, b.Sources...)
		if out.Provider == "" {
			out.Provider = provider
		}
	}
	sort.Slice(out.Services, func(i, j int) bool { return out.Services[i].ID < out.Services[j].ID })
	return out, fmt.Sprintf("mirrorgen: ingested %d spec file(s), %d service(s)", n, len(out.Services)), nil
}

func ingestSpecs(ctx context.Context, specsDir string) ([][]model.Service, int, error) {
	// Order matters only in that each Detect is exclusive: Smithy documents
	// declare `smithy`, Discovery declares `discoveryVersion`, OpenAPI declares
	// `openapi`, and no document carries two.
	recvs := []receiver.Receiver{smithy.Receiver{}, discovery.Receiver{}, openapi.Receiver{}}
	var groups [][]model.Service
	n := 0
	if _, err := os.Stat(specsDir); err != nil {
		return nil, 0, nil
	}
	// Absent or unreadable lock: fall back to the ID each model declares. That
	// is correct for the index (which has no lock) and for the ~82% of services
	// whose declared ID is already canonical.
	canonical, _ := lockIDs(specsDir)
	err := filepath.WalkDir(specsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		base := d.Name()
		if !strings.HasSuffix(strings.ToLower(base), ".json") {
			return nil
		}
		if base == "mirror.lock" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		head := data
		if len(head) > 4096 {
			head = head[:4096]
		}
		rel, _ := filepath.Rel(specsDir, path)
		src := model.SourceRef{Path: rel, SHA256: sha256Hex(data)}
		for _, r := range recvs {
			if !r.Detect(path, head) {
				continue
			}
			svcs, err := r.Ingest(ctx, src, data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "mirrorgen: skip %s: %v\n", path, err)
				return nil
			}
			// One file declares one service; when the lock names it, that name
			// wins so aliased services (CloudWatch, ELBv2, DocumentDB, …) are
			// generated under the ID the rest of the system uses.
			if id, ok := canonical[rel]; ok && len(svcs) == 1 {
				svcs[0].ID = id
			}
			groups = append(groups, svcs)
			n++
			return nil
		}
		return nil
	})
	return groups, n, err
}

func runDiff(args []string, ingested model.Bundle, asJSON bool) error {
	var oldB, newB model.Bundle
	switch len(args) {
	case 0:
		oldB = *catalog.Bundle()
		newB = ingested
	case 2:
		var err error
		oldB, err = readBundle(args[0])
		if err != nil {
			return err
		}
		newB, err = readBundle(args[1])
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("usage: mirrorgen --diff [old.json new.json]")
	}
	rep := specdiff.Diff(oldB, newB)
	if asJSON {
		return rep.WriteJSON(os.Stdout)
	}
	fmt.Print(rep.String())
	return nil
}

func readBundle(path string) (model.Bundle, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return model.Bundle{}, err
	}
	var out model.Bundle
	if err := json.Unmarshal(b, &out); err != nil {
		return model.Bundle{}, err
	}
	return out, nil
}

type setEntry struct {
	ID   string
	Tier model.Tier
	// Paths narrows a service to the operations whose URI begins with one of
	// these prefixes, for the vendor that publishes one document per platform
	// rather than one per service. Empty means the whole document, which is
	// every service today. See narrow.go.
	Paths []string
}

func loadSet(path string) ([]setEntry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []setEntry
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		e := setEntry{ID: fields[0], Tier: model.TierMock}
		if len(fields) > 1 {
			e.Tier = model.Tier(fields[1])
		}
		if len(fields) > 2 {
			paths, err := parseSelector(fields[2:])
			if err != nil {
				return nil, fmt.Errorf("%s: %s: %w", path, fields[0], err)
			}
			e.Paths = paths
		}
		out = append(out, e)
	}
	return out, nil
}

func filterSet(svcs []model.Service, want []setEntry) []model.Service {
	keep := map[string]bool{}
	for _, e := range want {
		keep[e.ID] = true
	}
	var out []model.Service
	var missing []string
	have := map[string]bool{}
	for _, s := range svcs {
		if keep[s.ID] {
			out = append(out, s)
			have[s.ID] = true
		}
	}
	for _, e := range want {
		if !have[e.ID] {
			missing = append(missing, e.ID)
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "mirrorgen: not in ingested set (need specs-sync): %s\n", strings.Join(missing, ", "))
	}
	// The converse, which used to pass in silence: a document under specs/
	// that no line of specs/mirror.set asks for. It is ingested, counted in
	// "ingested N service(s)", and then dropped -- so the run says 153 and
	// writes 152, exit 0, with nothing naming the one that vanished.
	//
	// That is how a spec is added and the model never appears. It is the same
	// shape as the embed pattern that listed two providers by name: a file
	// sitting in the tree that the pipeline ignores, where the only symptom is
	// a later "no model for <service>" that reads like a missing document
	// rather than a missing declaration.
	var undeclared []string
	for _, s := range svcs {
		if !keep[s.ID] {
			undeclared = append(undeclared, s.ID)
		}
	}
	if len(undeclared) > 0 {
		sort.Strings(undeclared)
		fmt.Fprintf(os.Stderr, "mirrorgen: ingested but not declared in specs/mirror.set, "+
			"so no model is written: %s\n", strings.Join(undeclared, ", "))
	}
	return out
}

func emitAll(outDir string, svcs []model.Service) error {
	for _, svc := range svcs {
		if err := emitService(outDir, svc); err != nil {
			return err
		}
	}
	return emitIndex(outDir)
}

func emitService(outDir string, svc model.Service) error {
	provider, pkg := splitID(svc.ID)
	dir := filepath.Join(outDir, provider, pkg)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	raw, err := marshalService(svc)
	if err != nil {
		return err
	}
	// Models are committed, and the uncompressed set is ~94 MB against ~2.6 MB
	// gzipped — JSON of this shape compresses roughly 36x because every shape
	// repeats the same key names. Store compressed and inflate lazily so only
	// the services actually served pay for parsing.
	gz, err := gzipBytes(raw)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "model.json.gz"), gz, 0o644); err != nil {
		return err
	}
	// Remove any uncompressed model left by an earlier generator so the tree
	// never carries two sources of truth.
	if err := os.Remove(filepath.Join(dir, "model.json")); err != nil && !os.IsNotExist(err) {
		return err
	}
	goSrc := fmt.Sprintf(modelGoTmpl, pkg, pkg)
	return os.WriteFile(filepath.Join(dir, "model.go"), []byte(goSrc), 0o644)
}

func splitID(id string) (provider, pkg string) {
	provider, rest, ok := strings.Cut(id, ".")
	if !ok {
		provider, rest = "unknown", id
	}
	pkg = sanitizePkg(rest)
	if pkg == "" {
		pkg = "service"
	}
	return provider, pkg
}

func sanitizePkg(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out != "" && unicode.IsDigit(rune(out[0])) {
		out = "s" + out
	}
	return out
}

func marshalService(svc model.Service) ([]byte, error) {
	sort.Slice(svc.Operations, func(i, j int) bool { return svc.Operations[i].Name < svc.Operations[j].Name })
	b, err := json.Marshal(svc)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, b, "", "  "); err != nil {
		return nil, err
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

const modelGoTmpl = `// Code generated by mirrorgen; DO NOT EDIT.

// Package %s is the lazily-parsed canonical model for this service.
package %s

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/json"
	"io"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

//go:embed model.json.gz
var raw []byte

var (
	once sync.Once
	svc  *model.Service
)

// Model returns the canonical service model, inflated and parsed once on
// first use. Services that are never served cost only their compressed bytes.
func Model() *model.Service {
	once.Do(func() {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			panic("mirrorgen: " + err.Error())
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			panic("mirrorgen: " + err.Error())
		}
		svc = new(model.Service)
		if err := json.Unmarshal(plain, svc); err != nil {
			panic("mirrorgen: " + err.Error())
		}
	})
	return svc
}

// Operations returns operation names in model order.
func Operations() []string {
	m := Model()
	out := make([]string, len(m.Operations))
	for i, op := range m.Operations {
		out[i] = op.Name
	}
	return out
}
`

// runIndex walks a spec tree and reports every service it can ingest, one
// `service-id<TAB>relative-path` pair per line, sorted. Ingestion errors on
// individual files are reported to stderr by ingestSpecs and skipped: an
// unparseable upstream model should not stop the index.
func runIndex(ctx context.Context, dir string, out io.Writer) error {
	groups, n, err := ingestSpecs(ctx, dir)
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("mirrorgen: no models found under %s", dir)
	}
	lines := make([]string, 0, n)
	for _, g := range groups {
		for _, svc := range g {
			lines = append(lines, svc.ID+"\t"+svc.Source.Path)
		}
	}
	sort.Strings(lines)
	for _, l := range lines {
		if _, err := fmt.Fprintln(out, l); err != nil {
			return err
		}
	}
	return nil
}

// gzipBytes compresses at maximum level with no timestamp or filename in the
// header, so regeneration from identical input is byte-identical.
func gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(b); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// lockIDs maps a spec path (relative to the specs directory) to the canonical
// mirror.cloud service ID recorded by specs-sync.
//
// A model's declared endpointPrefix is neither always our ID (CloudWatch
// declares "monitoring") nor unique across services (RDS, DocumentDB, and
// Neptune all declare "rds"). The lock is written by the same resolution that
// selected the file, so it is the one place both facts are already settled.
func lockIDs(specsDir string) (map[string]string, error) {
	b, err := os.ReadFile(filepath.Join(specsDir, "mirror.lock"))
	if err != nil {
		return nil, err
	}
	var doc struct {
		Files []struct {
			ServiceID string `json:"serviceId"`
			Path      string `json:"path"`
		} `json:"files"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(doc.Files))
	for _, f := range doc.Files {
		if f.ServiceID != "" && f.Path != "" {
			out[filepath.FromSlash(f.Path)] = f.ServiceID
		}
	}
	return out, nil
}

// indexGoTmpl is the by-ID lookup the engine uses. One package embeds every
// model, decoding a service only when it is first asked for, so a binary
// carries the compressed set but pays parse cost only for services it serves.
const indexGoTmpl = `// Code generated by mirrorgen; DO NOT EDIT.

// Package generated indexes the canonical service models by service ID.
package generated

import (
	"bytes"
	"compress/gzip"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// The pattern is the layout, not a list of providers. It was
// ` + "`" + `all:aws all:gcp` + "`" + `, so a model generated for a third provider was written
// here, committed, and then not embedded: Model answered "no model for
// hostinger.api" with the file sitting in the tree. Nothing failed at build
// time, because an unembedded directory is indistinguishable from a provider
// with no models. TestEveryGeneratedModelIsEmbedded is the guard.
//
//go:embed */*/model.json.gz
var files embed.FS

// FS exposes the embedded models, so a test can ask what is actually in the
// binary rather than what is on disk. The two differing is the failure mode
// the embed pattern above describes.
func FS() fs.FS { return files }

var (
	mu     sync.Mutex
	cached = map[string]*model.Service{}
)

// Model returns the canonical model for a service ID, parsed on first use.
func Model(serviceID string) (*model.Service, error) {
	mu.Lock()
	defer mu.Unlock()
	if svc, ok := cached[serviceID]; ok {
		return svc, nil
	}
	provider, rest, ok := strings.Cut(serviceID, ".")
	if !ok {
		return nil, fmt.Errorf("generated: %q has no provider prefix", serviceID)
	}
	var pkg strings.Builder
	for _, r := range strings.ToLower(rest) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			pkg.WriteRune(r)
		}
	}
	raw, err := files.ReadFile(path.Join(provider, pkg.String(), "model.json.gz"))
	if err != nil {
		return nil, fmt.Errorf("generated: no model for %s: %w", serviceID, err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	plain, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	svc := new(model.Service)
	if err := json.Unmarshal(plain, svc); err != nil {
		return nil, err
	}
	cached[serviceID] = svc
	return svc, nil
}

// ServiceIDs lists every generated service, sorted.
func ServiceIDs() []string {
	var out []string
	providers, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil
	}
	for _, p := range providers {
		if !p.IsDir() {
			continue
		}
		svcs, err := fs.ReadDir(files, p.Name())
		if err != nil {
			continue
		}
		for _, s := range svcs {
			if !s.IsDir() {
				continue
			}
			svc, err := Model(p.Name() + "." + s.Name())
			if err == nil {
				out = append(out, svc.ID)
			}
		}
	}
	sort.Strings(out)
	return out
}
`

// emitIndex writes the by-ID lookup package.
func emitIndex(outDir string) error {
	return os.WriteFile(filepath.Join(outDir, "index.go"), []byte(indexGoTmpl), 0o644)
}
