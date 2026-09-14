package check

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// The demux's provider guesses, counted.
//
// C34 records what they are and why they keep costing an AWS service. Every
// provider that has entered this emulator entered the demux as a substring
// guess over the host or the path, and all of them run before the model is
// asked. Vercel took aws.api.ecr and aws.iotwireless by name, because the short
// name of `vercel.api` is the generic word `api`. Fly took aws.pinpoint by
// path: `flyRequest` is true of anything containing `/v1/apps`, and that is
// also Pinpoint's GetApps. The collision always lives in the NEW service, so no
// test of the service that was taken could fail.
//
// C55 records why nobody could see them growing: the ratchet counts one
// directory, internal/services, and these are in internal/edge. Provider code
// outside that one directory is invisible to every metric here, which is how
// 282 lines of one vendor's routing sat in the AWS REST-JSON codec until
// someone happened to look.
//
// Seven of the eight are now gone, which is what this metric was built to
// make visible: a request naming no AWS service is placed from the non-AWS
// models, by the host its specification declares and otherwise by the
// operation paths it declares. azureRequest remains because Azure's documents
// declare neither -- no `servers`, and every operation bound to `/` because
// the surface is addressed by query parameter and header.
//
// The count stays because the pressure that produced eight has not gone away.
// The next provider to arrive will be easier to add as a predicate than as a
// specification, and this is what makes that choice visible rather than
// quiet.

const edgeRel = "internal/edge"

// providerGuessSuffixes are the two shapes the demux's branches take: a
// predicate asking whether a request is a provider's, and a resolver returning
// which of that provider's services it is.
var providerGuessSuffixes = []string{"Request", "Service"}

// MeasureDemuxGuesses counts provider-named routing functions in internal/edge
// and returns them sorted.
//
// A function counts when its name is a provider followed by one of the
// suffixes, and a provider is a first-level directory under internal/generated
// or internal/services. Deriving the provider list from the tree rather than
// writing it down is what keeps this honest in both directions: a provider
// added to either place is counted without anyone remembering to, and the two
// sources are unioned because NEITHER alone is complete. azure has no generated
// model and no entry in specs/mirror.set -- that is exactly what makes
// TestEveryServedServiceIsDescribedByASpecification red for it -- so a list
// drawn from specs or from generated models would miss `azureRequest`, which is
// one of the eight this exists to count.
//
// The suffixes keep it narrow. `claimingRequest`, `sdkRequest`,
// `credentialScopeService` and the five streaming*Request helpers are not
// provider-named and are not counted; they are the demux doing its own job.
func MeasureDemuxGuesses(root string) (int, []string, error) {
	providers, err := providerNames(root)
	if err != nil {
		return 0, nil, err
	}
	dir := filepath.Join(root, filepath.FromSlash(edgeRel))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, nil // no edge package: nothing to guess with
		}
		return 0, nil, err
	}

	var found []string
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, goSuffix) || strings.HasSuffix(name, testSuffix) {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			return 0, nil, err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			if provider := guessedProvider(fn.Name.Name, providers); provider != "" {
				found = append(found, fn.Name.Name)
			}
		}
	}
	sort.Strings(found)
	return len(found), found, nil
}

// guessedProvider answers which provider a function name guesses for, or "".
func guessedProvider(name string, providers map[string]bool) string {
	for _, suffix := range providerGuessSuffixes {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		if stem := strings.TrimSuffix(name, suffix); providers[stem] {
			return stem
		}
	}
	return ""
}

// providerNames reads the providers out of the tree: the first-level
// directories under internal/generated and internal/services.
func providerNames(root string) (map[string]bool, error) {
	providers := map[string]bool{}
	for _, rel := range []string{"internal/generated", servicesRel} {
		entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				providers[strings.ToLower(e.Name())] = true
			}
		}
	}
	return providers, nil
}
