package bir

import (
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// SchemaVersion is the only schema this package accepts.
const SchemaVersion = "bir/1"

// Compiled holds everything prepared once at load time so that serving a
// request never parses or compiles anything.
type Compiled struct {
	// Programs maps a stable expression path (for example
	// "operations.SendMessage.require[1].cond") to its compiled program.
	Programs map[string]Program
}

// Program is a compiled expression. The engine evaluates it; this package only
// guarantees it compiled and that its references exist.
type Program struct {
	Source string
	// Refs are the top-level identifiers the expression reads.
	Refs []string
}

// Errors is a collection of validation failures. All problems in a bundle are
// reported at once: fixing behavior data one error per run is miserable.
type Errors []error

func (e Errors) Error() string {
	parts := make([]string, len(e))
	for i, err := range e {
		parts[i] = err.Error()
	}
	return strings.Join(parts, "\n")
}

// Load reads every YAML document for one service from fsys and validates the
// result against svc, the generated model for that service.
//
// svc is required. A bundle validated against nothing is a bundle that can
// describe responses the protocol cannot serialize, which is the failure the
// empty-shape catalog already demonstrated.
func Load(fsys fs.FS, dir string, svc *model.Service) (*Service, error) {
	if svc == nil {
		return nil, fmt.Errorf("bir: no generated model supplied for %s", dir)
	}
	if len(svc.Shapes) == 0 {
		return nil, fmt.Errorf("bir: generated model for %s carries no shapes; "+
			"run `make specs-sync && make generate`", svc.ID)
	}

	files, err := collect(fsys, dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("bir: no YAML documents under %s", dir)
	}

	out := &Service{
		Primitives: map[string]PrimRef{},
		Resources:  map[string]Resource{},
		Errors:     map[string]ErrorDef{},
		Limits:     map[string]Limit{},
		Operations: map[string]Operation{},
	}

	var problems Errors
	for _, name := range files {
		raw, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, err
		}
		var doc Service
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true) // an unknown key is a typo, not an extension
		if err := dec.Decode(&doc); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", name, err))
			continue
		}
		if err := merge(out, &doc, name); err != nil {
			problems = append(problems, err)
		}
	}
	if len(problems) > 0 {
		return nil, problems
	}

	if out.Schema != SchemaVersion {
		return nil, fmt.Errorf("bir: %s declares schema %q, want %q", dir, out.Schema, SchemaVersion)
	}
	if out.ServiceID != svc.ID {
		return nil, fmt.Errorf("bir: %s declares service %q but was loaded against model %q",
			dir, out.ServiceID, svc.ID)
	}

	if err := Validate(out, svc); err != nil {
		return nil, err
	}
	return out, nil
}

// collect returns the YAML documents for a service, sorted so that loading is
// deterministic regardless of filesystem order.
func collect(fsys fs.FS, dir string) ([]string, error) {
	var out []string
	err := fs.WalkDir(fsys, dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if ext := path.Ext(p); ext == ".yaml" || ext == ".yml" {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// merge folds one document into the service bundle. Duplicate keys across
// files are an error: two definitions of one operation is ambiguity, and
// silently letting the last file win is how drift starts.
func merge(dst, src *Service, name string) error {
	if src.Schema != "" {
		if dst.Schema != "" && dst.Schema != src.Schema {
			return fmt.Errorf("%s: schema %q conflicts with %q", name, src.Schema, dst.Schema)
		}
		dst.Schema = src.Schema
	}
	if src.ServiceID != "" {
		if dst.ServiceID != "" && dst.ServiceID != src.ServiceID {
			return fmt.Errorf("%s: service %q conflicts with %q", name, src.ServiceID, dst.ServiceID)
		}
		dst.ServiceID = src.ServiceID
	}
	if src.Provenance != "" {
		dst.Provenance = src.Provenance
	}
	// Service-level scalars have to be carried explicitly, and forgetting one
	// is silent: the field parses, validates, and is then dropped on the way
	// into the merged service. Shadow was dropped exactly that way, which left
	// a bundle marked as not-yet-serving registering alongside the pack it was
	// shadowing.
	if src.Shadow != "" {
		dst.Shadow = src.Shadow
	}
	if src.MissingInput != "" {
		dst.MissingInput = src.MissingInput
	}
	dst.Quirks = append(dst.Quirks, src.Quirks...)

	for k, v := range src.Primitives {
		if _, dup := dst.Primitives[k]; dup {
			return fmt.Errorf("%s: primitive %q defined twice", name, k)
		}
		dst.Primitives[k] = v
	}
	for k, v := range src.Resources {
		if _, dup := dst.Resources[k]; dup {
			return fmt.Errorf("%s: resource %q defined twice", name, k)
		}
		dst.Resources[k] = v
	}
	for k, v := range src.Errors {
		if _, dup := dst.Errors[k]; dup {
			return fmt.Errorf("%s: error %q defined twice", name, k)
		}
		dst.Errors[k] = v
	}
	for k, v := range src.Limits {
		if _, dup := dst.Limits[k]; dup {
			return fmt.Errorf("%s: limit %q defined twice", name, k)
		}
		dst.Limits[k] = v
	}
	for k, v := range src.Operations {
		if _, dup := dst.Operations[k]; dup {
			return fmt.Errorf("%s: operation %q defined twice", name, k)
		}
		dst.Operations[k] = v
	}
	return nil
}

// ServiceIDOf reads the service ID a bundle declares, without validating it
// against a model.
//
// The declared ID is the bundle's identity: a directory name cannot be, because
// a service ID may contain characters a directory name should not (aws.iot-data
// lives in aws/iotdata). Callers that need to pair a bundle with its model ask
// the bundle who it is, exactly as generation asks each upstream model.
func ServiceIDOf(fsys fs.FS, dir string) (string, error) {
	files, err := collect(fsys, dir)
	if err != nil {
		return "", err
	}
	for _, name := range files {
		raw, err := fs.ReadFile(fsys, name)
		if err != nil {
			return "", err
		}
		var head struct {
			Service string `yaml:"service"`
		}
		if err := yaml.Unmarshal(raw, &head); err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		if head.Service != "" {
			return head.Service, nil
		}
	}
	return "", fmt.Errorf("bir: no service declared under %s", dir)
}
