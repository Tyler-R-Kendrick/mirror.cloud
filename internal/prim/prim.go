// Package prim is the engine primitive registry: named, versioned Go
// functions a bundle calls through prim(name, args). Primitives are the
// escape hatch for genuinely algorithmic semantics (expression evaluation,
// query matching, checksums) that CEL cannot express; they are budgeted so
// they cannot become the main path.
//
// A bundle declares the primitives it calls under `primitives:` with the
// version it was written against, and the loader refuses an unknown name or
// a version the registry does not carry. The call itself names the primitive
// directly; an unregistered name fails loudly at request time rather than
// returning a plausible-looking value.
package prim

import (
	"fmt"
	"sort"
	"sync"
)

// Func is one primitive: a pure function of its arguments.
type Func struct {
	Name    string
	Version int
	Call    func(args []any) (any, error)
}

var (
	mu       sync.RWMutex
	registry = map[string]Func{}
)

// Register adds a primitive. It panics on a duplicate name, because two
// implementations of one name would make the bundle's version pin a lie.
func Register(f Func) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := registry[f.Name]; dup {
		panic(fmt.Sprintf("prim: duplicate primitive %q", f.Name))
	}
	registry[f.Name] = f
}

// Lookup answers the primitive and whether it is registered.
func Lookup(name string) (Func, bool) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := registry[name]
	return f, ok
}

// Names lists the registered primitives, sorted, for error messages.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
