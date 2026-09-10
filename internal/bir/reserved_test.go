package bir

import (
	"strings"
	"testing"
)

// A resource name that collides with something the engine binds is a mistake
// with two very different failure modes, and neither of them names the
// resource. These tests hold both, and hold the line between them: the check
// exists to explain a collision, not to forbid every name the engine happens
// to use somewhere.

// TestAResourceMayNotBeNamedForAnEngineVariable is the loud case. Four names
// are declared for every expression in every bundle, so a resource of that
// name is a second declaration: CEL rejects it and reports "overlapping
// identifier" against every expression in the service, including ones that
// never mention it.
//
// Comprehend has an inference endpoint, and `endpoint` is the obvious name for
// it, so this is reachable by writing the obvious thing.
func TestAResourceMayNotBeNamedForAnEngineVariable(t *testing.T) {
	for _, name := range ReservedVariables {
		t.Run(name, func(t *testing.T) {
			body := strings.ReplaceAll(demoBundle, "  thing:", "  "+name+":")
			body = strings.ReplaceAll(body, "resource: thing", "resource: "+name)
			_, err := loadDemo(t, body)
			if err == nil {
				t.Fatalf("a resource named %q loaded", name)
			}
			if !strings.Contains(err.Error(), "rename the resource") {
				t.Errorf("error does not say what to do: %v", err)
			}
			if !strings.Contains(err.Error(), "resources."+name) {
				t.Errorf("error does not name the resource: %v", err)
			}
		})
	}
}

// TestAParentMayNotBeNamedForAnEngineBinding is the quiet case. A resource
// called `event` is harmless until something is scoped under it, at which
// point `event.id` in that resource's collection means the statechart's event
// rather than the parent record -- and it compiles either way, so nothing
// says which was meant.
func TestAParentMayNotBeNamedForAnEngineBinding(t *testing.T) {
	body := strings.ReplaceAll(demoBundle, "  thing:", "  item:")
	body = strings.ReplaceAll(body, "resource: thing", "resource: item")
	body = strings.Replace(body, "errors:", `  part:
    collection: "parts:{item.id}"
    parent: item
    record:
      Id: id
errors:`, 1)
	_, err := loadDemo(t, body)
	if err == nil {
		t.Fatal("a resource scoped under a resource named `item` loaded")
	}
	if !strings.Contains(err.Error(), `"part" is scoped under it`) {
		t.Errorf("error does not name the child that makes this ambiguous: %v", err)
	}
}

// TestAnUnusedBindingNameIsLeftAlone keeps the check honest. CloudTrail names
// a resource `event` and nothing is scoped under it, so nothing is ambiguous
// and the bundle loads. A check that refused it anyway would be asking for a
// rename to prevent a problem that cannot occur.
func TestAnUnusedBindingNameIsLeftAlone(t *testing.T) {
	body := strings.ReplaceAll(demoBundle, "  thing:", "  event:")
	body = strings.ReplaceAll(body, "resource: thing", "resource: event")
	if _, err := loadDemo(t, body); err != nil {
		t.Fatalf("a resource named `event` with nothing under it should load: %v", err)
	}
}
