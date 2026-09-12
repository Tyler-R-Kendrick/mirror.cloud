package restjson

import "testing"

// Railway's every request is one POST to one path, so the operation has to come
// out of the query document. That made routing a substring switch over the seven
// field names, longest first -- and a substring switch is wrong on documents
// nobody would call adversarial.
//
// The schema declares both `project` and `projectId` on Service, so selecting a
// service's project is the natural query and the old switch answered it with
// the project handler. These cases are the ones that were broken, plus the
// ones that were not and the shapes the scan has to understand at all, so a
// future rewrite -- a real parser, per C53 -- has to keep every half.
func TestGraphQLRootField(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
		want string
	}{
		// Reinstating the substring switch fails six of the cases below. Three
		// are this group: a document that names the parent of the thing it asks
		// for, which Railway's schema makes the natural query.
		{"scalar naming the parent", `query { service(id:"s") { id projectId } }`, "service"},
		{"selecting the parent object", `query { service(id:"s") { id project { name } } }`, "service"},
		{"variable naming the parent", `query GetService($projectId: String!) { service(id: $projectId) { id } }`, "service"},

		// The ones it got right, which must stay right.
		{"shorthand", `{ projects { edges { node { id } } } }`, "projects"},
		{"mutation", `mutation { serviceCreate(input:{projectId:"p", name:"api"}) { id } }`, "serviceCreate"},
		{"delete", `mutation Kill { projectDelete(id:"p") }`, "projectDelete"},
		{"parent selecting children", `query { project(id:"p") { services { edges { node { id } } } } }`, "project"},

		// The other three. Each is a place a field name can sit that is not the
		// root field, and the string-literal one is the sharpest: a service whose
		// NAME contains a field name sent a create to the delete handler.
		{"comment", "# projectCreate is not what this asks for\nquery { projects { edges { node { id } } } }", "projects"},
		{"string literal", `mutation { serviceCreate(input:{name:"projectDelete"}) { id } }`, "serviceCreate"},
		{"paren in a default", `query Q($n: String = "a)b") { projects { edges { node { id } } } }`, "projects"},
		{"operation name", `query projectCreateAudit { projects { edges { node { id } } } }`, "projects"},

		// Shapes the scanner has to understand to answer at all.
		{"alias", `query { mine: projects { edges { node { id } } } }`, "projects"},
		{"leading fragment", "fragment F on Project { id }\nquery { project(id:\"p\") { ...F } }", "project"},
		{"directive", `query Q($d: Boolean!) @skip(if: $d) { projects { edges { node { id } } } }`, "projects"},

		// A comment is the one place a brace can appear that opens nothing, so
		// every scan has to know it -- not just the one skipping ignored tokens.
		// Both of these routed on a comment's TEXT before the scans shared it.
		{"comment before the selection set", "query Q # a { brace in a comment\n { projects { id } }", "projects"},
		{"comment closing a fragment body", "fragment F on Project { # }\n id }\nquery { projects { id } }", "projects"},

		// A root selection may be a spread or an inline fragment rather than a
		// field. The substring switch followed these by accident, because the
		// field name was somewhere in the document; the scan has to mean it.
		{"named spread", "fragment F on Query { projects { id } }\nquery { ...F }", "projects"},
		{"spread defined after its use", "query { ...F }\nfragment F on Query { service(id:\"s\") { id } }", "service"},
		{"spread past a same-prefix fragment", "fragment FF on Q { service { id } }\nfragment F on Q { projects { id } }\nquery { ...F }", "projects"},
		{"inline fragment with a type", `query { ... on Query { projects { id } } }`, "projects"},
		{"inline fragment with no type", `query { ... { projects { id } } }`, "projects"},

		// Nothing to route to.
		{"mutually cyclic fragments", "fragment A on Q { ...B }\nfragment B on Q { ...A }\nquery { ...A }", "Unknown"},
		{"self-cyclic fragment", "fragment A on Q { ...A }\nquery { ...A }", "Unknown"},
		{"spread naming no fragment", `query { ...Nope }`, "Unknown"},
		{"empty", ``, "Unknown"},
		{"not a document", `{{{`, "Unknown"},
		{"unterminated", `query {`, "Unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gqlRootField(tc.doc); got != tc.want {
				t.Fatalf("gqlRootField(%q) = %q, want %q", tc.doc, got, tc.want)
			}
		})
	}
}

// An operation the service does not serve now routes to its own name rather
// than to "Unknown", so the not-implemented fault and the x-mirror-not-
// implemented header say which field was asked for.
func TestGraphQLRootFieldNamesAnUnservedField(t *testing.T) {
	if got := gqlRootField(`mutation { deploymentCreate(input:{}) { id } }`); got != "deploymentCreate" {
		t.Fatalf("unserved field = %q, want deploymentCreate", got)
	}
}
