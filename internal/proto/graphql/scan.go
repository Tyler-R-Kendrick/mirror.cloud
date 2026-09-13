package graphql

// A GraphQL document is scanned, never substring-matched.
//
// The reason is recorded in this repository's own history. A router that asked
// "does the query contain `project`" answered a service lookup with the project
// handler, because Railway's schema declares both `project` and `projectId` and
// `{ service(id:"s") { id projectId } }` contains the shorter name. The same
// question misreads an operation name (`query GetService($projectId: String!)`),
// a comment, and a string literal. Those are four ways to be wrong that a
// scanner simply does not have, and the fourth -- a caller-controlled string
// choosing the handler -- dispatched a create to a delete.
//
// What follows is therefore a real reader of the grammar's ignored tokens,
// names, strings, and values. It is not a GraphQL implementation: it answers
// which field a document selects and what arguments that field is passed, and
// nothing about the selection set, because no service in this tree projects one.

// scanner walks a document's text.
type scanner struct {
	q string
	i int
	n int
}

func newScanner(q string) *scanner { return &scanner{q: q, n: len(q)} }

// varRef is an argument whose value is supplied by the request's `variables`
// rather than written in the document. It stays distinct from a string until
// Decode resolves it, because `$name` and "name" are different arguments and
// collapsing them would let a document name a variable it never declared.
type varRef string

func isNameByte(c byte, first bool) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (!first && c >= '0' && c <= '9')
}

// skipComment runs to the end of the line. Every scan below has to know about
// comments, not just the one that skips ignored tokens: a comment is the one
// place a brace can appear that opens nothing, and a scan that reads one raw
// lets a comment's TEXT decide which operation ran.
func (s *scanner) skipComment() {
	for s.i < s.n && s.q[s.i] != '\n' {
		s.i++
	}
}

// skip consumes GraphQL's ignored tokens: whitespace, commas and comments.
func (s *scanner) skip() {
	for s.i < s.n {
		switch c := s.q[s.i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',':
			s.i++
		case c == '#':
			s.skipComment()
		default:
			return
		}
	}
}

// skipString moves past a string literal from its opening quote, so that a
// brace or a paren inside one cannot close a group early. Block strings are
// recognised here too: `"""` is three quotes, and a scanner that reads them as
// an empty string followed by one more would take the rest of the document for
// a string's contents.
func (s *scanner) skipString() {
	if s.i+2 < s.n && s.q[s.i:s.i+3] == `"""` {
		s.i += 3
		for s.i+2 < s.n && s.q[s.i:s.i+3] != `"""` {
			if s.q[s.i] == '\\' {
				s.i++
			}
			s.i++
		}
		s.i += 3
		return
	}
	s.i++
	for s.i < s.n && s.q[s.i] != '"' {
		if s.q[s.i] == '\\' {
			s.i++
		}
		s.i++
	}
	s.i++
}

// name reads a GraphQL name, or "" when the next token is not one.
func (s *scanner) name() string {
	start := s.i
	for s.i < s.n && isNameByte(s.q[s.i], s.i == start) {
		s.i++
	}
	return s.q[start:s.i]
}

// toSelectionSet advances to just past the first brace that is not inside a
// string or a variable-definition group -- which is all an operation name,
// variable definitions and directives can put in the way.
func (s *scanner) toSelectionSet() bool {
	depth := 0
	for s.i < s.n {
		switch s.q[s.i] {
		case '"':
			s.skipString()
			continue
		case '#':
			s.skipComment()
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '{':
			if depth == 0 {
				s.i++
				return true
			}
		}
		s.i++
	}
	return false
}

// skipBlock runs from just past an opening brace to just past its match.
func (s *scanner) skipBlock() bool {
	depth := 1
	for s.i < s.n {
		switch s.q[s.i] {
		case '"':
			s.skipString()
			continue
		case '#':
			s.skipComment()
		case '{':
			depth++
		case '}':
			if depth--; depth == 0 {
				s.i++
				return true
			}
		}
		s.i++
	}
	return false
}

// seekFragment finds `fragment NAME on Type {` anywhere in the document and
// leaves the scanner just inside that brace, so a root selection that is a
// spread can be followed to the field it actually selects.
//
// It scans on its own copy of the position and only commits on success,
// because a fragment named in a spread need not exist and a failed search must
// not move the caller.
func (s *scanner) seekFragment(want string) bool {
	for i := 0; i+8 <= s.n; i++ {
		if s.q[i] != 'f' || s.q[i:i+8] != "fragment" {
			continue
		}
		if i > 0 && isNameByte(s.q[i-1], false) { // part of a longer word
			continue
		}
		probe := &scanner{q: s.q, i: i + 8, n: s.n}
		if probe.i < probe.n && isNameByte(probe.q[probe.i], false) {
			continue
		}
		probe.skip()
		if probe.name() != want {
			continue
		}
		if !probe.toSelectionSet() {
			return false
		}
		s.i = probe.i
		return true
	}
	return false
}

// field is the root field a document selects.
type field struct {
	// Name is the schema's field, which for a service whose every request is
	// one POST to one path IS the operation.
	Name string
	// Args are the arguments that field is passed, with a varRef wherever the
	// document wrote `$name`.
	Args map[string]any
	// Bad marks arguments this could not read to the end.
	//
	// It exists because the alternative is a PARTIAL argument map, and a
	// partial map is worse than none: an operation whose required member is
	// present but whose nested object was cut short passes validation and then
	// acts on input the caller never sent. Refusing the whole set turns an
	// unreadable document into a refusal, which is what it is.
	Bad bool
}

// unknown is the field name returned for a document this cannot read. It is a
// name no schema declares, so it routes to no operation and the request is
// refused rather than dispatched to a guess.
const unknown = "Unknown"

// rootField answers which field a document selects and what it is passed.
func rootField(q string) field {
	s := newScanner(q)
	for {
		s.skip()
		if s.i >= s.n {
			return field{Name: unknown}
		}
		if s.q[s.i] == '{' { // query shorthand, with no operation type at all
			s.i++
			return s.selection(0)
		}
		switch s.name() {
		case "query", "mutation", "subscription":
			if !s.toSelectionSet() {
				return field{Name: unknown}
			}
			return s.selection(0)
		case "fragment":
			// A fragment definition may precede the operation it serves.
			if !s.toSelectionSet() || !s.skipBlock() {
				return field{Name: unknown}
			}
		default:
			return field{Name: unknown}
		}
	}
}

// selection reads the first field of a selection set, seeing past an alias, an
// inline fragment and a named spread.
//
// Depth-bounded: a document may define fragments that spread each other in a
// cycle, and this runs on input nobody vouched for.
func (s *scanner) selection(depth int) field {
	if depth > 8 {
		return field{Name: unknown}
	}
	s.skip()
	if s.i+2 < s.n && s.q[s.i] == '.' && s.q[s.i+1] == '.' && s.q[s.i+2] == '.' {
		s.i += 3
		s.skip()
		switch word := s.name(); word {
		case "on": // inline fragment with a type condition
			s.skip()
			s.name()
			if !s.toSelectionSet() {
				return field{Name: unknown}
			}
			return s.selection(depth + 1)
		case "": // inline fragment with no type condition
			if !s.toSelectionSet() {
				return field{Name: unknown}
			}
			return s.selection(depth + 1)
		default: // a named spread: the field lives in its definition
			if !s.seekFragment(word) {
				return field{Name: unknown}
			}
			return s.selection(depth + 1)
		}
	}
	name := s.name()
	if name == "" {
		return field{Name: unknown}
	}
	s.skip()
	// `alias: field` -- the schema's field is the one after the colon, and the
	// alias names only how the caller wants it back.
	if s.i < s.n && s.q[s.i] == ':' {
		s.i++
		s.skip()
		if name = s.name(); name == "" {
			return field{Name: unknown}
		}
		s.skip()
	}
	args, ok := s.arguments()
	return field{Name: name, Args: args, Bad: !ok}
}

// arguments reads `(name: value, ...)` when the scanner is sitting on the open
// paren, and nil otherwise -- a field with no arguments is spelled by writing
// none, not by writing an empty group.
func (s *scanner) arguments() (map[string]any, bool) {
	if s.i >= s.n || s.q[s.i] != '(' {
		return nil, true // no arguments is not a failure to read them
	}
	s.i++
	args := map[string]any{}
	for {
		s.skip()
		if s.i >= s.n {
			return nil, false // ran out before the group closed
		}
		if s.q[s.i] == ')' {
			s.i++
			return args, true
		}
		name := s.name()
		if name == "" { // not a name where one must be
			return nil, false
		}
		s.skip()
		if s.i < s.n && s.q[s.i] == ':' {
			s.i++
		}
		s.skip()
		v, ok := s.value(0)
		if !ok {
			return nil, false
		}
		args[name] = v
	}
}

// value reads one argument value: a variable reference, a string, a number, a
// boolean, null, an enum, a list or an input object.
//
// An enum arrives as its name in a string, which is what it is on the wire in
// every other protocol here, and what a receiver's enum shape declares.
func (s *scanner) value(depth int) (any, bool) {
	// A list of lists of lists is legal and unbounded, and this reads a body
	// nobody vouched for, so the recursion is capped. The cap is far above any
	// nesting a schema declares -- an input object five deep is already
	// unusual -- so reaching it means the document is not a request anyone
	// meant to send, and refusing it is the answer.
	if depth > 64 {
		return nil, false
	}
	s.skip()
	if s.i >= s.n {
		return nil, false
	}
	switch c := s.q[s.i]; {
	case c == '$':
		s.i++
		name := s.name()
		if name == "" {
			return nil, false
		}
		return varRef(name), true
	case c == '"':
		return s.stringValue()
	case c == '[':
		s.i++
		list := []any{}
		for {
			s.skip()
			if s.i >= s.n {
				return nil, false
			}
			if s.q[s.i] == ']' {
				s.i++
				return list, true
			}
			v, ok := s.value(depth + 1)
			if !ok {
				return nil, false
			}
			list = append(list, v)
		}
	case c == '{':
		s.i++
		obj := map[string]any{}
		for {
			s.skip()
			if s.i >= s.n {
				return nil, false
			}
			if s.q[s.i] == '}' {
				s.i++
				return obj, true
			}
			name := s.name()
			if name == "" {
				return nil, false
			}
			s.skip()
			if s.i < s.n && s.q[s.i] == ':' {
				s.i++
			}
			v, ok := s.value(depth + 1)
			if !ok {
				return nil, false
			}
			obj[name] = v
		}
	case c == '-' || (c >= '0' && c <= '9'):
		return s.numberValue()
	default:
		switch word := s.name(); word {
		case "":
			return nil, false
		case "true":
			return true, true
		case "false":
			return false, true
		case "null":
			return nil, true
		default:
			return word, true // an enum value
		}
	}
}
