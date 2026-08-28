// Package bir defines the Behavior IR: service behavior expressed as
// versioned, provenance-tagged data rather than hand-written Go.
//
// A B-IR bundle plus the generic engine is what a behavior pack used to be.
// The schema and its rationale are in docs/BEHAVIOR_IR.md; this package is the
// normative Go shape of it, plus the loader and validator.
//
// Two invariants make the rest of the design work:
//
//   - B-IR never redefines wire shapes. Every output member, error and
//     pagination token must resolve against the generated model.Service, so a
//     bundle cannot describe a response the protocol cannot serialize.
//   - Expressions are pure. CEL sees a request, resolved reads, and an
//     injected clock reading — never the store, randomness or I/O. Reads are
//     declared so the engine can resolve them before evaluation, which keeps
//     every rule statically analyzable.
package bir

// Provenance records where a behavioral cell came from. Ordered weakest to
// strongest; the grade a service advertises is the weakest cell it serves.
type Provenance string

const (
	// ProvAuthored is the honest floor: a human or model wrote it down.
	ProvAuthored Provenance = "authored"
	// ProvDeclared comes from a vendored provider specification.
	ProvDeclared Provenance = "declared"
	// ProvRecorded was seen in captured real traffic.
	ProvRecorded Provenance = "observed/recorded"
	// ProvProbed was established by a maintainer-side probe of the real cloud.
	ProvProbed Provenance = "observed/probed"
	// ProvVerified is agreed by both sides of a contract.
	ProvVerified Provenance = "verified"
)

// Rank orders provenance for the grade ratchet. Higher is stronger.
func (p Provenance) Rank() int {
	switch p {
	case ProvVerified:
		return 4
	case ProvProbed:
		return 3
	case ProvRecorded:
		return 2
	case ProvDeclared:
		return 1
	default:
		return 0
	}
}

// Valid reports whether p is a known provenance value.
func (p Provenance) Valid() bool {
	switch p {
	case ProvAuthored, ProvDeclared, ProvRecorded, ProvProbed, ProvVerified:
		return true
	}
	return false
}

// Service is one service's behavior, loaded from behavior/<provider>/<id>/.
type Service struct {
	Schema     string               `yaml:"schema"`
	ServiceID  string               `yaml:"service"`
	Provenance Provenance           `yaml:"provenance,omitempty"`
	Primitives map[string]PrimRef   `yaml:"primitives,omitempty"`
	Resources  map[string]Resource  `yaml:"resources,omitempty"`
	Errors     map[string]ErrorDef  `yaml:"errors,omitempty"`
	Limits     map[string]Limit     `yaml:"limits,omitempty"`
	Quirks     []Quirk              `yaml:"quirks,omitempty"`
	Operations map[string]Operation `yaml:"operations,omitempty"`

	// Compiled holds programs prepared at load time. Nil until Load runs.
	Compiled *Compiled `yaml:"-"`
}

// PrimRef names a versioned engine primitive. Primitives are the escape hatch
// for genuinely algorithmic semantics (expression evaluation, checksum rules,
// policy evaluation); they are budgeted so they cannot become the main path.
type PrimRef struct {
	Name    string `yaml:"name"`
	Version int    `yaml:"version"`
}

// Resource is a stored entity type: how it is keyed, what a record holds, and
// optionally how its lifecycle moves.
type Resource struct {
	// Collection is the store collection name. It may interpolate a parent
	// resource ID, e.g. "msgs:{queue.id}".
	Collection string `yaml:"collection"`
	// Parent names the resource this one is scoped under, if any.
	Parent string `yaml:"parent,omitempty"`
	// Singleton, when set, is the fixed key for a one-per-scope resource.
	Singleton string `yaml:"singleton,omitempty"`
	// Key is the record member used as the store key, when it is not the ID.
	Key string `yaml:"key,omitempty"`

	ID     Identity          `yaml:"id,omitempty"`
	ARN    string            `yaml:"arn,omitempty"`
	Record map[string]string `yaml:"record,omitempty"`
	// Views are cached CEL expressions over a loaded record.
	Views      map[string]string `yaml:"views,omitempty"`
	Statechart *Statechart       `yaml:"statechart,omitempty"`
}

// Identity says how a resource's ID is produced and how callers name it.
type Identity struct {
	// Generate produces a fresh ID from the deterministic Rand.
	Generate *Generate `yaml:"generate,omitempty"`
	// InputMembers are request members that carry the ID on reads and deletes,
	// tried in order.
	InputMembers []string `yaml:"input_members,omitempty"`
	// Derive is a CEL expression producing the ID when neither of the above
	// applies (for example, extracting a name from a queue URL).
	Derive string `yaml:"derive,omitempty"`
}

// Generate describes deterministic ID generation. Randomness is an effect, not
// an expression, so ID shape stays declarative and reproducible from a seed.
type Generate struct {
	// Kind is "hex", "uuid" or "int".
	Kind  string `yaml:"kind"`
	Bytes int    `yaml:"bytes,omitempty"`
}

// Statechart is an SCXML-class lifecycle. Timers compile to stored deadlines
// evaluated lazily at observation points on the owned clock, so behavior stays
// deterministic under a controllable clock and needs no background goroutines.
type Statechart struct {
	Initial string           `yaml:"initial"`
	States  map[string]State `yaml:"states"`
}

// State is one lifecycle state.
type State struct {
	Final  bool                    `yaml:"final,omitempty"`
	On     map[string][]Transition `yaml:"on,omitempty"`
	Timers []Timer                 `yaml:"timers,omitempty"`
}

// Transition moves a record between states when an event fires and its guard
// holds. Transitions are evaluated in order; the first satisfied guard wins.
type Transition struct {
	Guard   string   `yaml:"guard,omitempty"`
	Target  string   `yaml:"target"`
	Actions []Action `yaml:"actions,omitempty"`
}

// Timer fires a transition when a stored deadline has passed.
type Timer struct {
	Deadline string `yaml:"deadline"`
	Target   string `yaml:"target"`
}

// Action is one step of a transition. Exactly one field is set.
type Action struct {
	// Set assigns record members from CEL expressions.
	Set map[string]string `yaml:"set,omitempty"`
	// Deadline arms a named timer.
	Deadline *DeadlineAction `yaml:"deadline,omitempty"`
	// Move re-parents a record, optionally with fresh members.
	Move *MoveAction `yaml:"move,omitempty"`
}

// DeadlineAction arms a named deadline relative to the current clock reading.
type DeadlineAction struct {
	Name  string `yaml:"name"`
	After string `yaml:"after"`
}

// MoveAction re-parents a record into another collection.
type MoveAction struct {
	To    map[string]string `yaml:"to"`
	Set   map[string]any    `yaml:"set,omitempty"`
	State string            `yaml:"state,omitempty"`
}

// ErrorDef is one row of the service's error table. Ordering of require rules
// is what makes error precedence explicit — the part no specification states
// and every SDK retry policy depends on.
type ErrorDef struct {
	Code       string     `yaml:"code"`
	HTTP       int        `yaml:"http"`
	Fault      string     `yaml:"fault"`
	Message    string     `yaml:"message,omitempty"`
	Provenance Provenance `yaml:"provenance,omitempty"`
}

// Limit is a documented bound, carrying where the value came from.
type Limit struct {
	Value      any        `yaml:"value"`
	Provenance Provenance `yaml:"provenance,omitempty"`
	Note       string     `yaml:"note,omitempty"`
}

// Quirk records a deviation from the specification that the real service
// exhibits, with a pointer to the evidence for it.
type Quirk struct {
	Operation  string     `yaml:"operation,omitempty"`
	Note       string     `yaml:"note"`
	Provenance Provenance `yaml:"provenance,omitempty"`
	Source     string     `yaml:"source,omitempty"`
}

// Operation is the rule set for one API operation. The engine evaluates it in
// a fixed order: reads, lets, requires, select, wait, effects, output.
type Operation struct {
	Reads   map[string]Read   `yaml:"reads,omitempty"`
	Let     map[string]string `yaml:"let,omitempty"`
	Require []Require         `yaml:"require,omitempty"`
	Select  *Select           `yaml:"select,omitempty"`
	Wait    *Wait             `yaml:"wait,omitempty"`
	Effects []Effect          `yaml:"effects,omitempty"`
	List    *ListSpec         `yaml:"list,omitempty"`
	Output  map[string]string `yaml:"output,omitempty"`

	Provenance Provenance `yaml:"provenance,omitempty"`
}

// Read binds a stored record for the operation. Reads are declared rather than
// expressed so the engine resolves them before any CEL runs, which is what
// keeps expressions pure. Each binding x also binds x_found.
type Read struct {
	Resource string `yaml:"resource"`
	Key      string `yaml:"key,omitempty"`
}

// Require is one precondition. Rules are evaluated in order and the first
// failure decides the error, making precedence a reviewable property.
type Require struct {
	Cond    string `yaml:"cond"`
	Error   string `yaml:"error"`
	Message string `yaml:"message,omitempty"`
}

// Select gathers records for operations that read a set, such as a queue
// receive. It is the engine's observation point: lazy timers fire here.
type Select struct {
	Binding  string `yaml:"binding"`
	Resource string `yaml:"resource"`
	State    string `yaml:"state,omitempty"`
	OrderBy  string `yaml:"order_by,omitempty"`
	Limit    string `yaml:"limit,omitempty"`
	Group    *Group `yaml:"group,omitempty"`
	Filter   string `yaml:"filter,omitempty"`
}

// Group expresses ordered-group semantics such as SQS FIFO message groups.
type Group struct {
	When              string `yaml:"when,omitempty"`
	By                string `yaml:"by"`
	ExclusiveInFlight string `yaml:"exclusive_in_flight,omitempty"`
}

// Wait is long-polling as an engine capability rather than service code: park
// on the owned clock and a bus wakeup topic, then re-observe.
type Wait struct {
	Until     string            `yaml:"until"`
	Timeout   string            `yaml:"timeout"`
	OnTimeout map[string]any    `yaml:"on_timeout,omitempty"`
	Output    map[string]string `yaml:"output,omitempty"`
}

// ListSpec is the common "list a resource with pagination" shape. Tokens and
// page caps bind from the model's pagination trait, which is how the engine
// gives pagination to list operations that were previously unbounded.
type ListSpec struct {
	Resource string `yaml:"resource"`
	// Member is the output member the items are placed in. It is validated
	// against the generated output shape, so a bundle cannot invent a member
	// name no SDK can read.
	Member   string `yaml:"member"`
	Paginate string `yaml:"paginate,omitempty"`
	// Key narrows the list to a single record when it evaluates to a
	// non-empty string, and lists everything when it is empty. This is the
	// "describe one or describe all" shape that most AWS Describe* operations
	// have -- DescribeClusters returns one cluster given ClusterName and every
	// cluster without it -- and it is in the engine because it appeared in
	// well over a hundred hand-written packs as the same eight lines.
	//
	// A named record that does not exist yields an empty list, not a fault.
	// Operations that must fault use reads plus require instead; the
	// difference is a per-service decision and is stated in the bundle.
	Key string `yaml:"key,omitempty"`
	// Filter is a predicate over each candidate record, which is bound as
	// `item`. Records that do not satisfy it are omitted.
	Filter string `yaml:"filter,omitempty"`
}

// Effect is one store mutation. The vocabulary is closed on purpose: a new
// kind is an engine change with a test, so semantic pressure surfaces as a
// reviewed pull request instead of per-service drift.
type Effect struct {
	Create    *WriteEffect  `yaml:"create,omitempty"`
	Put       *WriteEffect  `yaml:"put,omitempty"`
	Patch     *WriteEffect  `yaml:"patch,omitempty"`
	Delete    *DeleteEffect `yaml:"delete,omitempty"`
	Counter   *CounterSpec  `yaml:"counter,omitempty"`
	Dedup     *DedupEffect  `yaml:"dedup,omitempty"`
	SendEvent *SendEvent    `yaml:"send_event,omitempty"`
	Emit      *EmitEffect   `yaml:"emit,omitempty"`
	Primitive *PrimEffect   `yaml:"primitive,omitempty"`
}

// WriteEffect creates or updates a resource record.
type WriteEffect struct {
	Resource string         `yaml:"resource"`
	Key      string         `yaml:"key,omitempty"`
	Record   map[string]any `yaml:"record,omitempty"`
	When     string         `yaml:"when,omitempty"`
}

// DeleteEffect removes a record.
type DeleteEffect struct {
	Resource string `yaml:"resource"`
	Key      string `yaml:"key,omitempty"`
	When     string `yaml:"when,omitempty"`
	Missing  string `yaml:"missing,omitempty"`
}

// CounterSpec increments a named monotonic counter, for sequence numbers.
type CounterSpec struct {
	Name string `yaml:"name"`
}

// DedupEffect is a TTL'd idempotency table, as used by FIFO deduplication.
// On a hit it short-circuits to the recorded output.
type DedupEffect struct {
	Table  string            `yaml:"table"`
	Key    string            `yaml:"key"`
	TTL    string            `yaml:"ttl"`
	When   string            `yaml:"when,omitempty"`
	OnHit  map[string]any    `yaml:"on_hit,omitempty"`
	Record map[string]string `yaml:"record,omitempty"`
}

// SendEvent drives a statechart transition on one record or a selected set.
type SendEvent struct {
	Resource string            `yaml:"resource,omitempty"`
	Key      string            `yaml:"key,omitempty"`
	ForEach  string            `yaml:"foreach,omitempty"`
	Event    string            `yaml:"event"`
	Context  map[string]string `yaml:"context,omitempty"`
	Missing  string            `yaml:"missing,omitempty"`
}

// EmitEffect publishes to another service through the routing table, so
// cross-service delivery is data instead of one service reaching into
// another's private records.
type EmitEffect struct {
	Target  string            `yaml:"target"`
	Payload map[string]string `yaml:"payload,omitempty"`
	When    string            `yaml:"when,omitempty"`
}

// PrimEffect invokes a stateful engine primitive.
type PrimEffect struct {
	Use  string            `yaml:"use"`
	Args map[string]string `yaml:"args,omitempty"`
	Bind string            `yaml:"bind,omitempty"`
}
