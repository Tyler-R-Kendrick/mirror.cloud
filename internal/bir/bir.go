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

	// Shadow, when set, says this bundle is proven but not yet serving: the
	// hand-written pack still answers requests, and the equivalence gate still
	// replays this bundle against the pack's recording on every run.
	//
	// It exists for the case where a bundle covers a service's semantics but
	// not yet every operation of its surface -- deleting the pack then would
	// lose operations, and registering both would mean two descriptions of one
	// service. The value is the reason, not a boolean, because a shadow bundle
	// with no stated gap is how a half-migration becomes permanent.
	Shadow string `yaml:"shadow,omitempty"`

	// MissingInput names the error table entry the engine answers with when a
	// required input member is absent. The model says which members are
	// required; it does not say what a service calls their absence, and
	// services disagree -- SQS answers MissingParameter where others answer
	// ValidationException or InvalidParameterException. Left unset the engine
	// answers ValidationException, which is a default rather than a fact, so a
	// bundle that knows better says so here.
	MissingInput string `yaml:"missing_input_error,omitempty"`

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

	ID  Identity `yaml:"id,omitempty"`
	ARN string   `yaml:"arn,omitempty"`
	// Record is the stored shape. A value is an expression, or one of the two
	// forms that must not be expressions because they draw on state an
	// expression may not touch: { generate: {...} } for a deterministic
	// identifier and { counter: "<name>" } for a monotonic sequence.
	Record map[string]any `yaml:"record,omitempty"`
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
	Batch   *BatchSpec        `yaml:"batch,omitempty"`
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
	// Reads are companion records loaded once per candidate, keyed off the
	// candidate itself, and bound for the filter exactly as an operation's
	// reads are bound for its requires: each binding x also binds x_found.
	//
	// This exists because a listed record is not always the whole record. A
	// service that splits an entity across collections -- SQS keeps a queue's
	// creation attributes on the queue and its later SetQueueAttributes
	// attributes beside it -- can be listed on a property that lives in the
	// companion, and ListDeadLetterSourceQueues does exactly that: it selects
	// queues whose redrive policy names a given dead-letter queue, and that
	// policy may sit in either record.
	//
	// The alternative was to let the filter reach into the store, which would
	// end CEL's purity for every expression in every bundle to serve one
	// operation. Declaring the join keeps the read where the engine can see it
	// and the expression pure. Each read needs an explicit key, since the
	// candidate rather than the request decides what to load.
	Reads map[string]Read `yaml:"reads,omitempty"`
	// Let are per-candidate bindings, evaluated after the joins and visible to
	// the filter, exactly as an operation's lets sit between its reads and its
	// requires. Without them a filter that has to merge two records and pull a
	// field out of a JSON-valued attribute repeats that whole chain at every
	// mention, which is how a predicate stops being readable.
	Let map[string]string `yaml:"let,omitempty"`
}

// Effect is one store mutation. The vocabulary is closed on purpose: a new
// kind is an engine change with a test, so semantic pressure surfaces as a
// reviewed pull request instead of per-service drift.
type Effect struct {
	Create    *WriteEffect    `yaml:"create,omitempty"`
	Put       *WriteEffect    `yaml:"put,omitempty"`
	Patch     *WriteEffect    `yaml:"patch,omitempty"`
	Delete    *DeleteEffect   `yaml:"delete,omitempty"`
	Counter   *CounterSpec    `yaml:"counter,omitempty"`
	Dedup     *DedupEffect    `yaml:"dedup,omitempty"`
	SendEvent *SendEvent      `yaml:"send_event,omitempty"`
	Emit      *EmitEffect     `yaml:"emit,omitempty"`
	Generate  *GenerateEffect `yaml:"generate,omitempty"`
	Primitive *PrimEffect     `yaml:"primitive,omitempty"`
}

// WriteEffect creates or updates a resource record.
type WriteEffect struct {
	Resource string         `yaml:"resource"`
	Key      string         `yaml:"key,omitempty"`
	Record   map[string]any `yaml:"record,omitempty"`
	When     string         `yaml:"when,omitempty"`
	// State is the lifecycle state a created record starts in, when that is
	// not the chart's initial state. An SQS message sent with a delay is born
	// invisible and becomes visible when its deadline passes.
	State string `yaml:"state,omitempty"`
	// Deadline arms a timer on the record as it is written, which is what
	// makes that delay a deadline rather than a background job.
	Deadline *WriteDeadline `yaml:"deadline,omitempty"`
}

// WriteDeadline arms a named timer on a record at write time.
type WriteDeadline struct {
	Name  string `yaml:"name"`
	After string `yaml:"after"`
	When  string `yaml:"when,omitempty"`
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

// GenerateEffect draws a fresh value from the deterministic Rand and binds it
// under fx for the operation's output to name.
//
// Expressions are pure and may not reach randomness, and record generators
// only exist where a record is being written -- so an operation that answers
// with a freshly minted value it does not store (a CLI token, a session
// identifier, a presigned nonce) had no way to say so. It could store the
// value instead, and the wire could not tell, which is exactly why that would
// be the wrong shape: state nobody reads, written to work around a gap.
type GenerateEffect struct {
	// Bind is the name under fx the value appears as.
	Bind     string `yaml:"bind"`
	Generate `yaml:",inline"`
	When     string `yaml:"when,omitempty"`
}

// PrimEffect invokes a stateful engine primitive.
type PrimEffect struct {
	Use  string            `yaml:"use"`
	Args map[string]string `yaml:"args,omitempty"`
	Bind string            `yaml:"bind,omitempty"`
}

// BatchSpec delegates an operation to a sibling, once per input entry.
//
// AWS batch operations are uniformly shaped: a list of entries each carrying a
// caller-chosen Id, answered by a Successful list and a Failed list whose rows
// carry that Id plus the error. Expressing that shape once means SendMessageBatch
// is four lines of data rather than a second copy of SendMessage that can drift
// from it -- which is what the hand-written packs had, and what made a fix to one
// silently not a fix to the other.
type BatchSpec struct {
	// Of is the sibling operation each entry is delegated to.
	Of string `yaml:"of"`
	// Entries is the input member holding the entry list.
	Entries string `yaml:"entries"`
	// ID is the member of each entry carrying the caller's correlation id.
	ID string `yaml:"id,omitempty"`
	// Carry are input members copied onto every delegated request, such as the
	// queue the whole batch addresses.
	Carry []string `yaml:"carry,omitempty"`
	// Successful and Failed name the output members the two result lists go in.
	Successful string `yaml:"successful,omitempty"`
	Failed     string `yaml:"failed,omitempty"`
	// Result names the members of each delegated answer to copy into its
	// Successful row, alongside the id.
	Result []string `yaml:"result,omitempty"`
}
