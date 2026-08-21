# Master Instruction Prompt — build `mirror.cloud`

> **Read this document as your complete and only specification.** It is deliberately self-contained. Do not seek clarification from any conversation, prior message, external document, or person. Every fact, interface, constraint, and acceptance criterion you need is written here. Where this document says "verify at pin time," verify it from the vendored source you fetched — not from memory.

---

## 0. Mission

Build **mirror.cloud**: a tool that **generates cloud API emulators from the cloud providers' own published specifications**, and serves them from a single static Go binary with zero authentication, zero telemetry, and zero friction.

It is not "another cloud emulator." It is the machine that makes them. The distinction is load-bearing and shows up in every design decision below: the protocol layer is **generated for every service in the vendored spec set**; behavior is **layered selectively** on top of it; and the fidelity you are getting is **always declared, never implied**.

Three properties define success:

1. **Generated, not hand-written.** Adding the 200th service costs ~nothing at the protocol layer.
2. **Per-product composability.** `mirror up s3` boots one product, one process, one port, sub-second.
3. **Three honest tiers.** `mock` | `emulate` | `proxy`, declared per product, surfaced on every response, never silently substituted.

### 0.1 Repository facts

- Module path: `github.com/tyler-r-kendrick/mirror.cloud`
- Binary name: `mirror`
- Language: **Go 1.22+**, modules only. No GOPATH, no vendored toolchain hacks.
- License: **Apache-2.0** (already present at repo root — do not replace it).
- Existing content: `LICENSE`, `README.md`, `docs/DIRECTION.md`, `docs/MASTER_PROMPT.md` (this file). Everything else you create.
- Default HTTP endpoint: `http://127.0.0.1:4566` (chosen for drop-in compatibility with existing tooling conventions).
- Dummy credentials `test`/`test` must work. So must any other credentials. Signatures are parsed, never verified.

### 0.2 Non-goals for v1 — do not build these

- Full coverage of every cloud service at `emulate` tier. Generated `mock` tier covers the long tail; that is the design, not a shortfall.
- **AWS Lambda or any compute-execution service.** Cut deliberately. Ship the `ComputeProvider` extension interface (§3.9) and return a distinguishable `NotImplemented`. Do not ship a partial Lambda.
- A real IAM policy evaluation engine. Allow-all, documented, behind the `Authorizer` seam (§3.9).
- Broker protocol emulation (Kafka/AMQP/MQTT wire protocols). Orchestrate real brokers instead if the need arises; it does not arise in v1.
- User-authored contract ingestion (Pact / OpenAPI / HAR / Postman). The receiver interface must accommodate it (§3.2) and the model must be provider-neutral, but **no such receiver ships in v1**.
- Any auth token, license check, telemetry, phone-home, usage counter, or gated capability. Ever. This is a governance covenant, not a preference.
- Any dependency on, or code from, LocalStack, Floci, or any other emulator's source or container images.

---

## 1. Absolute constraints

1. **No network calls to any real cloud at runtime**, except in explicitly-enabled `proxy` tier, which is **off by default** and requires an explicit flag plus an explicit endpoint. Tests must never require real cloud credentials or real cloud network access.
2. **All provider specifications are vendored and version-pinned** under `specs/` with a lockfile recording source URL, git ref, path, and SHA-256 per file. Fetching happens in an explicit, reproducible `make specs-sync` step, never at build or runtime.
3. **Generated code is committed to the repository**, and a test asserts that regenerating from the pinned specs produces a byte-identical tree. Non-reproducible generation is a build failure.
4. **No stub implementations below the declared fidelity table (§4).** If an operation is listed as `emulate` tier, it works. If you cannot make it work, move it to `mock` tier in the table and say so in the support matrix. Silent partial implementations are the single worst failure mode of this project.
5. **Fail loud, never silently fall back.** A client that asked for the error case and silently received the success case will write a passing test asserting the wrong behavior. Unsupported operation → distinguishable error. Unsupported parameter → explicit error, never ignored. Mock-tier response → labeled on the wire.
6. **Determinism is a hard requirement.** Same seed + same request sequence ⇒ byte-identical responses, modulo fields documented as nondeterministic. No `time.Now()`, no `math/rand`, no `uuid.New()` anywhere outside the `Clock` and `Rand` implementations. Enforce this with a lint check in CI.
7. **Cold start under 2 seconds** on ordinary developer hardware with persistence cold, asserted by an automated test.
8. **Licenses:** only permissive dependencies (MIT / Apache-2.0 / BSD / ISC). No copyleft in the module graph. Add a CI check. This is an adoption decision, not a philosophical one — a copyleft license in this category measurably suppresses uptake regardless of whether the obligation actually binds a test-dependency user.
9. **Never imply vendor affiliation.** Do not use any cloud vendor's trademarks in binary names, package names, image names, or the project name, and never suggest endorsement. Describing compatibility ("works with the AWS SDK", an image named `mirror-s3`) is fine; naming a product after someone else's mark is not.
10. **Idiomatic Go.** `gofmt`, `go vet`, `staticcheck` clean. Package-level doc comments on every package; doc comments on every exported identifier. No secrets or hardcoded credentials beyond documented dummy values.

---

## 2. Architecture

```
provider specs (vendored, pinned)
        │
        ▼
   receivers ──► normalization ──► fusion ──► canonical behavioral model
                                                        │
                          ┌─────────────────────────────┼──────────────────────────┐
                          ▼                             ▼                          ▼
                 generated protocol layer      generated mock-tier          support matrix,
                 (routing, codecs,             synthesizers                 API-surface diffs
                  validation, errors)
                          │
                          ▼
   edge (demux + identity + namespacing) ──► behavior packs (emulate tier)
                          │                            │
                          │                            └──► store / blobs / bus / clock / rand
                          └──► proxy · record · replay ──► cassettes ──► drift reports
```

**The owned middle is the canonical behavioral model.** It is provider-neutral: it must be able to describe an AWS Smithy service and a Google Discovery service without either one's vocabulary leaking into it. This is enforced by the cross-cloud proof in §4.9 — if GCS requires a special case in the model, the model is wrong.

---

## 3. Frozen interfaces

**These interfaces are frozen by this document.** Every swarm codes against exactly what is written here, from its first line, without waiting for another swarm to publish them. Swarm S0 materializes them verbatim. If a swarm believes a signature is wrong, it implements it as written anyway and records the objection in `docs/INTERFACE_NOTES.md`; changing a frozen interface mid-flight breaks every other swarm and is forbidden.

Where a signature says `context.Context`, it is always the first parameter. Where a method can fail, it returns `error`. Nothing panics across a package boundary.

### 3.1 Canonical model — `internal/model`

```go
package model

// Provider identifies a cloud vendor namespace. Provider-neutral by construction:
// nothing in this package may branch on a specific Provider value.
type Provider string

const (
	ProviderAWS   Provider = "aws"
	ProviderGCP   Provider = "gcp"
	ProviderAzure Provider = "azure"
)

// Protocol is the wire protocol a service speaks.
type Protocol string

const (
	ProtoAWSJSON10  Protocol = "awsJson1_0"
	ProtoAWSJSON11  Protocol = "awsJson1_1"
	ProtoRESTJSON1  Protocol = "restJson1"
	ProtoRESTXML    Protocol = "restXml"
	ProtoAWSQuery   Protocol = "awsQuery"
	ProtoEC2Query   Protocol = "ec2Query"
	ProtoGCPRESTSON Protocol = "gcpRestJson"
)

// Confidence records the evidentiary class of a model cell. Higher-precedence
// evidence narrows; lower-precedence evidence completes.
//   verified > observed > declared
type Confidence string

const (
	ConfDeclared Confidence = "declared" // from a published specification
	ConfObserved Confidence = "observed" // from recorded real traffic
	ConfVerified Confidence = "verified" // from a mutually-agreed contract
)

// Tier is the fidelity of the behavior serving a service. Always explicit.
type Tier string

const (
	TierMock    Tier = "mock"
	TierEmulate Tier = "emulate"
	TierProxy   Tier = "proxy"
)

// SourceRef is provenance for any model element. Every element traces to one.
type SourceRef struct {
	Repo   string // e.g. "github.com/aws/api-models-aws"
	Ref    string // pinned git ref
	Path   string // path within the source
	SHA256 string // content hash of the source file
}

// Bundle is the complete canonical model: the sole interchange between
// receivers and every generator, runtime, and exporter.
type Bundle struct {
	SchemaVersion string
	Provider      Provider
	Services      []Service
	Sources       []SourceRef
}

type Service struct {
	ID             string   // stable, lowercase: "aws.s3", "gcp.storage"
	Namespace      string   // spec-native namespace, e.g. "com.amazonaws.s3"
	Protocol       Protocol
	EndpointPrefix string   // "s3", "dynamodb", "sqs"
	TargetPrefix   string   // X-Amz-Target prefix; "" when not applicable
	QueryVersion   string   // awsQuery/ec2Query Version parameter; "" otherwise
	XMLNamespace   string   // restXml/awsQuery response xmlns; "" otherwise
	Aliases        []string // alternate endpoint prefixes / host matches
	Operations     []Operation
	Shapes         map[string]Shape // shape ID -> shape
	Source         SourceRef
}

type Operation struct {
	Name        string
	HTTP        HTTPBinding
	Target      string      // full X-Amz-Target value, when applicable
	QueryAction string      // Action= value for query protocols
	Input       string      // shape ID
	Output      string      // shape ID
	Errors      []string    // shape IDs
	Idempotent  bool
	Readonly    bool
	Pagination  *Pagination
	Confidence  Confidence
	Source      SourceRef
}

type HTTPBinding struct {
	Method string // "POST", "GET", ...
	URI    string // e.g. "/{Bucket}/{Key+}"; "/" for RPC-style protocols
	Code   int    // success status code
}

type Pagination struct {
	InputToken  string
	OutputToken string
	Items       string
	PageSize    string
}

// ShapeKind enumerates the type system. Deliberately minimal and
// provider-neutral: every receiver normalizes into exactly these.
type ShapeKind string

const (
	KindStructure ShapeKind = "structure"
	KindList      ShapeKind = "list"
	KindMap       ShapeKind = "map"
	KindUnion     ShapeKind = "union"
	KindEnum      ShapeKind = "enum"
	KindString    ShapeKind = "string"
	KindInteger   ShapeKind = "integer"
	KindLong      ShapeKind = "long"
	KindFloat     ShapeKind = "float"
	KindDouble    ShapeKind = "double"
	KindBoolean   ShapeKind = "boolean"
	KindBlob      ShapeKind = "blob"
	KindTimestamp ShapeKind = "timestamp"
	KindDocument  ShapeKind = "document"
)

type Shape struct {
	ID         string
	Kind       ShapeKind
	Members    map[string]Member // structure/union
	Member     string            // list/map value shape ID
	Key        string            // map key shape ID
	EnumValues []string
	Constraints Constraints
	Streaming  bool // blob streaming payload
	Error      *ErrorTrait
	Doc        string
}

type Member struct {
	Shape    string
	Required bool
	Binding  MemberBinding
	Default  any
	Doc      string
}

// MemberBinding carries HTTP-binding placement for REST protocols and
// serialization naming for all protocols.
type MemberBinding struct {
	Location   string // "" | "label" | "query" | "header" | "prefixHeaders" | "payload" | "statusCode" | "queryParams"
	Name       string // wire name (header name, query param, XML/JSON field)
	TimestampFormat string // "" | "date-time" | "http-date" | "epoch-seconds"
	XMLAttribute bool
	XMLFlattened bool
	XMLNamespace string
}

type Constraints struct {
	MinLength, MaxLength *int64
	MinValue, MaxValue   *float64
	Pattern              string
	UniqueItems          bool
}

type ErrorTrait struct {
	Fault      string // "client" | "server"
	HTTPStatus int
	Retryable  bool
	Code       string // wire error code
}
```

### 3.2 Receivers — `internal/receiver`

```go
package receiver

// Receiver ingests one specification format and emits canonical model
// fragments. v1 ships smithy and discovery receivers. The interface exists
// unchanged for future pact/openapi/har receivers; do not narrow it to
// cloud-provider specs.
type Receiver interface {
	// Name is a stable identifier, e.g. "smithy", "discovery".
	Name() string
	// Detect reports whether this receiver can parse the file at path.
	Detect(path string, head []byte) bool
	// Ingest parses one source file into services plus their provenance.
	Ingest(ctx context.Context, src model.SourceRef, data []byte) ([]model.Service, error)
}
```

### 3.3 Fusion — `internal/fusion`

```go
package fusion

// Fuse merges service fragments from any number of receivers into one Bundle.
// Precedence when two fragments describe the same cell:
//   verified > observed > declared.
// Equal confidence: the fragment whose SourceRef sorts first wins, and the
// conflict is recorded. Fusion never drops provenance and never silently
// resolves a conflict without recording it.
func Fuse(ctx context.Context, provider model.Provider, in [][]model.Service) (model.Bundle, []Conflict, error)

type Conflict struct {
	ServiceID string
	Path      string // dotted path to the conflicting cell
	Winner    model.SourceRef
	Losers    []model.SourceRef
	Reason    string
}
```

### 3.4 Service provider interface — `internal/spi`

```go
package spi

// Identity is the caller, derived from the request. Never authenticated.
type Identity struct {
	Account     string // 12-digit account ID
	Region      string
	AccessKeyID string
	ARN         string
	Project     string // GCP project, when applicable
}

// Request is one decoded operation invocation.
type Request struct {
	ServiceID string
	Operation string
	// Input is the decoded operation input, keyed by member name as declared
	// in the model. Values use Go-native types: string, float64, bool,
	// []byte, time.Time, []any, map[string]any.
	Input    map[string]any
	Identity Identity
	// Body is the raw streaming payload for operations whose input shape has
	// a streaming member (e.g. S3 PutObject). nil otherwise.
	Body io.ReadCloser
	// HTTP is the raw request, for the narrow cases that need it
	// (S3 addressing, presigned URLs, conditional headers). Behavior packs
	// must prefer Input and treat this as an escape hatch.
	HTTP *http.Request
}

// Response is one operation result. Exactly one of Output or Stream is set
// for success; Err is set for failure.
type Response struct {
	Output map[string]any
	Stream io.ReadCloser
	// Headers are protocol-level additions (e.g. S3 x-amz-* metadata).
	Headers http.Header
	Status  int // 0 = use the model's declared success code
}

// Handler serves operations for one service.
type Handler interface {
	ServiceID() string
	// Operations lists the operation names this handler serves. The edge
	// returns NotImplemented for any operation not listed.
	Operations() []string
	Invoke(ctx context.Context, req *Request) (*Response, error)
}

// BehaviorPack is a Handler with a declared fidelity tier.
type BehaviorPack interface {
	Handler
	Tier() model.Tier
}

// Fault is the canonical error type. Behavior packs return these; the edge
// renders them into the service's wire protocol.
type Fault struct {
	Code       string // wire error code, e.g. "NoSuchBucket"
	Message    string
	HTTPStatus int
	Fault      string         // "client" | "server"
	Fields     map[string]any // extra members on the error shape
}

func (f *Fault) Error() string

// Deps is the dependency bundle every behavior pack receives at construction.
type Deps struct {
	Store  Store
	Blobs  BlobStore
	Bus    Bus
	Clock  Clock
	Rand   Rand
	Journal Journal
	Model  *model.Bundle
}
```

### 3.5 State — `internal/spi` (continued)

```go
// Store is account+region namespaced structured state.
type Store interface {
	Scope(account, region string) Scope
	Snapshot(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
	Close() error
}

type Scope interface {
	Collection(name string) Collection
}

type Collection interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, val []byte) error
	Delete(ctx context.Context, key string) error
	// List returns entries with the given key prefix in lexicographic order,
	// starting strictly after `after`, up to limit entries. limit <= 0 means
	// no limit. The bool reports whether more entries remain.
	List(ctx context.Context, prefix, after string, limit int) ([]KV, bool, error)
	// Txn runs fn atomically. Concurrent Txns on one Collection serialize.
	Txn(ctx context.Context, fn func(Tx) error) error
}

type KV struct {
	Key   string
	Value []byte
}

type Tx interface {
	Get(key string) ([]byte, bool, error)
	Put(key string, val []byte) error
	Delete(key string) error
	List(prefix, after string, limit int) ([]KV, bool, error)
}

// BlobStore is large-payload storage, separate from Store so that object
// bodies never transit the structured-state serialization path.
type BlobStore interface {
	Put(ctx context.Context, key string, r io.Reader) (BlobInfo, error)
	Get(ctx context.Context, key string) (io.ReadSeekCloser, BlobInfo, error)
	Stat(ctx context.Context, key string) (BlobInfo, error)
	Delete(ctx context.Context, key string) error
	Snapshot(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
}

type BlobInfo struct {
	Size   int64
	MD5    string // hex
	SHA256 string // hex
}

// Bus is in-process event delivery (S3 notifications -> SQS/SNS, and future
// stream sources). Delivery is synchronous-by-default and ordered per topic
// so tests are deterministic.
type Bus interface {
	Publish(ctx context.Context, topic string, payload []byte) error
	Subscribe(topic string, fn func(ctx context.Context, payload []byte)) (cancel func())
}

// Clock is the only source of time in the process.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	// Advance moves a controllable clock forward. Real-time implementations
	// return an error.
	Advance(d time.Duration) error
	// After fires after d according to this clock.
	After(d time.Duration) <-chan time.Time
}

// Rand is the only source of randomness in the process. All methods are
// deterministic given the seed and the call sequence.
type Rand interface {
	Intn(n int) int
	Bytes(n int) []byte
	Hex(n int) string
	UUID() string
	// Derive returns a child Rand deterministically seeded from key, so a
	// response can be seeded by request hash without consuming global entropy.
	Derive(key string) Rand
}

// Journal records every request for diagnostics and drift analysis.
type Journal interface {
	Record(Entry)
	Query(f Filter) []Entry
}

type Entry struct {
	At        time.Time
	RequestID string
	ServiceID string
	Operation string
	Tier      model.Tier
	Account   string
	Region    string
	Status    int
	ErrorCode string
	Duration  time.Duration
	Note      string
}

type Filter struct {
	ServiceID string
	Operation string
	Since     time.Time
	Limit     int
}
```

### 3.6 Registry and edge — `internal/registry`

```go
package registry

// Registry maps service IDs to the pack serving them. Packs register
// themselves in package init via Register; the edge never imports a
// behavior pack directly.
type Registry interface {
	Register(factory Factory)
	// Resolve returns the pack for a service ID, honoring the enabled-service
	// set and the configured tier for that service.
	Resolve(serviceID string) (spi.BehaviorPack, bool)
	Enabled() []string
}

// Factory constructs a pack once dependencies are available.
type Factory struct {
	ServiceID string
	Tier      model.Tier
	New       func(spi.Deps) (spi.BehaviorPack, error)
}
```

### 3.7 Protocol codecs — `internal/proto`

```go
package proto

// Codec decodes an HTTP request into a decoded input map and encodes a
// Response or Fault back onto the wire, per one protocol. Generated
// dispatch tables call into these; the codecs themselves are hand-written
// (this is the "thin adapter" exception to the no-hand-written-protocol rule).
type Codec interface {
	Protocol() model.Protocol
	// Route identifies the operation from the raw request.
	Route(svc *model.Service, r *http.Request) (op *model.Operation, err error)
	Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error)
	Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error
	EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error
}
```

### 3.8 Proxy — `internal/proxy`

```go
package proxy

type Mode string

const (
	ModeOff         Mode = "off"     // default
	ModePassthrough Mode = "passthrough"
	ModeRecord      Mode = "record"
	ModeReplay      Mode = "replay"
	ModeHybrid      Mode = "hybrid" // replay if present, else record
)

// Cassette is a recorded request/response corpus. Secrets are scrubbed at
// write time, never at read time.
type Cassette interface {
	Lookup(key string) (*Interaction, bool)
	Append(i *Interaction) error
	Flush() error
}
```

### 3.9 Extension seams (must exist in v1, unimplemented behind them)

```go
package spi

// Authorizer is the seam for future policy evaluation. v1 ships AllowAll.
type Authorizer interface {
	Authorize(ctx context.Context, id Identity, serviceID, operation, resource string) error
}

// ComputeProvider is the seam for future function-execution services
// (Lambda and equivalents). v1 registers no implementation; services that
// need it return NotImplemented.
type ComputeProvider interface {
	Create(ctx context.Context, spec ComputeSpec) (string, error)
	Invoke(ctx context.Context, id string, payload []byte) ([]byte, error)
	Delete(ctx context.Context, id string) error
}
```

---

## 4. Technical reference — everything you need, stated inline

### 4.1 Specification sources (vendor these, pin these)

| Provider | Source | Contents |
|---|---|---|
| AWS | `github.com/aws/api-models-aws` | Smithy 2.0 JSON AST models for AWS services, one directory per service, versioned subdirectories |
| Google | `https://storage.googleapis.com/$discovery/rest?version=v1` | Google API Discovery document for Cloud Storage JSON API v1 |
| Google (reference) | `github.com/googleapis/googleapis` | Protobuf definitions; not required for v1's GCS scope |
| Azure | `github.com/Azure/azure-rest-api-specs` | OpenAPI/TypeSpec. **Reserved — not ingested in v1.** |

Vendor into `specs/<provider>/…`. Write `specs/mirror.lock` with, per file: source repo/URL, git ref or ETag, path, SHA-256, and ingestion date. `make specs-sync` is the only thing that writes it. **Verify the actual directory layout of the AWS model repository at pin time and write the resolved paths into the lockfile** — do not hardcode a layout from memory.

### 4.2 Smithy traits you must handle

Service-level: `aws.api#service` (carries `sdkId`, `arnNamespace`, `endpointPrefix`, `cloudFormationName`); `aws.protocols#awsJson1_0`, `#awsJson1_1`, `#restJson1`, `#restXml`, `#awsQuery`, `#ec2Query`; `aws.protocols#awsQueryError`; `smithy.api#xmlNamespace`; `smithy.api#paginated`.

Operation/member-level: `smithy.api#http` (`method`, `uri`, `code`), `#httpLabel`, `#httpQuery`, `#httpQueryParams`, `#httpHeader`, `#httpPrefixHeaders`, `#httpPayload`, `#httpResponseCode`, `#required`, `#default`, `#enumValue`, `#length`, `#range`, `#pattern`, `#uniqueItems`, `#timestampFormat`, `#streaming`, `#idempotent`, `#readonly`, `#error` (`"client"`/`"server"`), `#httpError`, `#retryable`, `#xmlName`, `#xmlAttribute`, `#xmlFlattened`, `#jsonName`, `#sparse`, `#documentation`, `#deprecated`.

Unknown traits are recorded on the shape and ignored, never fatal — provider specs add traits continuously and an unknown trait must not break `make specs-sync`.

### 4.3 Protocol dispatch rules

**awsJson1_0 / awsJson1_1** — `POST /`; `Content-Type: application/x-amz-json-1.0` (or `1.1`); `X-Amz-Target: <ServiceShapeName>.<OperationName>` where `ServiceShapeName` is the *shape name* of the service shape (e.g. `DynamoDB_20120810`, `AmazonSQS`, `AmazonSSM`, `secretsmanager`). Input and output are JSON objects using member names (or `jsonName` when present). Errors: HTTP 400 for `client` fault, 500 for `server`, body `{"__type":"<namespace>#<ErrorName>","message":"..."}`. Also emit `x-amzn-errortype`. Accept both the bare and namespaced `__type` forms on input for robustness.

**restJson1** — method and URI from the `http` trait; `{Label}` and `{Label+}` (greedy, may contain `/`) path templates; members bound to query, header, prefix-headers, or payload per their traits; unbound members form the JSON body. Errors: `x-amzn-errortype` header plus `{"message":"..."}` body, status from `httpError` or the fault default.

**restXml** — as restJson1 but XML body, honoring `xmlName`, `xmlAttribute`, `xmlFlattened`, `xmlNamespace`. Used by S3. Errors: `<Error><Code>…</Code><Message>…</Message><RequestId>…</RequestId><HostId>…</HostId></Error>` with the operation-appropriate status.

**awsQuery** — `POST /` with `application/x-www-form-urlencoded`; body contains `Action=<OperationName>&Version=<serviceVersion>` plus flattened members using the `Member.N` / `Name.N.Key` conventions. Response is XML: `<{Operation}Response xmlns="…"><{Operation}Result>…</{Operation}Result><ResponseMetadata><RequestId>…</RequestId></ResponseMetadata></{Operation}Response>`. Errors: `<ErrorResponse><Error><Type>Sender|Receiver</Type><Code>…</Code><Message>…</Message></Error><RequestId>…</RequestId></ErrorResponse>`, status 400 for Sender / 500 for Receiver.

**ec2Query** — awsQuery variant with no `…Result` wrapper and different error envelope. Implement the codec; no v1 service uses it, so it may be tested by generated conformance only.

**Critical cross-protocol note.** SQS is served by modern SDKs over **awsJson1_0** (target prefix `AmazonSQS`), but older SDKs, older tooling, and some Terraform provider paths still send **awsQuery**. **SQS must accept both wire formats and dispatch to the same behavior pack.** The edge decides by inspecting `X-Amz-Target` first, then the `Action` form field. SNS, STS, and IAM remain awsQuery.

**Service → protocol map for v1:**

| Service | Protocol | Target/Version |
|---|---|---|
| S3 | restXml | — (path + virtual-host addressing) |
| DynamoDB | awsJson1_0 | `DynamoDB_20120810` |
| SQS | awsJson1_0 **and** awsQuery | `AmazonSQS` / `Version=2012-11-05` |
| SNS | awsQuery | `Version=2010-03-31` |
| STS | awsQuery | `Version=2011-06-15` |
| IAM | awsQuery | `Version=2010-05-08` |
| SSM | awsJson1_1 | `AmazonSSM` |
| Secrets Manager | awsJson1_1 | `secretsmanager` |
| GCS | gcpRestJson | — |

Confirm each of these against the vendored spec at ingestion time; the model is the source of truth and this table is a cross-check, not an override.

### 4.4 S3 addressing

Support both, on the same port:

- **Path-style**: `POST http://127.0.0.1:4566/{bucket}/{key}`
- **Virtual-host style**: `Host: {bucket}.s3.localhost.localstack.cloud:4566`, `Host: {bucket}.s3.127.0.0.1.nip.io:4566`, or any `Host` whose leftmost labels precede a recognized `s3` label. Extract the bucket from the host, rewrite the path, and continue.

Terraform's AWS provider and several SDKs default to virtual-host style; `s3_use_path_style = true` is the documented workaround but must not be *required*. Document both, support both, and cover both in tests.

### 4.5 Identity extraction (never verification)

Parse `Authorization: AWS4-HMAC-SHA256 Credential=<AKID>/<date>/<region>/<service>/aws4_request, SignedHeaders=…, Signature=…`.

- Extract `AKID`, `region`, `service` from the credential scope. **Never validate the signature.**
- Region precedence: explicit `X-Mirror-Region` header > credential scope region > configured default (`us-east-1`).
- Account derivation, in order: `X-Mirror-Account-Id` header (12 digits) → an entry in the configured AKID→account map → the AKID itself if it is exactly 12 digits → default `000000000000`.
- Requests with no `Authorization` header (presigned URLs, anonymous S3 GETs, GCS emulator traffic) get the default account and the configured default region.
- Presigned URLs: honor `X-Amz-Algorithm`/`X-Amz-Credential`/`X-Amz-Expires` query parameters for identity and expiry semantics; do not verify the signature but **do** enforce expiry, because tests assert on it.
- Construct `Identity.ARN` as `arn:aws:iam::<account>:user/mirror-local` unless an STS-assumed session says otherwise.

### 4.6 Client configuration conventions (this is the DX surface — get it exactly right)

- AWS SDKs and CLI v2 honor `AWS_ENDPOINT_URL` and `AWS_ENDPOINT_URL_<SERVICE>` (e.g. `AWS_ENDPOINT_URL_S3`), plus `endpoint_url` in profile config. Supporting these means **zero code changes** in user applications. Make this the headline path in docs; make `awslocal`-style wrappers the secondary path.
- Google Cloud Storage clients honor `STORAGE_EMULATOR_HOST` (Go, Python, Node). Document per-language specifics for clients that require an explicit host override instead.
- `mirror env` must print a copy-pasteable block of exports for the currently-enabled services, in `sh`, `fish`, and PowerShell forms.
- Terraform AWS provider configuration must be documented and tested: `access_key`/`secret_key` set to dummies, `region`, `skip_credentials_validation`, `skip_metadata_api_check`, `skip_requesting_account_id`, `s3_use_path_style`, and per-service `endpoints { … }`.

### 4.7 Diagnostics endpoints

- `GET /_mirror/health` → `{"status":"ok","uptime_ms":…,"version":…}`
- `GET /_mirror/services` → enabled services with ID, protocol, tier, operation count, spec provenance
- `GET /_mirror/journal?service=&operation=&limit=` → recent entries
- `GET /_mirror/model/{serviceID}` → the canonical model for one service, as JSON
- `POST /_mirror/clock/advance` → advance the controllable clock (only when the controllable clock is configured)
- `POST /_mirror/snapshot` / `POST /_mirror/restore`
- `POST /_mirror/reset` → drop all state

All diagnostics live under `/_mirror/` so they can never collide with a cloud API path. Every response carries `x-mirror-request-id` and `x-mirror-fidelity`.

### 4.8 Fidelity table — v1 emulate tier

Everything listed here **works**. Anything not listed, in these or any other service, is served by the generated mock tier or returns `NotImplemented`.

**S3** — `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListBuckets`, `GetBucketLocation`, `Get/PutBucketVersioning`, `Get/PutBucketTagging`, `Get/PutBucketNotificationConfiguration`, `PutObject`, `GetObject` (incl. `Range`, `If-Match`/`If-None-Match`/`If-Modified-Since`), `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject`, `ListObjects`, `ListObjectsV2` (prefix, delimiter, common prefixes, continuation tokens, max-keys), `ListObjectVersions`, `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`, `ListMultipartUploads`, `Get/PutObjectTagging`, presigned GET/PUT. ETag semantics: hex MD5 for single-part; `"<md5-of-concatenated-part-md5s>-<partcount>"` for multipart. Versioning: enabled/suspended, version IDs, delete markers. Notifications publish to the `Bus` in S3 event JSON shape for SQS and SNS destinations.

**DynamoDB** — `CreateTable`, `DeleteTable`, `DescribeTable`, `ListTables`, `UpdateTable` (GSI create/delete; throughput accepted and ignored, documented), `PutItem`, `GetItem`, `DeleteItem`, `UpdateItem`, `BatchGetItem`, `BatchWriteItem`, `Query`, `Scan`, `TransactGetItems`, `TransactWriteItems`, `TagResource`/`UntagResource`/`ListTagsOfResource`. Expression support, exhaustively: `KeyConditionExpression` (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`); `FilterExpression` and `ConditionExpression` (comparators, `AND`/`OR`/`NOT`, parentheses, `attribute_exists`, `attribute_not_exists`, `attribute_type`, `begins_with`, `contains`, `size`, `IN`, `BETWEEN`); `ProjectionExpression` (top-level, nested document paths, list indices); `UpdateExpression` (`SET`, `REMOVE`, `ADD`, `DELETE`, with `if_not_exists`, `list_append`, arithmetic `+`/`-`). `ExpressionAttributeNames`/`Values` throughout. LSIs and GSIs with `ALL`/`KEYS_ONLY`/`INCLUDE` projections. `ExclusiveStartKey`/`LastEvaluatedKey` pagination with a 1 MB page cap. `ReturnValues` (`NONE`, `ALL_OLD`, `UPDATED_OLD`, `ALL_NEW`, `UPDATED_NEW`). `ConditionalCheckFailedException` with `Item` when requested. `ConsistentRead` accepted as a no-op, documented. **Streams are out of scope** and return `NotImplemented`.

**SQS** — `CreateQueue`, `DeleteQueue`, `GetQueueUrl`, `ListQueues`, `Get/SetQueueAttributes`, `SendMessage`, `SendMessageBatch`, `ReceiveMessage` (long polling via `WaitTimeSeconds`, `VisibilityTimeout`, `MaxNumberOfMessages`, `MessageAttributeNames`, `AttributeNames`), `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`, `PurgeQueue`, `TagQueue`/`UntagQueue`/`ListQueueTags`. FIFO queues: `MessageGroupId` ordering, `MessageDeduplicationId`, `ContentBasedDeduplication`, 5-minute dedup window on the controllable clock. DLQ: `RedrivePolicy` with `maxReceiveCount`. Both wire protocols per §4.3.

**SNS** — `CreateTopic`, `DeleteTopic`, `ListTopics`, `Get/SetTopicAttributes`, `Subscribe`, `ConfirmSubscription`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `Publish`, `PublishBatch`, `TagResource`/`UntagResource`. Delivery: `sqs` in-process via `Bus` (honoring `RawMessageDelivery`), `http`/`https` via real outbound POST with the SNS envelope and the subscription-confirmation handshake. `FilterPolicy` with exact-string, prefix, numeric, and `anything-but` matching. `lambda` protocol returns `NotImplemented`.

**STS** — `GetCallerIdentity`, `AssumeRole`, `GetSessionToken`, `GetFederationToken`. Credentials derived deterministically from role ARN + session name so tests can assert on them. `AssumeRole` yields an `Identity` whose ARN is `arn:aws:sts::<account>:assumed-role/<RoleName>/<SessionName>`.

**IAM** — `CreateRole`, `GetRole`, `UpdateRole`, `DeleteRole`, `ListRoles`, `PutRolePolicy`, `GetRolePolicy`, `DeleteRolePolicy`, `ListRolePolicies`, `AttachRolePolicy`, `DetachRolePolicy`, `ListAttachedRolePolicies`, `CreatePolicy`, `GetPolicy`, `DeletePolicy`, `ListPolicies`, `CreateUser`, `GetUser`, `DeleteUser`, `ListUsers`, `CreateAccessKey`, `TagRole`/`UntagRole`. Policy documents are stored and returned verbatim. **They are never evaluated** — `AllowAll` authorizer, stated in the support matrix and the README.

**SSM Parameter Store** — `PutParameter`, `GetParameter`, `GetParameters`, `GetParametersByPath` (recursive, with pagination), `DeleteParameter`, `DeleteParameters`, `DescribeParameters`, `LabelParameterVersion`, `GetParameterHistory`, `AddTagsToResource`/`RemoveTagsFromResource`/`ListTagsForResource`. `SecureString` is stored with a reversible local encoding and is **documented as not real encryption**.

**Secrets Manager** — `CreateSecret`, `GetSecretValue`, `PutSecretValue`, `UpdateSecret`, `DeleteSecret` (recovery window and `ForceDeleteWithoutRecovery`), `RestoreSecret`, `ListSecrets`, `DescribeSecret`, `ListSecretVersionIds`, `GetRandomPassword`, `TagResource`/`UntagResource`. Version staging labels `AWSCURRENT`/`AWSPREVIOUS`.

**Google Cloud Storage (cross-cloud proof)** — buckets: `insert`, `get`, `list`, `delete`, `patch`. Objects: `insert` (`uploadType=media`, `multipart`, and `resumable` with session URIs and chunked `PUT`s), `get` (metadata and `alt=media`, incl. `Range`), `list` (`prefix`, `delimiter`, `pageToken`), `delete`, `copy`, `rewrite`, `compose`, `patch`. Generation and metageneration numbers with `ifGenerationMatch` preconditions. Batch endpoint is out of scope.

### 4.9 The cross-cloud constraint

GCS exists in v1 for exactly one reason: **to falsify the claim that the model is provider-neutral.** If serving GCS requires an AWS-shaped special case anywhere in `internal/model`, `internal/fusion`, `internal/registry`, `internal/spi`, or the edge, the abstraction is wrong and must be fixed rather than special-cased. A CI check must assert that `internal/model` contains no string literal naming a specific service.

### 4.10 Mock-tier synthesis rules

For any service in the vendored spec set without an `emulate` pack:

1. **Validate the input** against the model shape — required members, enums, length, range, pattern. Violations produce the protocol-correct validation error, not a synthesized success.
2. **Synthesize a schema-valid output**, deterministically seeded by `Rand.Derive(hash(serviceID, operation, canonicalized input))`, so the same request always yields the same response.
3. **CRUD-by-convention** where operation naming permits: `Create*`/`Put*` stores keyed by the input's identifier member; `Get*`/`Describe*` reads it; `Delete*` removes it; `List*` enumerates. Where inference fails, fall back to pure synthesis.
4. **Label it.** `x-mirror-fidelity: mock` on every response, a journal entry, and a one-line warning on first use per operation.
5. **`--strict` refuses** to serve mock tier at all, returning `NotImplemented` instead. CI for the emulate-tier suites runs in `--strict` so a behavior-pack regression can never be masked by the synthesizer.
6. **Stability across regeneration.** Synthesis is seeded from the *shape structure and request*, never from spec file ordering, trait ordering, or map iteration order. A spec re-pin that does not change a shape must not change that shape's synthesized output. Assert this with a test that regenerates from a mutated-but-equivalent spec and diffs the synthesized responses. Without this, `mirror spec update` silently breaks every user test that asserted on a mock value.

### 4.11 NotImplemented semantics

Unimplemented operations return the protocol-correct error envelope with code `MirrorNotImplemented`, a message naming the service, operation, and the tier that would be required, an `x-mirror-not-implemented: <serviceID>.<operation>` header, and HTTP 501. It must be impossible to mistake this for a real cloud error — that mistake costs a developer an afternoon.

**One exception, and it matters more than it looks:** see §4.14. Returning 501 for a *read* operation that a tool calls during refresh will break that tool. Read the next three sections before implementing any service.

### 4.12 Wire-level realities that break naive implementations

These are the specific details that separate "works" from "almost works." Every one of them is a known, high-probability failure for a spec-driven implementation, because none of them are fully described by the spec.

1. **`aws-chunked` transfer encoding (S3).** Modern AWS SDKs upload objects with `Content-Encoding: aws-chunked`, `x-amz-decoded-content-length: <n>`, and a payload framed as `<hex-size>;chunk-signature=<sig>\r\n<data>\r\n…` — often with a trailing checksum chunk. **If you read the body naively you will store the chunk framing inside the object and every content assertion will fail.** Detect the encoding and de-frame before the body reaches the behavior pack. This is the single most likely cause of a "why is my file corrupted" bug in this project.
2. **Flexible checksums.** SDKs send `x-amz-checksum-crc32` (and sha1/sha256/crc32c variants) and `x-amz-sdk-checksum-algorithm`, and newer SDK versions default to sending a checksum on every upload. Accept, validate when present, echo back the corresponding `x-amz-checksum-*` on `GetObject`/`HeadObject`, and never reject a request merely because a checksum header is unrecognized.
3. **`Content-MD5` and `Expect: 100-continue`.** Honor `Content-MD5` when present. Handle `Expect: 100-continue` correctly or large uploads will stall.
4. **Error class drives SDK retry.** An error returned as 5xx will be retried up to the SDK's retry count, turning a clear failure into a slow, confusing one. Classify faults exactly: client errors are 4xx, and `Fault.Fault` must match the model's `error` trait.
5. **Endpoint self-reference.** `CreateQueue`/`GetQueueUrl` return a `QueueUrl` the client then uses for every subsequent call. It must be built from the address the *client* reached the emulator on (or a configured `--advertise-url`), never a hardcoded localhost. The same applies to S3 `Location`, SNS topic ARNs embedded in responses, and GCS `selfLink`/`mediaLink`.
6. **`HeadObject` returns no body but correct headers**, including `Content-Length`, `ETag`, `Last-Modified`, and user metadata.
7. **`ListObjectsV2` `encoding-type=url`** must URL-encode keys in the response when requested; SDKs request it to survive keys with special characters.
8. **CORS preflight.** Answer `OPTIONS` with permissive CORS headers so browser-based SDKs work. Configurable, permissive by default for a local tool.
9. **Timestamp formats differ per protocol and per member.** Honor `timestampFormat`: epoch seconds for JSON protocols by default, ISO-8601 for query/XML, RFC 7231 HTTP-date for headers. Getting this wrong produces "invalid date" errors deep inside SDK deserializers.
10. **Empty vs absent.** JSON protocols distinguish an absent member from an empty list/map. Emitting `[]` where the real service omits the member causes SDK-level and Terraform-level diffs.

### 4.13 Terraform reality — the read-path trap

`terraform apply` on a single `aws_s3_bucket` triggers **a dozen or more read calls during refresh**: `GetBucketAcl`, `GetBucketPolicy`, `GetBucketCors`, `GetBucketWebsite`, `GetBucketVersioning`, `GetBucketLogging`, `GetBucketLifecycleConfiguration`, `GetBucketReplication`, `GetBucketEncryption`, `GetBucketObjectLockConfiguration`, `GetBucketRequestPayment`, `GetBucketAccelerateConfiguration`, `GetBucketTagging`, and more. DynamoDB refresh calls `DescribeTimeToLive`, `DescribeContinuousBackups`, and `ListTagsOfResource`. SQS refresh calls `GetQueueAttributes` for the full attribute set.

**If any of these return 501, `terraform apply` fails** — and DoD item 8 fails with it.

Therefore: every emulate-tier service must answer its **entire refresh read-path** with a valid "not configured" response — the correct empty document, or the correct `NoSuch*Configuration` error that the provider is written to tolerate — rather than `NotImplemented`. Determine the exact set empirically by running Terraform against the emulator with provider debug logging enabled and enumerating every call it makes; commit that enumeration as `test/terraform/READ_PATH.md` and cover it with tests. This is not optional polish; it is the difference between passing and failing the definition of done.

### 4.14 Scope of the generated spec set (binary size and boot budget)

Do **not** generate code for every service in the AWS model repository. That path produces a multi-hundred-megabyte binary and blows the 2-second cold-start budget, defeating the project's core ergonomic promise.

- `specs/mirror.set` declares the generated set: the eight AWS emulate-tier services, GCS, plus a curated list of roughly twenty to thirty commonly co-used AWS services that ship at **mock tier** (choose from services that ordinary applications call incidentally — e.g. tagging, metrics, logs, identity, queue-adjacent, and configuration services).
- `mirror spec add <service>` appends to the set and regenerates, so a user who needs service #31 gets it with one command rather than a feature request. This is the mechanism that makes "the wall is gone" true, and it is a better promise than a large default binary.
- Service models are embedded **per service** and loaded **lazily on first request**, so `mirror up s3` pays for S3 only. Enabled-service selection filters at registration time, before any model is parsed.
- Assert binary size and cold start in CI. If either regresses past budget, the set is too large.

### 4.15 Hosted / shared mode

Beyond localhost, mirror must be runnable as a **shared team service** — one deployment several developers or a CI fleet point at — with the least possible friction:

- `--bind` for the listen address (default `127.0.0.1`, explicit opt-in to `0.0.0.0`), `--advertise-url` for self-referencing responses (§4.12.5), and optional TLS via supplied certificate paths.
- **Tenant isolation is already the account+region namespacing.** In hosted mode, each developer sets a distinct 12-digit account (via access key or `X-Mirror-Account-Id`) and is isolated by construction. Document this as the recommended team pattern.
- Named, per-tenant snapshots so a team can share a seeded fixture state: `mirror snapshot save --name golden`, then `mirror snapshot load --name golden --account <id>`.
- Hosted mode must print a startup banner stating plainly that there is no authentication and the deployment must not be exposed to untrusted networks. **Never add an auth token to solve this** — that is the covenant. Document network-level isolation as the answer.

---

## 5. Subagent swarms

Fourteen swarms. Every swarm codes against the frozen interfaces in §3 **from turn one** — no swarm waits for another to publish a contract, because the contracts are already published above. The only genuine dependency is that S0 lands the interface files so the tree compiles; every other swarm writes its code and its unit tests against the interfaces as specified here and against the reference fakes S0 ships.

**The decoupling rule.** Behavior packs (S5–S9) receive `spi.Request` with a decoded `Input map[string]any`. They therefore do **not** depend on the generated protocol layer, the codecs, or the spec ingestion pipeline in order to be written or unit-tested. A behavior pack's unit tests construct `spi.Request` values directly. This is what makes the fan-out real rather than nominal.

---

### S0 — Contracts & Foundation

**Produces.** `go.mod`; `internal/model`, `internal/spi`, `internal/receiver`, `internal/fusion`, `internal/registry`, `internal/proto` (interface only) with every declaration from §3 transcribed **verbatim**; `internal/config` (env + file + flags, precedence flags > env > file > default); `internal/logging` (structured, leveled, request-ID propagating); `internal/journal`; `internal/diag` (all §4.7 endpoints); `internal/idgen` (request IDs from `Rand`).

**Also produces — the parallelism enabler:** `internal/spitest`, a package of reference in-memory implementations of `Store`, `BlobStore`, `Bus`, `Clock` (controllable), `Rand` (seeded), and `Journal`, plus `spitest.Deps(t)` returning a ready `spi.Deps`. Every other swarm unit-tests against these without waiting for S3.

**Success.** `go build ./...` succeeds; `spitest` implementations pass a shared conformance suite that S3's real implementations must also pass; diagnostics endpoints serve against an empty registry.

---

### S1 — Spec ingestion & code generation

**Produces.** `internal/receiver/aws/smithy` (Smithy 2.0 JSON AST → `[]model.Service`, handling every trait in §4.2); `internal/receiver/gcp/discovery` (Google Discovery document → `[]model.Service`); `internal/fusion` implementation; `cmd/mirrorgen` emitting per-service Go packages under `internal/generated/<provider>/<service>/` containing the operation dispatch table, input/output shape descriptors, validation functions, and error descriptors; `make specs-sync` and `make generate`; `specs/mirror.lock`.

**Also produces.** `internal/specdiff`: a semantic API-surface differ over two `model.Bundle`s reporting added/removed operations, changed shapes, changed required-ness, and changed error sets — human-readable and machine-readable output.

**Also owns.** `specs/mirror.set` (§4.14) and the per-service lazy-embedding scheme: each generated service package embeds only its own model and parses it on first use, so enabled-service selection filters before any parsing happens.

**Success.** All v1 services ingest cleanly; `make generate` is byte-idempotent (asserted by test); unknown traits are recorded and never fatal; `specdiff` produces a correct report over two hand-built bundles in tests; generation is stable under spec re-pin per §4.10.6.

---

### S2 — Edge, codecs & identity

**Produces.** `internal/edge` (HTTP server, service demux by host/target/action/path, request-ID assignment, panic recovery, `x-mirror-*` headers, `NotImplemented` semantics per §4.11, S3 addressing per §4.4); `internal/proto/aws/{awsjson,restjson,restxml,awsquery}` and `internal/proto/gcp/gcprest` implementing `proto.Codec`; `internal/identity` (SigV4 parsing and derivation per §4.5, presigned URL expiry).

**Also owns every item in §4.12** — `aws-chunked` de-framing before the body reaches any behavior pack, checksum header handling, `Expect: 100-continue`, CORS preflight, per-protocol timestamp formats, empty-vs-absent member semantics, fault-class-to-status correctness, and `--advertise-url` resolution for self-referencing responses. These are transport concerns and must not be duplicated in behavior packs.

**Success.** Given a hand-built `model.Bundle` and a stub `Handler`, every codec round-trips representative requests for its protocol; SQS dual-protocol dispatch resolves correctly from both `X-Amz-Target` and `Action`; virtual-host and path-style S3 both route; unknown service and unknown operation both produce §4.11 errors; an `aws-chunked` upload arrives at the handler as de-framed bytes with the correct length.

---

### S3 — State, persistence & determinism

**Produces.** `internal/store` (account+region namespaced `Store`, in-memory with an optional embedded key-value persistence backend selected by config); `internal/blobs` (`BlobStore`, memory and on-disk); `internal/bus`; `internal/clock` (real and controllable); `internal/rand` (seeded, with `Derive`); snapshot/restore for the whole process state in one archive with a manifest recording the spec-lock hash.

**Success.** Passes S0's shared conformance suite; two accounts and two regions are provably isolated; snapshot → restart → restore round-trips arbitrary resource state; a snapshot taken under one spec-lock hash refuses to restore under a different one with a clear error; determinism suite passes.

---

### S4 — Mock-tier synthesizer

**Produces.** `internal/mock`: a generic `spi.BehaviorPack` constructed from any `model.Service`, implementing §4.10 in full — validation, deterministic synthesis, CRUD-by-convention inference, labeling, and `--strict` refusal.

**Success.** For every service in the vendored set that lacks an emulate pack, every operation returns a response that validates against its own declared output shape; identical requests yield byte-identical responses; required-member violations produce protocol-correct errors; `--strict` returns `NotImplemented`; CRUD inference demonstrably round-trips create → get → list → delete on at least three unrelated services.

---

### S5 — S3 behavior pack

**Produces.** `internal/services/aws/s3` implementing §4.8's S3 list at `emulate` tier, on `Store` + `BlobStore` + `Bus`.
**Success.** Unit tests against `spitest` cover every listed operation including multipart assembly, ETag computation for both single and multipart, versioning with delete markers, `ListObjectsV2` delimiter/common-prefix/continuation semantics, range and conditional GETs, and notification publication.

---

### S6 — DynamoDB behavior pack

**Produces.** `internal/services/aws/dynamodb` implementing §4.8's DynamoDB list at `emulate` tier, plus `internal/services/aws/dynamodb/expr`: a self-contained lexer, parser, and evaluator for the expression language, delivered as an independently testable package with its own exhaustive table-driven test suite.
**Success.** Every construct enumerated in §4.8 parses and evaluates correctly, including precedence, parenthesization, and error cases for malformed expressions; GSI/LSI projection semantics correct; pagination cursors are opaque, stable, and resumable; conditional failures return the documented error with `Item` when requested.

---

### S7 — SQS & SNS behavior packs

**Produces.** `internal/services/aws/sqs`, `internal/services/aws/sns`, and the SNS→SQS delivery path over `Bus`.
**Success.** Visibility timeout, long polling, and DLQ redrive all exercised on the **controllable clock** (no wall-clock sleeps in tests); FIFO ordering and deduplication correct; SNS→SQS delivery observed end-to-end in-process with and without `RawMessageDelivery`; `FilterPolicy` matching covered per match type; the HTTP subscription-confirmation handshake completes against a local test server.

---

### S8 — STS, IAM, SSM & Secrets Manager packs

**Produces.** `internal/services/aws/{sts,iam,ssm,secretsmanager}` per §4.8, plus `AllowAllAuthorizer`.
**Success.** `GetCallerIdentity` and `AssumeRole` return coherent, deterministic identities that the edge then honors on subsequent requests; parameter hierarchies resolve recursively with correct pagination; secret version staging labels transition correctly across `PutSecretValue`; `DeleteSecret` recovery-window semantics behave correctly on the controllable clock.

---

### S9 — GCS pack (cross-cloud proof)

**Produces.** `internal/services/gcp/gcs` per §4.8, served through the `gcpRestJson` codec, plus `internal/receiver/gcp/discovery` integration validation.
**Success.** All three upload types work, including resumable sessions with chunked PUTs and interrupted-then-resumed uploads; generation/metageneration preconditions enforced; the §4.9 provider-neutrality check passes.

---

### S10 — Proxy, record & replay

**Produces.** `internal/proxy` implementing all five modes from §3.8; cassette format (plain-text, diffable, one interaction per record, deterministic ordering); secret scrubbing at write time (Authorization headers, `X-Amz-Security-Token`, anything matching configured secret patterns, and any value that appeared in the process environment); `mirror drift` comparison of emulated vs recorded responses with a structured divergence report.

**Also produces — proxy as the accuracy oracle, not just a feature.** A maintainer-side workflow, `mirror record --fixtures`, that captures real-cloud behavior for an emulate-tier service once, scrubs it, and writes **differential conformance fixtures** into `test/fixtures/<service>/`. These fixtures are committed and consumed by S12 to grade behavior packs against recorded real behavior on every commit.

This matters more than it looks. Shape conformance against a spec proves the *envelope* is right and says nothing about whether `ListObjectsV2` paginates correctly or a message reappears after its visibility timeout. Recorded fixtures are the only cheap source of behavioral ground truth, and they invert the maintenance problem: a behavior pack no longer requires someone who *knows* the service's semantics, only a recording that adjudicates them. Design the fixture format to be plain-text and diffable so a re-record produces a reviewable change.

**Success.** Round-trip against a **local test server standing in for the real cloud** — record, then replay with the test server switched off, byte-identical; scrubbing verified by a test asserting no known-secret value survives into the cassette; drift report correctly identifies injected divergences; the fixture-capture workflow produces fixtures S12 can consume. **No test may require real cloud access** — fixtures are captured by a maintainer and committed, never fetched during a test run. Cassette and fixture output directories are gitignored by default except for the committed fixture set, and `mirror doctor` warns when an ad-hoc cassette directory is tracked by git.

---

### S11 — CLI & day-2 tooling

**Produces.** `cmd/mirror` with: `up` (accepting `s3`, `s3,sqs`, `--profile aws-core`, `--all`, `--tier <service>=<tier>`, `--strict`, `--seed`, `--persist`, `--bind`, `--advertise-url`, `--tls-cert`/`--tls-key`), `env`, `doctor`, `services`, `spec sync|pin|add|update|diff`, `snapshot save|load` (with `--name` and `--account` per §4.15), `drift`, `support-matrix`, `version`. Plus an `awslocal`-equivalent wrapper and a `gcslocal` equivalent. Hosted-mode startup banner per §4.15.

- `doctor` must diagnose, concretely: unset or wrong `AWS_ENDPOINT_URL*`, port already bound, region mismatch between client and server, path-style vs virtual-host misconfiguration, a client configured to reach the real cloud while the emulator is running, missing `STORAGE_EMULATOR_HOST`, and a stale spec lock. Each finding prints the exact command or export that fixes it.
- `support-matrix` generates `docs/SUPPORT.md` from the model. It is never hand-edited; a CI check asserts it is current.

**Success.** Every subcommand has a test; `up s3` serves S3 and returns §4.11 errors for DynamoDB; `env` output, when evaluated, makes a real SDK client reach the emulator; `doctor` detects each listed misconfiguration in a test harness.

---

### S12 — Conformance & integration harness

**Produces.** Everything needed to prove §6, automated end to end:

- `internal/conformance`: for every operation in every ingested service, generate a valid input from the shape, encode it, route it, decode it, and assert the decoded input matches; assert required-member violations produce the correct error shape and status; assert every declared error shape renders correctly per protocol. **This proves shape, not behavior — do not mistake it for an accuracy claim.**
- `internal/differential`: replays S10's committed real-cloud fixtures (`test/fixtures/<service>/`) against each emulate-tier pack and reports behavioral divergence. This is the project's actual accuracy oracle and the only one that can substantiate a fidelity claim; keep it green and report its coverage in `docs/SUPPORT.md`.
- `test/sdk/go`: `aws-sdk-go-v2` round-trips for every emulate-tier service. **`aws-sdk-go-v2` may be used only in tests, never in the emulator itself.**
- `test/sdk/python`: `boto3` round-trips, and `google-cloud-storage` round-trips for GCS.
- `test/cli`: AWS CLI v2 smoke scripts, skipped with a clear message when the CLI is absent.
- `test/terraform`: configurations covering S3 bucket + object + versioning, DynamoDB table + GSI, SQS queue + DLQ, SNS topic + SQS subscription, IAM role + policy, SSM parameter, Secrets Manager secret — each with `apply` then `destroy`, both asserted clean. Skipped with a clear message when Terraform is absent. **Also produces `test/terraform/READ_PATH.md`** (§4.13): the empirically enumerated refresh read-path per resource, captured from provider debug logs, with a test asserting none of those calls returns 501.
- Suites for: multi-tenancy isolation, persistence round-trip, determinism (same seed ⇒ byte-identical), cold-start budget, per-service standalone boot, mock-tier labeling and `--strict` refusal, generated-code reproducibility, and provider-neutrality (§4.9).
- `make test` (unit) and `make test-integration` (boots the binary, runs everything above, tears down), plus a CI workflow running both.

**Success.** Every oracle in §6 passes from a clean checkout with no manual intervention.

---

### S13 — Distribution, docs & governance

**Produces.** `Makefile` and build scripts producing a static binary for linux/darwin × amd64/arm64; a `Dockerfile` yielding a minimal image; **per-service image targets** so `mirror-s3` is a distributable artifact; `README.md` (quick start that works verbatim in under a minute, the three-tier explanation, the endpoint-configuration table, a link to the generated support matrix); `docs/EXTENDING.md` (how to add a receiver, a behavior pack, a codec, and a provider — each with a worked example); `docs/DAY2.md` (spec updates, drift, snapshots, upgrades); `docs/SUPPORT.md` (generated); `GOVERNANCE.md` and `COVENANT.md`.

**The covenants are product requirements, not paperwork.** `COVENANT.md` states, as binding project policy: no capability will ever require an auth token, account, or license key; no telemetry or phone-home will ever be added; the project's scope is *spec-complete by generation, behavior-complete only where declared*; and any future commercial offering may monetize only hosting, collaboration, and support — never the runner, never a protocol, never a service.

**Success.** A reader who has never seen the project can clone, build, run, and pass the Terraform smoke tests using the README alone.

---

## 6. Definition of Done

Automated, no manual steps, from a clean checkout:

1. `go build ./...`, `go vet ./...`, `gofmt -l` empty, `staticcheck` clean.
2. `go test ./...` passes.
3. `make generate` produces a byte-identical tree (generated code is committed and reproducible from pinned specs).
4. Conformance: every ingested operation round-trips through its protocol; every declared error shape renders correctly; required-member violations produce protocol-correct errors.
5. `aws-sdk-go-v2` performs every operation in §4.8 against the running binary.
6. `boto3` performs the same core operations; `google-cloud-storage` performs the GCS operations.
7. AWS CLI v2 works against `--endpoint-url http://127.0.0.1:4566` for the core operations.
8. `terraform apply` and `terraform destroy` both succeed for every configuration in `test/terraform`.
9. Two account IDs cannot observe each other's resources across every emulate-tier service.
10. Snapshot → restart → restore preserves and re-serves all state; a spec-lock mismatch is refused with a clear error.
11. Same seed + same request sequence ⇒ byte-identical responses.
12. `mirror up s3` serves S3 and returns §4.11 errors for every other service.
13. Mock tier: an un-packed service returns schema-valid, deterministic, `x-mirror-fidelity: mock`-labeled responses; `--strict` returns `NotImplemented` instead.
14. Proxy: record → replay round-trips against a local stand-in server; no secret value appears in any cassette.
15. Cold start under 2 seconds with persistence cold, asserted by test.
16. `docs/SUPPORT.md` is current with respect to the model (CI-asserted).
17. `internal/model` contains no service-specific string literal (provider-neutrality, CI-asserted).
18. Every dependency is permissively licensed (CI-asserted).
19. No `time.Now`, `math/rand`, or nondeterministic UUID call outside `internal/clock` and `internal/rand` (CI-asserted).
20. Docker image builds and passes the same integration smoke suite.
21. Every call in `test/terraform/READ_PATH.md` is answered without a 501 (§4.13).
22. S3 accepts `aws-chunked` uploads and stores de-framed bytes, verified by an SDK round-trip that reads back a byte-identical payload (§4.12.1).
23. Binary size and cold start are within budget with the full generated spec set enabled (§4.14).
24. Mock-tier synthesized output is unchanged by a spec re-pin that does not change the shape (§4.10.6).
25. Self-referencing URLs in responses reflect the address the client used or `--advertise-url` (§4.12.5).
26. Every emulate-tier service is graded green by `internal/differential` against its committed real-cloud fixtures, and `docs/SUPPORT.md` reports that fixture coverage per service. Shape conformance (item 4) does not satisfy this item.

---

## 7. Coordination rules

1. **Interfaces are frozen by §3.** Transcribe them exactly. Objections go in `docs/INTERFACE_NOTES.md`; they do not go in code.
2. **Behavior packs never import the edge, the codecs, or generated protocol code.** They depend only on `internal/spi` and `internal/model`. A CI import-graph check enforces this.
3. **The edge never imports a behavior pack.** Registration is by `init()` into the registry, wired in `cmd/mirror`.
4. **Every swarm ships tests with its code.** A swarm's deliverable is not complete without them. The integration harness is kept green continuously, not reconciled at the end.
5. **No TODOs, no stubs, no "not yet implemented" below the §4.8 fidelity line.** If something cannot be finished, move it to mock tier in §4.8, update the support matrix, and say so in the README. Honest reduction is acceptable; silent partial implementation is not.
6. **No wall-clock sleeps in tests.** Anything time-dependent uses the controllable clock.
7. **Every exported identifier is documented.** Every package has a package comment explaining its role in the pipeline.
8. **When the spec and this document disagree, the vendored spec wins** — and the disagreement is recorded in `docs/INTERFACE_NOTES.md`.
9. **Degradation policy — protect the spine.** If the work must be reduced, reduce it by dropping *services*, never by dropping the pipeline. The spine is: vendored spec → receiver → canonical model → codegen → codec → edge → **one** emulate-tier behavior pack (S3) → SDK round-trip → Terraform apply/destroy. A repository that proves that spine end to end with three services is a success. A repository with eight half-working services and no reproducible generation is a failure, regardless of how much of it appears to work. When you drop something, record it in `docs/SUPPORT.md` and the README; do not leave it implied.
10. **Verify against a real client early, not at the end.** The first thing that should work after the spine compiles is an `aws-sdk-go-v2` call against the running binary. Every wire-level assumption in §4.12 is cheap to fix on day one and expensive to fix once eight packs are built on top of it.
