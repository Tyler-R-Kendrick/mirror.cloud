// Package spi is the service-provider interface between the edge and
// behavior packs. Packs depend only on this package and internal/model.
package spi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

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
	ServiceID     string
	SourceService string // trusted originating service for internal service-to-service calls
	Operation     string
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
	HTTP                 *http.Request
	S3ValidateSignatures bool
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
	Headers    http.Header    // protocol-level additions required on an error response
}

func (f *Fault) Error() string {
	if f == nil {
		return ""
	}
	if f.Message != "" {
		return fmt.Sprintf("%s: %s", f.Code, f.Message)
	}
	return f.Code
}

// NotImplemented returns the distinguishable unimplemented-operation fault.
func NotImplemented(serviceID, operation, requiredTier string) *Fault {
	return &Fault{
		Code:       "MirrorNotImplemented",
		Message:    fmt.Sprintf("%s.%s is not implemented at the requested fidelity; required tier: %s", serviceID, operation, requiredTier),
		HTTPStatus: 501,
		Fault:      "client",
		Fields:     map[string]any{"service": serviceID, "operation": operation, "requiredTier": requiredTier},
	}
}

// Deps is the dependency bundle every behavior pack receives at construction.
type Deps struct {
	Store                     Store
	Blobs                     BlobStore
	Bus                       Bus
	Clock                     Clock
	Rand                      Rand
	Journal                   Journal
	Model                     *model.Bundle
	Authorizer                Authorizer
	Compute                   ComputeProvider
	S3AllowNonstandardRegions bool
}

// Store is account+region namespaced structured state.
type Store interface {
	Scope(account, region string) Scope
	Scopes(ctx context.Context) ([]Identity, error)
	Snapshot(ctx context.Context, w io.Writer) error
	Restore(ctx context.Context, r io.Reader) error
	Close() error
}

// Scope is one account+region namespace.
type Scope interface {
	Collection(name string) Collection
}

// Collection is a named key-value collection inside a Scope.
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

// KV is one collection entry.
type KV struct {
	Key   string
	Value []byte
}

// Tx is an atomic mutation over one Collection.
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

// BlobInfo is metadata for a stored blob.
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
	// AfterUntil fires at t according to this clock.
	AfterUntil(t time.Time) <-chan time.Time
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

// Entry is one journaled request.
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

// Filter selects journal entries.
type Filter struct {
	ServiceID string
	Operation string
	Since     time.Time
	Limit     int
}

// Authorizer is the seam for future policy evaluation. v1 ships AllowAll.
type Authorizer interface {
	Authorize(ctx context.Context, id Identity, serviceID, operation, resource string) error
}

// RequestAuthorizer optionally evaluates decoded request fields as policy context.
type RequestAuthorizer interface {
	AuthorizeRequest(ctx context.Context, req *Request, resource string) error
}

// ComputeProvider is the seam for future function-execution services
// (Lambda and equivalents). v1 registers no implementation; services that
// need it return NotImplemented.
type ComputeProvider interface {
	Create(ctx context.Context, spec ComputeSpec) (string, error)
	Invoke(ctx context.Context, id string, payload []byte) ([]byte, error)
	Delete(ctx context.Context, id string) error
}

// ComputeSpec describes a future function-execution unit.
type ComputeSpec struct {
	Runtime string
	Handler string
	Code    []byte
}

// AllowAllAuthorizer implements Authorizer as documented allow-all.
type AllowAllAuthorizer struct{}

// Authorize always succeeds.
func (AllowAllAuthorizer) Authorize(context.Context, Identity, string, string, string) error {
	return nil
}
