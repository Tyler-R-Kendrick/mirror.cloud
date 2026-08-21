// Package model is the provider-neutral canonical behavioral model.
// Nothing in this package may branch on a specific Provider value or name a
// specific service. Receivers normalize into these types; generators, the
// edge, and exporters consume only these types.
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
//
//	verified > observed > declared
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

// Service is one API surface inside a Bundle.
type Service struct {
	ID             string // stable, lowercase: "aws.s3", "gcp.storage"
	Namespace      string // spec-native namespace, e.g. "com.amazonaws.s3"
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

// Operation is one RPC or REST method on a Service.
type Operation struct {
	Name        string
	HTTP        HTTPBinding
	Target      string // full X-Amz-Target value, when applicable
	QueryAction string // Action= value for query protocols
	Input       string // shape ID
	Output      string // shape ID
	Errors      []string
	Idempotent  bool
	Readonly    bool
	Pagination  *Pagination
	Confidence  Confidence
	Source      SourceRef
}

// HTTPBinding is the REST binding for an operation.
type HTTPBinding struct {
	Method string // "POST", "GET", ...
	URI    string // e.g. "/{Bucket}/{Key+}"; "/" for RPC-style protocols
	Code   int    // success status code
}

// Pagination describes token-based paging members.
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

// Shape is one named type in a service model.
type Shape struct {
	ID            string
	Kind          ShapeKind
	Members       map[string]Member // structure/union
	Member        string            // list/map value shape ID
	Key           string            // map key shape ID
	EnumValues    []string
	Constraints   Constraints
	Streaming     bool // blob streaming payload
	Error         *ErrorTrait
	Doc           string
	UnknownTraits []string // recorded, never fatal
}

// Member is one field of a structure or union.
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
	Location        string // "" | "label" | "query" | "header" | "prefixHeaders" | "payload" | "statusCode" | "queryParams"
	Name            string // wire name (header name, query param, XML/JSON field)
	TimestampFormat string // "" | "date-time" | "http-date" | "epoch-seconds"
	XMLAttribute    bool
	XMLFlattened    bool
	XMLNamespace    string
}

// Constraints are schema bounds from the specification.
type Constraints struct {
	MinLength, MaxLength *int64
	MinValue, MaxValue   *float64
	Pattern              string
	UniqueItems          bool
}

// ErrorTrait describes a modeled error shape.
type ErrorTrait struct {
	Fault      string // "client" | "server"
	HTTPStatus int
	Retryable  bool
	Code       string // wire error code
}

// ServiceByID returns the named service, if present.
func (b *Bundle) ServiceByID(id string) *Service {
	if b == nil {
		return nil
	}
	for i := range b.Services {
		if b.Services[i].ID == id {
			return &b.Services[i]
		}
	}
	return nil
}

// OperationByName returns the named operation, if present.
func (s *Service) OperationByName(name string) *Operation {
	if s == nil {
		return nil
	}
	for i := range s.Operations {
		if s.Operations[i].Name == name {
			return &s.Operations[i]
		}
	}
	return nil
}
