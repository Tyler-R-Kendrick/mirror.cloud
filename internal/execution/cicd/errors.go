package cicd

import "fmt"

// ErrorKind is the typed failure class for JobControl operations.
// Mirrors spi.CapabilityErrorKind and extends it for job lifecycle classes.
type ErrorKind string

const (
	ErrValidation     ErrorKind = "validation"
	ErrUnsupported    ErrorKind = "unsupported"
	ErrUnavailable    ErrorKind = "unavailable"
	ErrAbsent         ErrorKind = "absent"
	ErrConflict       ErrorKind = "conflict"
	ErrCancelled      ErrorKind = "cancelled"
	ErrTimeout        ErrorKind = "timeout"
	ErrPolicyDenied   ErrorKind = "policy_denied"
	ErrIntegrity      ErrorKind = "integrity"
	ErrOutcomeUnknown ErrorKind = "outcome_unknown"
)

// Error is a typed JobControl failure.
type Error struct {
	Kind    ErrorKind
	Op      string
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		if e.Op != "" {
			return fmt.Sprintf("%s: %s: %s", e.Kind, e.Op, e.Message)
		}
		return fmt.Sprintf("%s: %s", e.Kind, e.Message)
	}
	if e.Op != "" {
		return fmt.Sprintf("%s: %s", e.Kind, e.Op)
	}
	return string(e.Kind)
}

// AsError returns *Error when err is one.
func AsError(err error) (*Error, bool) {
	e, ok := err.(*Error)
	return e, ok
}
