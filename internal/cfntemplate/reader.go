// Package cfntemplate reads CloudFormation templates: their JSON or YAML, the checks a template passes
// before a stack is made from it, the order its resources are made in, its conditions and its intrinsic
// functions. It holds no stack and calls no service: the engine in internal/services/aws/cloudformation
// gives it the caller's account and Region, the values a Ref reads (parameters, and the ids and attributes
// of the resources made so far), and takes back plain values and the service's ValidationError.
package cfntemplate

import (
	"cmp"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Reader reads templates for one request. Account and Region are the caller's, which the pseudo parameters
// answer, and NotificationARNs the request's, read whenever AWS::NotificationARNs is (none when nil).
// Walks is for tests only: it counts the resources whose properties an ordering walked, which the engine's
// tests of its cost read through the pack that embeds the Reader. Nothing in the library or the engine reads it.
type Reader struct {
	Account, Region  string
	NotificationARNs func() []any
	Walks            int
	tpl              map[string]any  // the template read: its Mappings, which findInMap reads, and its parameters' types
	skip             map[string]bool // the resources of the template read that the stack does not make (skipped)
}

// Read parses body, the template of the stack named stack, of id id (none of a stack, to read a template
// alone), and orders its resources. in is the request's input, whose Parameters it reads, and check the
// caller's own check of the parsed template. It answers the template, the resources a stack makes from it
// in the order they are made, and what a Ref reads besides a resource: the parameters, the pseudo
// parameters and the conditions they settle.
func (p *Reader) Read(body string, in map[string]any, stack, id string, check func(tpl map[string]any) error) (map[string]any, []string, map[string]string, error) {
	tpl, err := Parse(body)
	var order []string
	var refs map[string]string
	var known map[string]string
	if err == nil {
		known, err = Parameters(in, tpl)
	}
	if err == nil {
		p.tpl, refs = tpl, p.Conditioned(tpl, known, stack, id)
		order, err = p.order(tpl, refs)
		p.skipped(order, refs)
	}
	if err == nil {
		err = cmp.Or(p.sound(tpl, refs, stack), check(tpl))
	}
	if err == nil && stack != "" { // a stack, or a change set to one, is made from it
		order, err = made(tpl, order, refs)
	}
	return tpl, order, refs, err
}

// Unknown is whether v is, or holds, an id known only once its resource is made.
func Unknown(v any) bool { return holds(v, unknown) }

func invalid(message string) *spi.Fault {
	return &spi.Fault{Code: "ValidationError", Message: message, HTTPStatus: 400, Fault: "client"}
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }
