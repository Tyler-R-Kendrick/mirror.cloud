package cfntemplate

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Coerce reads a template's value as a property of type want (the template reference's Boolean,
// Integer or String), itself or its text as a Ref to a parameter gives it ("the parameter value
// becomes a string", User Guide, Parameters), and answers whether it is one. A Boolean is true or
// false in any case, as YAML 1.1, the templates' YAML, spells it true, True or TRUE; an Integer is
// decimal digits after an optional minus, as a Number parameter's text is ("016" is 16, "-5" is -5; "16.0", "1e1", "+16" and " 16" are
// none); a String is a scalar's text (123 is "123"). Anything else is answered as it was given.
func Coerce(v any, want string) (any, bool) {
	switch v.(type) {
	case string, bool, float64, int, int64, uint64:
	default:
		return v, false
	}
	if text, yes := spi.JSONText(v), strings.EqualFold(spi.JSONText(v), "true"); want == "Boolean" {
		return yes, yes || strings.EqualFold(text, "false")
	} else if n, err := strconv.Atoi(text); want == "Integer" {
		return n, err == nil && strings.TrimLeft(strings.TrimPrefix(text, "-"), "0123456789") == ""
	} else {
		return text, true
	}
}

// Str is a String property's value: a scalar's text (coerce), nothing for anything else.
func Str(v any) string { s, _ := Coerce(v, "String"); t, _ := s.(string); return t }

// Text is a value as a String holds it, an output's OutputValue (the model's String) or a ${Name} in a
// Fn::Sub: a list's members comma-joined, as a CommaDelimitedList is written, anything else its text.
func Text(v any) string {
	if list, isList := v.([]any); isList {
		parts := []string{}
		for _, x := range list {
			parts = append(parts, Str(x))
		}
		return strings.Join(parts, ",")
	}
	return spi.JSONText(v)
}

// IsTrue is whether v is the Boolean true, in any case (coerce).
func IsTrue(v any) bool { b, _ := Coerce(v, "Boolean"); return b == true }

// PhysicalName is the name of a resource the template leaves unnamed, in the modelled form
// <stack>-<logical id>-<suffix>. The suffix is twelve hex digits of a hash of the stack id and logical
// id, so no two stacks, and no two resources of one stack, share a name, and an update finds the name its
// create made. Only letters, digits and hyphens are kept; a bucket's name is lower case and a FIFO
// queue's ends in .fifo; the stack name and logical id, never the suffix, are cut to the longest name its
// service takes, longest.
func PhysicalName(stack, logical, stackID, typ string, longest int, props map[string]any) string {
	sum := sha256.Sum256([]byte(stackID + "/" + logical))
	end := "-" + hex.EncodeToString(sum[:6])
	if typ == "AWS::SQS::Queue" && IsTrue(props["FifoQueue"]) {
		end += ".fifo"
	}
	s, l, room := unnameable.ReplaceAllString(stack, ""), unnameable.ReplaceAllString(logical, ""), longest-len(end)-1
	l = l[:min(len(l), max(room/2, room-len(s)))]
	n := s[:min(len(s), room-len(l))] + "-" + l + end
	if typ == "AWS::S3::Bucket" {
		return strings.ToLower(n)
	}
	return n
}

var unnameable = regexp.MustCompile(`[^A-Za-z0-9-]+`)
