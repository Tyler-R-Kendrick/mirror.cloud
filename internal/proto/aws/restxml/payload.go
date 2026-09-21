package restxml

import (
	"encoding/xml"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// decodePayload reads the request body as the operation's payload member,
// walking the generated model rather than a hand-written struct per
// operation: the model already says which element each member arrives under
// (xmlName), which members are attributes, which lists are flattened and what
// their elements are called -- everything the per-operation structs were
// re-deriving by hand.
//
// A body that does not parse, whose root is not the payload's element, that
// carries an element the shape does not declare, or whose boolean or number
// does not read as one, is kept raw under `_body`, which is how the packs
// answer MalformedXML -- the reference validates against its schema and says
// so. A payload the model types as a string or blob -- a bucket policy -- is
// the raw body under its own name.
func decodePayload(svc *model.Service, op *model.Operation, raw []byte, in map[string]any) {
	// The four named configurations are stored and answered back as documents
	// keyed by their element names, which is what the encoder writes, so they
	// keep the element-keyed walk rather than the member-keyed one.
	if ns := namedConfigurationShape(op.Name); ns.configuration != "" {
		configuration, ok := parseNamedConfiguration(raw, ns.configuration)
		if !ok {
			in["_body"] = string(raw)
			return
		}
		in[ns.configuration] = configuration
		return
	}
	name, member, ok := payloadMember(svc, op)
	if !ok {
		in["_body"] = string(raw)
		return
	}
	switch svc.Shapes[member.Shape].Kind {
	case model.KindString, model.KindBlob, model.KindDocument:
		in[name] = string(raw)
		return
	}
	var root namedXMLNode
	bad := xml.Unmarshal(raw, &root) != nil || root.XMLName.Local != wireName(name, member.Binding)
	var value any
	if !bad {
		value = decodeShape(svc, member.Shape, root, 0, &bad)
	}
	if bad {
		in["_body"] = string(raw)
		return
	}
	in[name] = value
	// ponytail: the S3 pack reads a few payload members flat at the top of the
	// input -- `Objects` rather than `Delete.Objects` -- because the hand
	// parser put them there. Lifted here until that pack is extracted, at
	// which point this table and the reads go together.
	for _, lift := range liftedMembers[op.Name] {
		if payload, ok := in[name].(map[string]any); ok {
			if v, ok := payload[lift]; ok {
				in[lift] = v
			}
		}
	}
}

var liftedMembers = map[string][]string{
	"DeleteObjects":       {"Objects", "Quiet"},
	"CreateBucket":        {"LocationConstraint"},
	"RestoreObject":       {"Days"},
	"PutBucketTagging":    {"TagSet"},
	"PutObjectTagging":    {"TagSet"},
	"PutBucketVersioning": {"Status"},
}

// payloadMember is the one input member bound to the body.
func payloadMember(svc *model.Service, op *model.Operation) (string, model.Member, bool) {
	for name, m := range svc.Shapes[op.Input].Members {
		if m.Binding.Location == "payload" {
			return name, m, true
		}
	}
	return "", model.Member{}, false
}

// wireName is the element (or attribute) a member arrives as: its xmlName,
// and its own name where the specification gives none. A namespace prefix on
// an attribute name (`xsi:type`) is not part of what encoding/xml reports.
func wireName(name string, b model.MemberBinding) string {
	if b.Name == "" {
		return name
	}
	if b.XMLAttribute {
		if _, local, ok := strings.Cut(b.Name, ":"); ok {
			return local
		}
	}
	return b.Name
}

// decodeShape turns one element into the value its shape describes: a map
// keyed by member name for a structure, a list for a list, and a scalar
// coerced by kind. Members absent from the element are absent from the map;
// an element that is present and empty is an empty map, because `<SSE-S3/>`
// means something by being there. Anything the shape cannot account for sets
// bad, and the caller keeps the body raw.
func decodeShape(svc *model.Service, shapeID string, node namedXMLNode, depth int, bad *bool) any {
	shape, ok := svc.Shapes[shapeID]
	text := strings.TrimSpace(node.Text)
	if !ok || depth > maxDepth {
		return text
	}
	switch shape.Kind {
	case model.KindStructure, model.KindUnion:
		out := map[string]any{}
		known := map[string]bool{}
		for name, m := range shape.Members {
			wire := wireName(name, m.Binding)
			if m.Binding.XMLAttribute {
				for _, a := range node.Attrs {
					if a.Name.Local == wire {
						out[name] = a.Value
					}
				}
				continue
			}
			known[wire] = true
			if m.Binding.XMLFlattened && svc.Shapes[m.Shape].Kind == model.KindList {
				var items []any
				for _, c := range node.Children {
					if c.XMLName.Local == wire {
						items = append(items, decodeShape(svc, svc.Shapes[m.Shape].Member, c, depth+1, bad))
					}
				}
				if items != nil {
					out[name] = items
				}
				continue
			}
			for _, c := range node.Children {
				if c.XMLName.Local == wire {
					out[name] = decodeShape(svc, m.Shape, c, depth+1, bad)
					break
				}
			}
		}
		for _, c := range node.Children {
			if !known[c.XMLName.Local] {
				*bad = true
			}
		}
		return out
	case model.KindList:
		elem := shape.MemberBinding.Name
		if elem == "" {
			elem = "member"
		}
		items := []any{}
		for _, c := range node.Children {
			if c.XMLName.Local == elem {
				items = append(items, decodeShape(svc, shape.Member, c, depth+1, bad))
			} else {
				*bad = true
			}
		}
		return items
	case model.KindMap:
		key, val := shape.KeyBinding.Name, shape.MemberBinding.Name
		if key == "" {
			key = "key"
		}
		if val == "" {
			val = "value"
		}
		out := map[string]any{}
		for _, entry := range node.Children {
			var k string
			var v any
			for _, c := range entry.Children {
				switch c.XMLName.Local {
				case key:
					k = strings.TrimSpace(c.Text)
				case val:
					v = decodeShape(svc, shape.Member, c, depth+1, bad)
				}
			}
			out[k] = v
		}
		return out
	case model.KindBoolean:
		b, err := strconv.ParseBool(text)
		*bad = *bad || err != nil
		return b
	case model.KindInteger:
		n, err := strconv.Atoi(text)
		*bad = *bad || err != nil
		return n
	case model.KindLong:
		n, err := strconv.ParseInt(text, 10, 64)
		*bad = *bad || err != nil
		return n
	}
	return text
}

// maxDepth bounds the shape-graph descent, for the reason awsquery gives: the
// walk follows the model, and a shape that reached itself would recurse on a
// one-line body. No S3 payload is deeper than eight levels.
const maxDepth = 32
