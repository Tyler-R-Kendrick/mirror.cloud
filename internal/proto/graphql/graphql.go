// Package graphql is the wire codec for GraphQL over HTTP.
//
// It is the partner of internal/receiver/graphql: the receiver reads a schema
// into the model, and this speaks the protocol that schema is served over. The
// two were written apart, and the gap between them was where the last
// hand-written pack lived.
//
// WHAT A GRAPHQL REQUEST IS, IN THIS CODEC'S TERMS. One POST to one endpoint,
// carrying `{"query": ..., "variables": ...}`. The document's root field names
// the operation -- for a service where every request goes to one path, the
// field IS the routing. The arguments that field is passed are the operation's
// input, and they arrive two ways: written into the document as literals, or
// named as `$variables` and supplied beside it.
//
// THE DISTINCTION THAT MATTERS, AND WHICH COST A RELEASE TO FIND. A request's
// arguments are not its envelope. `{query, variables}` is the envelope;
// `projectCreate(input: {name: "x"})` passes one argument named `input`. A
// codec that hands a handler the envelope has told it nothing the model
// declares, and the handler must then dig the arguments out itself -- which is
// what the Railway pack's own `hydrate()` did, flattening `variables.input` to
// the top level. That flattening was invisible while a hand-written pack was
// the only reader, because the pack and the flattening were written together.
// It became a wall the moment the engine validated the same request against the
// shape the schema declares: the schema says `projectCreate(input:)`, so the
// input has one member, and a flattened `{"name": "x"}` has none of it.
//
// So Decode presents arguments. That is the whole of the fix, and it belongs
// here rather than in any service's code, because which arguments a field takes
// is a fact about GraphQL and the schema, not about a vendor.
//
// WHAT THIS DELIBERATELY DOES NOT DO. It does not honour a selection set: a
// response carries everything the operation answered, not the subset the
// document asked for. Nothing in this tree projects one, and a codec that
// pretended to would be claiming a fidelity no test checks. It does not honour
// an alias either -- `mine: project(id:)` comes back under `project` -- because
// Encode is handed the response and the operation but not the request, so the
// alias is not in scope by the time the answer is written. Both are honest
// gaps, named here rather than discovered later.
package graphql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Codec implements proto.Codec for GraphQL over HTTP.
type Codec struct{}

func (Codec) Protocol() model.Protocol { return model.ProtoGraphQL }

// envelope is the request body a GraphQL server is posted.
type envelope struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

// read parses the body and leaves it re-readable, because Route runs before
// Decode and both need it.
func read(r *http.Request) envelope {
	if r.Body == nil {
		return envelope{}
	}
	b, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(b))
	var env envelope
	_ = json.Unmarshal(b, &env)
	return env
}

// Route answers which operation a request runs: the document's root field.
//
// A request to a path the service does not serve is refused rather than read.
// The endpoint is part of what the schema's provenance established, so a POST
// to some other path is not this service's request at all, and reading its body
// for a field name would let any path reach any operation.
func (Codec) Route(svc *model.Service, r *http.Request) (*model.Operation, error) {
	if !servedHere(svc, r) {
		return nil, fmt.Errorf("graphql: %s: not served at %s", svc.ID, path(r))
	}
	name := rootField(read(r).Query).Name
	if op := svc.OperationByName(name); op != nil {
		return op, nil
	}
	// An operation the schema does not declare still routes to a named
	// operation rather than to an error, so the layer above answers with the
	// service's own "no such field" rather than with a transport failure.
	return &model.Operation{
		Name: name,
		HTTP: model.HTTPBinding{Method: http.MethodPost, URI: endpoint(svc), Code: 200},
	}, nil
}

// Decode presents the root field's arguments as the operation's input.
//
// A document whose arguments cannot be read to the end is refused rather than
// half-decoded. Handing back what was read before the document stopped making
// sense is how a required member arrives present and a nested object arrives
// truncated, which validates and then acts on input the caller never sent.
func (c Codec) Decode(svc *model.Service, op *model.Operation, r *http.Request) (*spi.Request, error) {
	env := read(r)
	root := rootField(env.Query)
	if root.Bad {
		return nil, fmt.Errorf("graphql: %s: %s: cannot read the arguments", svc.ID, op.Name)
	}
	in := make(map[string]any, len(root.Args))
	for name, v := range root.Args {
		in[name] = resolve(v, env.Variables)
	}
	return &spi.Request{ServiceID: svc.ID, Operation: op.Name, Input: in, HTTP: r}, nil
}

// resolve replaces variable references with what `variables` supplies, at any
// depth: `input: {name: $name}` is as legal as `input: $input`.
//
// A reference with nothing supplied for it resolves to nil rather than to its
// own name. The alternative -- leaving `$name` as a string -- is how a
// validator is told a required member is present when the caller never sent
// one.
func resolve(v any, vars map[string]any) any {
	switch t := v.(type) {
	case varRef:
		return vars[string(t)]
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, item := range t {
			out[k] = resolve(item, vars)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = resolve(item, vars)
		}
		return out
	default:
		return v
	}
}

// Encode writes `{"data": {<field>: <answer>}}`.
//
// GraphQL has one response envelope and one status. A field that answers a
// scalar answers it directly -- Railway's projectDelete is `true` and nothing
// more -- which is what an output projecting bir.TopLevelRaw means here. It is
// read untyped, unlike the REST codecs' raw body, because in GraphQL the value
// is placed INTO a JSON document rather than written as the body: a boolean
// stays a boolean instead of becoming the four characters "true".
func (Codec) Encode(svc *model.Service, op *model.Operation, w http.ResponseWriter, resp *spi.Response) error {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(http.StatusOK)
	if resp == nil || resp.Output == nil {
		return json.NewEncoder(w).Encode(map[string]any{"data": nil})
	}
	var answer any = resp.Output
	if raw, ok := resp.Output[bir.TopLevelRaw]; ok {
		answer = raw
	} else if list, ok := resp.Output[bir.TopLevelList]; ok {
		answer = list
	}
	return json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{op.Name: answer}})
}

// EncodeFault writes a GraphQL error.
//
// The status is 200. That is not a quirk being emulated but the protocol: a
// GraphQL request that reaches the server and is answered -- even with an error
// -- succeeded at the transport layer, and the errors live in the body. A
// client that reads the status to decide whether the call worked is wrong about
// GraphQL, and a server that reported 400 here would be agreeing with it.
func (Codec) EncodeFault(svc *model.Service, op *model.Operation, w http.ResponseWriter, f *spi.Fault, requestID string) error {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(map[string]any{
		"errors": []any{map[string]any{
			"message":    f.Message,
			"extensions": map[string]any{"code": f.Code},
		}},
	})
}
