// Package graphql ingests a GraphQL introspection result into the canonical
// model.
//
// It exists because railway/graphql was the last hand-written pack with no
// route out. Every other extraction moved behaviour from Go into data against a
// model that already existed; Railway had no model, and could not have one,
// because mirror read Smithy, Discovery and OpenAPI and GraphQL is none of
// those. C53 recorded that as the blocker and named the question that decides
// the approach -- whether the selection set is emulated at all. It is not: no
// test in the tree projects one, so the model's job here is to declare what an
// operation answers, not to reshape it per request.
//
// WHAT A GRAPHQL SCHEMA IS, IN THIS MODEL'S TERMS. A root field of the query or
// mutation type is an operation. Its arguments are the members of a synthesized
// input structure. Its return type is the output shape. NON_NULL is `required`
// on the member that carries it, LIST is a list shape, and the named types
// reachable from there are the shape graph. That mapping is total: nothing in
// an introspection result has to be dropped or guessed at.
//
// WHAT IT CANNOT KNOW, AND THIS IS A PROPERTY OF THE FORMAT. An introspection
// result does not carry the endpoint it was served from. OpenAPI has `servers`,
// Discovery has `baseUrl`, Smithy has an endpoint prefix trait; GraphQL has
// nothing, because the schema is the answer to a question asked AT an endpoint
// the client already knew. So every operation here binds to POST at
// defaultEndpoint, and a caller that serves a schema somewhere else has to say
// so from outside the document. Writing the real path in here would be one
// provider's deployment detail compiled into a format reader -- the smell C41
// records, and the reason this file names no vendor.
package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

// Receiver implements receiver.Receiver for GraphQL introspection results.
type Receiver struct{}

func (Receiver) Name() string { return "graphql" }

// defaultEndpoint is where operations bind absent outside knowledge. `/graphql`
// is the community default and the one every GraphQL server documents first; it
// is a default, not a discovery, and the package comment says why there can be
// no discovery.
const defaultEndpoint = "/graphql"

// Detect looks for the introspection result's own root. `__schema` with a
// `queryType` is the shape of an answer to the introspection query and of
// nothing else: Smithy declares `smithy`, Discovery `discoveryVersion`, OpenAPI
// `openapi`, and none of them carries a double-underscored key at all.
func (Receiver) Detect(_ string, head []byte) bool {
	if doc, err := parse(head); err == nil && doc != nil && doc.QueryType != nil {
		return true
	}
	// A document larger than the head window truncates mid-value, so fall back
	// to the two keys rather than refusing the file.
	text := string(head)
	return strings.Contains(text, `"__schema"`) && strings.Contains(text, `"queryType"`)
}

// Ingest turns one introspection result into one service.
func (Receiver) Ingest(ctx context.Context, src model.SourceRef, data []byte) ([]model.Service, error) {
	_ = ctx
	doc, err := parse(data)
	if err != nil {
		return nil, fmt.Errorf("graphql: %s: %w", src.Path, err)
	}
	if doc == nil || doc.QueryType == nil {
		return nil, fmt.Errorf("graphql: %s: no __schema with a queryType", src.Path)
	}
	types := map[string]*gqlType{}
	for i := range doc.Types {
		if name := doc.Types[i].Name; name != "" {
			types[name] = &doc.Types[i]
		}
	}
	sh := &shaper{types: types, shapes: map[string]model.Shape{}, active: map[string]bool{}}

	svc := model.Service{
		ID:             serviceID(src.Path),
		Namespace:      doc.QueryType.Name,
		Protocol:       model.ProtoRESTJSON1,
		EndpointPrefix: provider(serviceID(src.Path)),
		Shapes:         sh.shapes,
		Source:         src,
	}

	// A root field of the query type is readonly and one of the mutation type
	// is not. That is the only thing GraphQL says about an operation's effect,
	// and it says it structurally rather than per-field, so it is `declared`.
	for _, root := range []struct {
		holder   *typeName
		readonly bool
	}{{doc.QueryType, true}, {doc.MutationType, false}} {
		if root.holder == nil {
			continue
		}
		t := types[root.holder.Name]
		if t == nil {
			continue
		}
		fields := append([]gqlField(nil), t.Fields...)
		sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
		for _, f := range fields {
			out, _ := sh.resolve(f.Type)
			svc.Operations = append(svc.Operations, model.Operation{
				Name: f.Name,
				HTTP: model.HTTPBinding{Method: "POST", URI: defaultEndpoint, Code: 200},
				// A field with no arguments still gets an input shape, because
				// an operation the engine can validate must have one to
				// validate against; it is simply a structure with no members.
				Input:      sh.arguments(f),
				Output:     out,
				Readonly:   root.readonly,
				Confidence: model.ConfDeclared,
				Source:     src,
			})
		}
	}
	if len(svc.Operations) == 0 {
		return nil, fmt.Errorf("graphql: %s: schema declares no root fields", src.Path)
	}
	return []model.Service{svc}, nil
}

// ---------------------------------------------------------------- the document

type schemaDoc struct {
	QueryType    *typeName `json:"queryType"`
	MutationType *typeName `json:"mutationType"`
	Types        []gqlType `json:"types"`
}

type typeName struct {
	Name string `json:"name"`
}

type gqlType struct {
	Kind          string     `json:"kind"`
	Name          string     `json:"name"`
	Description   string     `json:"description"`
	Fields        []gqlField `json:"fields"`
	InputFields   []gqlInput `json:"inputFields"`
	EnumValues    []gqlEnum  `json:"enumValues"`
	PossibleTypes []typeRef  `json:"possibleTypes"`
}

type gqlField struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Args        []gqlInput `json:"args"`
	Type        typeRef    `json:"type"`
}

type gqlInput struct {
	Name         string  `json:"name"`
	Description  string  `json:"description"`
	Type         typeRef `json:"type"`
	DefaultValue *string `json:"defaultValue"`
}

type gqlEnum struct {
	Name string `json:"name"`
}

type typeRef struct {
	Kind   string   `json:"kind"`
	Name   string   `json:"name"`
	OfType *typeRef `json:"ofType"`
}

// parse reads both shapes an introspection result arrives in: the raw
// `{"__schema": ...}` and the `{"data": {"__schema": ...}}` a GraphQL response
// wraps it in. Saving what curl printed is the common way to obtain one, so
// refusing the wrapper would refuse the usual file.
func parse(data []byte) (*schemaDoc, error) {
	var envelope struct {
		Data *struct {
			Schema *schemaDoc `json:"__schema"`
		} `json:"data"`
		Schema *schemaDoc `json:"__schema"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	if envelope.Schema != nil {
		return envelope.Schema, nil
	}
	if envelope.Data != nil {
		return envelope.Data.Schema, nil
	}
	return nil, nil
}
