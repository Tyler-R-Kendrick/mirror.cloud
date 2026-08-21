// Package discovery ingests Google API Discovery documents.
package discovery

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver"
)

// Receiver implements receiver.Receiver for Google Discovery JSON.
type Receiver struct{}

func (Receiver) Name() string { return "discovery" }

func (Receiver) Detect(_ string, head []byte) bool {
	s := string(head)
	return strings.Contains(s, `"discoveryVersion"`) || strings.Contains(s, `"resources"`)
}

type document struct {
	Name      string              `json:"name"`
	Version   string              `json:"version"`
	Title     string              `json:"title"`
	BasePath  string              `json:"basePath"`
	RootURL   string              `json:"rootUrl"`
	Resources map[string]resource `json:"resources"`
	Schemas   map[string]schema   `json:"schemas"`
}

type resource struct {
	Methods   map[string]method   `json:"methods"`
	Resources map[string]resource `json:"resources"`
}

type method struct {
	ID         string `json:"id"`
	HTTPMethod string `json:"httpMethod"`
	Path       string `json:"path"`
	FlatPath   string `json:"flatPath"`
}

type schema struct {
	Type       string            `json:"type"`
	Properties map[string]schema `json:"properties"`
	Items      *schema           `json:"items"`
}

// Ingest parses one Discovery document.
func (Receiver) Ingest(ctx context.Context, src model.SourceRef, data []byte) ([]model.Service, error) {
	_ = ctx
	var doc document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	id := "gcp." + strings.ToLower(doc.Name)
	if doc.Name == "storage" {
		id = "gcp.storage"
	}
	svc := model.Service{
		ID:             id,
		Namespace:      doc.Name,
		Protocol:       model.ProtoGCPRESTSON,
		EndpointPrefix: doc.Name,
		Shapes:         map[string]model.Shape{},
		Source:         src,
	}
	var walk func(resource)
	walk = func(r resource) {
		for _, m := range r.Methods {
			name := m.ID
			if name == "" {
				continue
			}
			svc.Operations = append(svc.Operations, model.Operation{
				Name:       name,
				HTTP:       model.HTTPBinding{Method: m.HTTPMethod, URI: "/" + strings.TrimPrefix(m.Path, "/"), Code: 200},
				Confidence: model.ConfDeclared,
				Source:     src,
			})
		}
		for _, child := range r.Resources {
			walk(child)
		}
	}
	for _, r := range doc.Resources {
		walk(r)
	}
	for sid, sch := range doc.Schemas {
		svc.Shapes[sid] = toShape(sid, sch)
	}
	return []model.Service{svc}, nil
}

func toShape(id string, s schema) model.Shape {
	sh := model.Shape{ID: id, Kind: kind(s.Type), Members: map[string]model.Member{}}
	for n, p := range s.Properties {
		sh.Members[n] = model.Member{Shape: n}
		_ = p
	}
	if s.Items != nil {
		sh.Member = id + ".items"
	}
	return sh
}

func kind(t string) model.ShapeKind {
	switch t {
	case "object":
		return model.KindStructure
	case "array":
		return model.KindList
	case "integer":
		return model.KindInteger
	case "number":
		return model.KindDouble
	case "boolean":
		return model.KindBoolean
	default:
		return model.KindString
	}
}

var _ receiver.Receiver = Receiver{}
