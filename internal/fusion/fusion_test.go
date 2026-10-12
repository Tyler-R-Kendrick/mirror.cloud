package fusion

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestFusePrecedence(t *testing.T) {
	a := []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Confidence: model.ConfDeclared, Source: model.SourceRef{Path: "a"}}},
		Shapes:     map[string]model.Shape{"In": {ID: "In"}},
	}}
	b := []model.Service{{
		ID:         "aws.demo",
		Operations: []model.Operation{{Name: "Ping", Confidence: model.ConfVerified, Source: model.SourceRef{Path: "b"}}, {Name: "Pong", Confidence: model.ConfDeclared}},
	}}
	out, _, err := Fuse(context.Background(), model.ProviderAWS, [][]model.Service{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Services) != 1 {
		t.Fatalf("%d services", len(out.Services))
	}
	var ping model.Operation
	for _, op := range out.Services[0].Operations {
		if op.Name == "Ping" {
			ping = op
		}
	}
	if ping.Confidence != model.ConfVerified {
		t.Fatalf("want verified, got %s", ping.Confidence)
	}
}

func TestFuseFoldsRouteAliasesOntoTheirOperations(t *testing.T) {
	doc := []model.Service{{
		ID: "vendor.api",
		Operations: []model.Operation{
			{Name: "CreateThing", Confidence: model.ConfDeclared, HTTP: model.HTTPBinding{Method: "POST", URI: "/v11/things"}},
			{Name: "GetThing", Confidence: model.ConfDeclared, HTTP: model.HTTPBinding{Method: "GET", URI: "/v9/things/{idOrName}"}},
		},
	}}
	extra := func(aliases ...model.RouteAlias) []model.Service {
		return []model.Service{{ID: "vendor.api", RouteAliases: aliases}}
	}
	fuse := func(in ...[]model.Service) (model.Bundle, error) {
		b, _, err := Fuse(context.Background(), model.Provider("vendor"), in)
		return b, err
	}

	// The supplement may come first or last, and the operation keeps the
	// document's address as its own.
	for _, order := range [][]int{{0, 1}, {1, 0}} {
		parts := [][]model.Service{doc, extra(
			model.RouteAlias{Operation: "CreateThing", URI: "/v9/things"},
			model.RouteAlias{Operation: "CreateThing", URI: "/v10/things"},
			model.RouteAlias{Operation: "GetThing", URI: "/v9/things/{idOrName}"}, // the document's own address, repeated
		)}
		b, err := fuse(parts[order[0]], parts[order[1]])
		if err != nil {
			t.Fatal(err)
		}
		svc := b.Services[0]
		if svc.RouteAliases != nil {
			t.Fatalf("aliases survived fusion: %v", svc.RouteAliases)
		}
		got := svc.OperationByName("CreateThing").HTTP
		if got.URI != "/v11/things" || len(got.AltURIs) != 2 || got.AltURIs[0] != "/v10/things" || got.AltURIs[1] != "/v9/things" {
			t.Fatalf("CreateThing bound as %+v", got)
		}
		if alts := svc.OperationByName("GetThing").HTTP.AltURIs; len(alts) != 0 {
			t.Fatalf("an alias repeating the document's address was recorded: %v", alts)
		}
	}

	if _, err := fuse(doc, extra(model.RouteAlias{Operation: "NoSuchOp", URI: "/v9/x"})); err == nil {
		t.Error("an alias naming no operation was accepted")
	}
	// Two operations at one address is a guess the router would make by name.
	if _, err := fuse(doc, extra(model.RouteAlias{Operation: "CreateThing", URI: "/v11/things"}, model.RouteAlias{Operation: "GetThing", URI: "/v11/things"})); err != nil {
		t.Errorf("same address, different methods is not a collision: %v", err)
	}
	collide := [][]model.Service{{{
		ID: "vendor.api",
		Operations: []model.Operation{
			{Name: "A", Confidence: model.ConfDeclared, HTTP: model.HTTPBinding{Method: "GET", URI: "/v1/x/{a}"}},
			{Name: "B", Confidence: model.ConfDeclared, HTTP: model.HTTPBinding{Method: "GET", URI: "/v2/x/{b}"}},
		},
	}}, extra(model.RouteAlias{Operation: "B", URI: "/v1/x/{b}"})}
	if _, err := fuse(collide...); err == nil {
		t.Error("an alias landing on another operation's address was accepted")
	}
}
