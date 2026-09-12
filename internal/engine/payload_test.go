package engine_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// TestASplattedPayloadDoesNotFailTheRequiredCheck covers the third answer to
// "what is a payload member".
//
// Where the shape is opaque bytes or a bare array the codec sets the member,
// and asking whether it is present is a real question. Where it is a structure
// -- or a union the receiver could not resolve to one -- the codec spreads the
// body's own members across the input instead, so the member is structurally
// never present and requiring it rejects every well-formed request.
//
// Vercel's CreateProjectEnv is the first operation served from a bundle that
// has one, and it is not the last: 111 operations across the generated models
// are shaped that way, including every CloudFront Create and Update, all of
// Pinpoint, and twenty-four of S3's Put. Each would have failed here the
// moment its pack was extracted.
//
// What a body must contain stays checkable -- the bundle says it, in a rule
// that can name the member and the reason, which is what the second case here
// asserts.
func TestASplattedPayloadDoesNotFailTheRequiredCheck(t *testing.T) {
	p, err := bundled.New("vercel.api", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}

	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProject",
		Input: map[string]any{"name": "payload"}})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	project, _ := created.Output["id"].(string)

	// The request carries the body's members and never `body` itself, which is
	// exactly what the REST/JSON codec produces for this shape.
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProjectEnv",
		Input: map[string]any{"idOrName": project, "key": "K", "value": "V"}})
	if err != nil {
		t.Fatalf("create env: %v", err)
	}
	rec, _ := got.Output["created"].(map[string]any)
	if rec["key"] != "K" || rec["value"] != "V" {
		t.Fatalf("created %#v", got.Output)
	}

	// And the bundle still answers for a body that is there but says nothing,
	// which is the check the model no longer makes.
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateProjectEnv",
		Input: map[string]any{"idOrName": project}})
	fault, ok := err.(*spi.Fault)
	if !ok || fault.HTTPStatus != 400 || fault.Code != "bad_request" {
		t.Fatalf("env with no key: %#v", err)
	}
}
