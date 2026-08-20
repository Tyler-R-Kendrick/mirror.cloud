package ssm

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestSecureStringRoundTripAndPath(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	_, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutParameter", Input: map[string]any{"Name": "/app/db", "Value": "secret", "Type": "SecureString"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetParameter", Input: map[string]any{"Name": "/app/db"}})
	if err != nil {
		t.Fatal(err)
	}
	param := got.Output["Parameter"].(map[string]any)
	if param["Value"] != "secret" {
		t.Fatalf("decoded %v", param)
	}
	_, _ = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "PutParameter", Input: map[string]any{"Name": "/app/x", "Value": "1", "Type": "String"}})
	list, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetParametersByPath", Input: map[string]any{"Path": "/app/"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Output["Parameters"].([]any)) != 2 {
		t.Fatalf("%v", list.Output)
	}
}
