package secretsmanager

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestCreateGetDeleteRestore(t *testing.T) {
	p := &Pack{deps: spitest.Deps(t)}
	ctx := context.Background()
	id := spi.Identity{Account: "a", Region: "r"}
	created, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateSecret", Input: map[string]any{"Name": "n", "SecretString": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSecretValue", Input: map[string]any{"SecretId": created.Output["ARN"]}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Output["SecretString"] != "v" {
		t.Fatalf("%v", got.Output)
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "DeleteSecret", Input: map[string]any{"SecretId": "n"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSecretValue", Input: map[string]any{"SecretId": "n"}}); err == nil {
		t.Fatal("expected deleted")
	}
	_, err = p.Invoke(ctx, &spi.Request{Identity: id, Operation: "RestoreSecret", Input: map[string]any{"SecretId": "n"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetSecretValue", Input: map[string]any{"SecretId": "n"}}); err != nil {
		t.Fatal(err)
	}
}
