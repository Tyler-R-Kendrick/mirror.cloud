// Package spitest ships reference in-memory implementations of every
// spi dependency so behavior packs unit-test without waiting for the
// production store.
package spitest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/blobs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bus"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/journal"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/store"
)

// SeedKMSKey adds a KMS record for cross-service tests that use an explicit key ARN.
func SeedKMSKey(t testing.TB, deps spi.Deps, identity spi.Identity, keyARN, state string) {
	t.Helper()
	keyID := keyARN[strings.LastIndex(keyARN, "/")+1:]
	record, _ := json.Marshal(map[string]any{"KeyId": keyID, "Arn": keyARN, "KeyState": state})
	if err := deps.Store.Scope(identity.Account, identity.Region).Collection("kms").Put(context.Background(), keyID, record); err != nil {
		t.Fatal(err)
	}
}

// Deps returns a ready spi.Deps for tests.
func Deps(t *testing.T) spi.Deps {
	t.Helper()
	return spi.Deps{
		Store:   store.NewMemory("test"),
		Blobs:   blobs.NewMemory(),
		Bus:     bus.New(),
		Clock:   clock.NewControllable(),
		Rand:    rand.New("test"),
		Journal: journal.New(),
		Model:   &model.Bundle{SchemaVersion: "1"},
	}
}
