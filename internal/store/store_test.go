package store_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/store"
)

func TestMemoryPutGetListTxn(t *testing.T) {
	ctx := context.Background()
	c := store.NewMemory("lock").Scope("111111111111", "us-east-1").Collection("items")

	if _, ok, err := c.Get(ctx, "k"); err != nil || ok {
		t.Fatalf("empty get: ok=%v err=%v", ok, err)
	}
	if err := c.Put(ctx, "alpha", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "beta", []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "gamma", []byte("g")); err != nil {
		t.Fatal(err)
	}

	got, ok, err := c.Get(ctx, "beta")
	if err != nil || !ok || string(got) != "b" {
		t.Fatalf("get beta: ok=%v val=%q err=%v", ok, got, err)
	}
	got[0] = 'Z'
	got2, ok, err := c.Get(ctx, "beta")
	if err != nil || !ok || string(got2) != "b" {
		t.Fatalf("get copies: %q", got2)
	}

	kvs, more, err := c.List(ctx, "", "", 0)
	if err != nil || more || len(kvs) != 3 {
		t.Fatalf("list all: n=%d more=%v err=%v", len(kvs), more, err)
	}
	if kvs[0].Key != "alpha" || kvs[1].Key != "beta" || kvs[2].Key != "gamma" {
		t.Fatalf("list order: %+v", kvs)
	}

	kvs, more, err = c.List(ctx, "", "alpha", 1)
	if err != nil || !more || len(kvs) != 1 || kvs[0].Key != "beta" {
		t.Fatalf("list after/limit: %+v more=%v err=%v", kvs, more, err)
	}

	if err := c.Put(ctx, "pref/1", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "pref/2", []byte("2")); err != nil {
		t.Fatal(err)
	}
	kvs, _, err = c.List(ctx, "pref/", "", 0)
	if err != nil || len(kvs) != 2 {
		t.Fatalf("list prefix: n=%d err=%v", len(kvs), err)
	}

	if err := c.Txn(ctx, func(tx spi.Tx) error {
		if _, ok, err := tx.Get("missing"); err != nil || ok {
			t.Fatalf("txn get missing: ok=%v err=%v", ok, err)
		}
		if err := tx.Put("txn", []byte("in")); err != nil {
			return err
		}
		if err := tx.Put("drop", []byte("x")); err != nil {
			return err
		}
		if err := tx.Delete("drop"); err != nil {
			return err
		}
		v, ok, err := tx.Get("txn")
		if err != nil || !ok || string(v) != "in" {
			t.Fatalf("txn get: ok=%v val=%q err=%v", ok, v, err)
		}
		listed, _, err := tx.List("txn", "", 0)
		if err != nil || len(listed) != 1 || listed[0].Key != "txn" {
			t.Fatalf("txn list: %+v err=%v", listed, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	v, ok, err := c.Get(ctx, "txn")
	if err != nil || !ok || string(v) != "in" {
		t.Fatalf("after txn: ok=%v val=%q err=%v", ok, v, err)
	}
	if _, ok, err := c.Get(ctx, "drop"); err != nil || ok {
		t.Fatalf("txn delete did not stick: ok=%v err=%v", ok, err)
	}
}

func TestTwoAccountIsolation(t *testing.T) {
	ctx := context.Background()
	m := store.NewMemory("lock")
	a := m.Scope("111111111111", "us-east-1").Collection("buckets")
	b := m.Scope("222222222222", "us-east-1").Collection("buckets")
	if err := a.Put(ctx, "shared", []byte("acct-a")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, "shared", []byte("acct-b")); err != nil {
		t.Fatal(err)
	}
	va, ok, err := a.Get(ctx, "shared")
	if err != nil || !ok || string(va) != "acct-a" {
		t.Fatalf("account a: ok=%v val=%q err=%v", ok, va, err)
	}
	vb, ok, err := b.Get(ctx, "shared")
	if err != nil || !ok || string(vb) != "acct-b" {
		t.Fatalf("account b: ok=%v val=%q err=%v", ok, vb, err)
	}
	kvs, _, err := a.List(ctx, "", "", 0)
	if err != nil || len(kvs) != 1 || string(kvs[0].Value) != "acct-a" {
		t.Fatalf("account a list: %+v err=%v", kvs, err)
	}
}

func TestSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	src := store.NewMemory("abc")
	c := src.Scope("1", "r").Collection("c")
	if err := c.Put(ctx, "k", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	snap, err := store.SnapshotBytes(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "k", []byte("changed")); err != nil {
		t.Fatal(err)
	}
	if err := src.Restore(ctx, bytes.NewReader(snap)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || string(got) != "hello" {
		t.Fatalf("restore: ok=%v val=%q err=%v", ok, got, err)
	}

	dst := store.NewMemory("abc")
	if err := dst.Restore(ctx, bytes.NewReader(snap)); err != nil {
		t.Fatal(err)
	}
	got, ok, err = dst.Scope("1", "r").Collection("c").Get(ctx, "k")
	if err != nil || !ok || string(got) != "hello" {
		t.Fatalf("restore into empty: ok=%v val=%q err=%v", ok, got, err)
	}
}

func TestLockSHAMismatchRefuse(t *testing.T) {
	ctx := context.Background()
	src := store.NewMemory("sha-one")
	if err := src.Scope("1", "r").Collection("c").Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	snap, err := store.SnapshotBytes(src)
	if err != nil {
		t.Fatal(err)
	}
	dst := store.NewMemory("sha-two")
	if err := dst.Scope("1", "r").Collection("c").Put(ctx, "k", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	err = dst.Restore(ctx, bytes.NewReader(snap))
	if err == nil {
		t.Fatal("expected lockSHA mismatch")
	}
	msg := err.Error()
	if !strings.Contains(msg, "sha-one") || !strings.Contains(msg, "sha-two") {
		t.Fatalf("error should name both hashes: %s", msg)
	}
	got, ok, err := dst.Scope("1", "r").Collection("c").Get(ctx, "k")
	if err != nil || !ok || string(got) != "keep" {
		t.Fatalf("mismatch must not clobber: ok=%v val=%q err=%v", ok, got, err)
	}
}
