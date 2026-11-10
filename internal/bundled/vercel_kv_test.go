package bundled_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A SETEX or EXPIRE of N seconds lasts N seconds from the command, wherever in
// the clock second the command fell.
func TestVercelKVExpiryIsNotTruncated(t *testing.T) {
	deps := spitest.Deps(t)
	p, err := bundled.New("vercel.kv", deps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	cmd := func(argv ...any) string {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{ServiceID: "vercel.kv", Operation: "Command", Identity: id, Input: map[string]any{"body": argv}})
		if err != nil {
			t.Fatalf("%v: %v", argv, err)
		}
		return fmt.Sprint(res.Output["result"])
	}
	advance := func(d time.Duration) {
		t.Helper()
		if err := deps.Clock.Advance(d); err != nil {
			t.Fatal(err)
		}
	}
	want := func(when, got, want string) {
		t.Helper()
		if got != want {
			t.Fatalf("%s: got %s, want %s", when, got, want)
		}
	}

	// 0.9 s into a second.
	advance(900 * time.Millisecond)
	cmd("SETEX", "a", "1", "v")
	cmd("SET", "b", "5")
	want("EXPIRE", cmd("EXPIRE", "b", "1"), "1")
	want("TTL at once", cmd("TTL", "a"), "1")

	// 0.2 s later the clock second has turned; the keys have 0.8 s left.
	advance(200 * time.Millisecond)
	want("0.2s: GET a", cmd("GET", "a"), "v")
	want("0.2s: EXISTS a", cmd("EXISTS", "a"), "1")
	want("0.2s: TTL a", cmd("TTL", "a"), "1")
	want("0.2s: STRLEN a", cmd("STRLEN", "a"), "1")
	want("0.2s: GET b", cmd("GET", "b"), "5")
	// INCR and APPEND keep the deadline they found.
	want("0.2s: INCR b", cmd("INCR", "b"), "6")
	want("0.2s: APPEND a", cmd("APPEND", "a", "w"), "2")

	advance(799 * time.Millisecond)
	want("0.999s: GET a", cmd("GET", "a"), "vw")
	want("0.999s: GET b", cmd("GET", "b"), "6")
	// Redis rounds a TTL to the nearest second: a millisecond left reads 0.
	want("0.999s: TTL a", cmd("TTL", "a"), "0")

	advance(time.Millisecond)
	want("1s: GET a", cmd("GET", "a"), "<nil>")
	want("1s: EXISTS a", cmd("EXISTS", "a"), "0")
	want("1s: TTL a", cmd("TTL", "a"), "-2")
	want("1s: GET b", cmd("GET", "b"), "<nil>")
	want("1s: INCR b starts over", cmd("INCR", "b"), "1")
	want("1s: TTL b", cmd("TTL", "b"), "-1")

	// An entry written before the millisecond deadline existed (a restored
	// snapshot) keeps its whole-second deadline.
	advance(100 * time.Millisecond) // 2.0 s
	col := deps.Store.Scope(id.Account, id.Region).Collection("vkv")
	cmd("SETEX", "old", "2", "v")
	raw, ok, err := col.Get(ctx, "old")
	if err != nil || !ok {
		t.Fatalf("stored entry: %v %v", ok, err)
	}
	var rec map[string]any
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	if rec["expires_ms"] == nil {
		t.Fatalf("stored entry has no millisecond deadline: %s", raw)
	}
	delete(rec, "expires_ms")
	rec["expires_at"] = 4
	raw, _ = json.Marshal(rec)
	if err := col.Put(ctx, "old", raw); err != nil {
		t.Fatal(err)
	}
	want("old entry: GET", cmd("GET", "old"), "v")
	want("old entry: TTL", cmd("TTL", "old"), "2")
	want("old entry: APPEND", cmd("APPEND", "old", "w"), "2")
	advance(1999 * time.Millisecond)
	want("old entry 1.999s: GET", cmd("GET", "old"), "vw")
	advance(time.Millisecond)
	want("old entry 2s: GET", cmd("GET", "old"), "<nil>")
	// No deadline at all is no TTL.
	cmd("SET", "forever", "v")
	raw, _, _ = col.Get(ctx, "forever")
	rec = map[string]any{}
	_ = json.Unmarshal(raw, &rec)
	delete(rec, "expires_ms")
	rec["expires_at"] = 0
	raw, _ = json.Marshal(rec)
	if err := col.Put(ctx, "forever", raw); err != nil {
		t.Fatal(err)
	}
	advance(time.Hour)
	want("old entry without a TTL: GET", cmd("GET", "forever"), "v")
	want("old entry without a TTL: TTL", cmd("TTL", "forever"), "-1")
}

func kvPack(t *testing.T) func(argv ...any) (string, error) {
	t.Helper()
	p, err := bundled.New("vercel.kv", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	return func(argv ...any) (string, error) {
		res, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "vercel.kv", Operation: "Command", Identity: id, Input: map[string]any{"body": argv}})
		if err != nil {
			return "", err
		}
		return fmt.Sprint(res.Output["result"]), nil
	}
}

// Redis deletes a key whose EXPIRE is zero or in the past and answers 1, as
// for any EXPIRE that found the key.
func TestVercelKVExpireNowDeletesTheKey(t *testing.T) {
	cmd := kvPack(t)
	want := func(want string, argv ...any) {
		t.Helper()
		if got, err := cmd(argv...); err != nil || got != want {
			t.Fatalf("%v: got %q, %v; want %q", argv, got, err, want)
		}
	}
	for _, seconds := range []string{"0", "-1", "-100"} {
		want("OK", "SET", "k", "v")
		want("1", "EXPIRE", "k", seconds)
		want("<nil>", "GET", "k")
		want("0", "EXISTS", "k")
		want("-2", "TTL", "k")
		// Nothing to expire: 0, as for any EXPIRE of a missing key.
		want("0", "EXPIRE", "k", seconds)
	}
	// A key with a TTL goes the same way.
	want("OK", "SETEX", "k", "100", "v")
	want("1", "EXPIRE", "k", "0")
	want("<nil>", "GET", "k")
	// SETEX still refuses a TTL that is not positive, as Redis does.
	for _, seconds := range []string{"0", "-1"} {
		_, err := cmd("SETEX", "k", seconds, "v")
		if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 || f.Message != "ERR invalid expire time in setex" {
			t.Fatalf("SETEX k %s v: %#v", seconds, err)
		}
	}
	// And EXPIRE still refuses what is not a number of seconds.
	want("OK", "SET", "k", "v")
	_, err := cmd("EXPIRE", "k", "soon")
	if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 {
		t.Fatalf("EXPIRE k soon: %#v", err)
	}
	want("v", "GET", "k")
}
