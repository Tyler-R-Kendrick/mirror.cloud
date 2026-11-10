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

// SETEX and EXPIRE read their seconds as Redis does: a signed 64-bit integer in
// its one canonical spelling. What is not one is "not an integer"; what is one
// and cannot be added to the clock is an invalid expire time.
func TestVercelKVReadsSecondsAsRedisDoes(t *testing.T) {
	cmd := kvPack(t)
	refused := func(message string, argv ...any) {
		t.Helper()
		_, err := cmd(argv...)
		if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 || f.Message != message {
			t.Fatalf("%v: got %#v, want 400 %q", argv, err, message)
		}
	}
	want := func(want string, argv ...any) {
		t.Helper()
		if got, err := cmd(argv...); err != nil || got != want {
			t.Fatalf("%v: got %q, %v; want %q", argv, got, err, want)
		}
	}
	const notInt = "ERR value is not an integer or out of range"
	want("OK", "SET", "k", "v")

	// Not an integer as Redis parses one: a sign it does not take, a leading
	// zero, a minus zero, a fraction, a space, nothing, or past 64 bits.
	for _, seconds := range []string{"soon", "+5", "007", "00", "-0", "-05", "5.5", " 5", "5 ", "", "-", "9223372036854775808", "-9223372036854775809", "99999999999999999999", "-99999999999999999999"} {
		refused(notInt, "SETEX", "k", seconds, "v")
		refused(notInt, "EXPIRE", "k", seconds)
	}
	// An integer that cannot be turned into milliseconds on the clock. The
	// test clock stands at zero, so the last second that fits is 2^63/1000.
	for _, seconds := range []string{"9223372036854775807", "9223372036854776", "1000000000000000000"} {
		refused("ERR invalid expire time in setex", "SETEX", "k", seconds, "v")
		refused("ERR invalid expire time in expire", "EXPIRE", "k", seconds)
	}
	refused("ERR invalid expire time in expire", "EXPIRE", "k", "-9223372036854775808")
	refused("ERR invalid expire time in expire", "EXPIRE", "k", "-9223372036854776")
	for _, seconds := range []string{"0", "-1", "-9223372036854775808"} {
		refused("ERR invalid expire time in setex", "SETEX", "k", seconds, "v")
	}
	want("v", "GET", "k")
	want("-1", "TTL", "k")

	// Sixteen digits are an integer like any other: the largest that fits.
	want("OK", "SETEX", "big", "9223372036854775", "v")
	want("9223372036854775", "TTL", "big")
	want("1", "EXPIRE", "k", "1000000000000000")
	want("1000000000000000", "TTL", "k")
	// A number in the JSON array is its decimal spelling.
	want("1", "EXPIRE", "k", 5.0)
	want("5", "TTL", "k")
	// The most negative seconds that fit delete the key, as any negative does.
	want("1", "EXPIRE", "k", "-9223372036854775")
	want("<nil>", "GET", "k")
}

// What the bundle's quirk note says of SET options: they are ignored, not
// refused. The value is stored and an earlier TTL is cleared.
func TestVercelKVSetIgnoresItsOptions(t *testing.T) {
	cmd := kvPack(t)
	if _, err := cmd("SETEX", "k", "100", "old"); err != nil {
		t.Fatal(err)
	}
	if got, err := cmd("SET", "k", "v", "EX", "10"); err != nil || got != "OK" {
		t.Fatalf("SET with EX: %q, %v", got, err)
	}
	if got, err := cmd("GET", "k"); err != nil || got != "v" {
		t.Fatalf("GET: %q, %v", got, err)
	}
	if got, err := cmd("TTL", "k"); err != nil || got != "-1" {
		t.Fatalf("TTL after SET with EX: %q, %v", got, err)
	}
}

// Whatever a client puts in the command array, the answer is the service's:
// a JSON null, list or object where a word belongs, or a number that cannot
// be counted with, is a 400 and never an evaluation error.
func TestVercelKVAnswersMalformedArguments(t *testing.T) {
	cmd := kvPack(t)
	refused := func(message string, argv ...any) {
		t.Helper()
		_, err := cmd(argv...)
		if f, ok := err.(*spi.Fault); !ok || f.HTTPStatus != 400 || f.Message != message {
			t.Fatalf("%v: got %#v, want 400 %q", argv, err, message)
		}
	}
	want := func(want string, argv ...any) {
		t.Helper()
		if got, err := cmd(argv...); err != nil || got != want {
			t.Fatalf("%v: got %q, %v; want %q", argv, got, err, want)
		}
	}
	const notInt = "ERR value is not an integer or out of range"
	const notScalar = "redis command arguments must be strings or numbers"
	want("OK", "SET", "k", "v")
	want("OK", "SET", "n", "5")

	// Seconds that are no word at all are not an integer.
	for _, seconds := range []any{nil, []any{1.0}, map[string]any{"s": 1.0}, true} {
		refused(notInt, "EXPIRE", "k", seconds)
		refused(notInt, "SETEX", "k", seconds, "v")
	}
	// Nor is such an increment.
	for _, by := range []any{nil, []any{}, map[string]any{}, "007", "+1", "-0", "1.5", "", "9223372036854775808"} {
		refused(notInt, "INCRBY", "n", by)
		refused(notInt, "DECRBY", "n", by)
	}
	// Anywhere else it is a malformed command.
	for _, argv := range [][]any{
		{nil}, {[]any{"GET"}, "k"}, {"GET", nil}, {"SET", nil, "v"}, {"SET", "k", nil}, {"SET", "k", []any{}},
		{"SET", "k", map[string]any{}}, {"APPEND", "k", nil}, {"SETEX", "k", "5", nil}, {"DEL", []any{"k"}},
		{"SET", "k", "v", nil}, {"EXISTS", map[string]any{}}, {"INCR", nil}, {"HSET", "h", nil, "v"},
	} {
		refused(notScalar, argv...)
	}
	want("v", "GET", "k")
	want("-1", "TTL", "k")
	want("5", "GET", "n")

	// Counting past 64 bits is refused, as Redis refuses it, and counts nothing.
	want("OK", "SET", "top", "9223372036854775807")
	refused("ERR increment or decrement would overflow", "INCR", "top")
	refused("ERR increment or decrement would overflow", "INCRBY", "top", "1")
	refused("ERR increment or decrement would overflow", "DECRBY", "top", "-1")
	want("9223372036854775806", "DECR", "top")
	want("OK", "SET", "bottom", "-9223372036854775808")
	refused("ERR increment or decrement would overflow", "DECR", "bottom")
	refused("ERR increment or decrement would overflow", "INCRBY", "bottom", "-1")
	refused("ERR decrement would overflow", "DECRBY", "n", "-9223372036854775808")
	want("-1", "INCRBY", "bottom", "9223372036854775807")
	// A stored value Redis would not count with is not an integer.
	for _, stored := range []string{"007", "9223372036854775808", "-0", "+1"} {
		want("OK", "SET", "odd", stored)
		refused(notInt, "INCR", "odd")
		want(stored, "GET", "odd")
	}
}
