# Vercel KV: SETEX and EXPIRE run their whole length (2026-11)

A `SETEX` or `EXPIRE` of N seconds now keeps the key for N seconds from the
command.

Before, `vercel.kv` kept and compared a key's deadline in whole clock seconds,
so the part of the second already spent when the command arrived was taken off
the lifetime. `SETEX k 1 v` sent at 12:00:00.990 was a miss at 12:00:01.000,
ten milliseconds later. Every lifetime was short by up to a second.

- **SETEX**, **EXPIRE**: the deadline is the command's instant plus N seconds.
- **GET**, **EXISTS**, **STRLEN**, **DEL**, **TTL**, **APPEND** and the
  **INCR** family see the key as live until that instant and a miss from it.
- **TTL** answers the time left rounded to the nearest second, which is what
  Redis does (`(ttl_ms + 500) / 1000`). Asked on a second boundary of the
  key's lifetime it answers what it did before; in between it can differ by
  one, and a live key in its last half second answers `0`.

## EXPIRE with zero or negative seconds deletes the key

Redis documents `EXPIRE`: "The timeout can also be specified with a
non-positive value, in which case the key is deleted", and the reply is `1`,
as for any `EXPIRE` that found the key (`0` when there was none). Before,
`EXPIRE k 0` and `EXPIRE k -1` answered 400 `ERR invalid expire time in
expire` and left the key. `SETEX` with a TTL that is not positive is still
that error, as in Redis.

## SETEX and EXPIRE read their seconds as Redis reads an integer

Redis parses the seconds with `string2ll`: a signed 64-bit integer in its one
canonical spelling. Answers that change:

- **Not that spelling is `ERR value is not an integer or out of range`.** A
  leading zero (`007`), a plus sign (`+5`), `-0`, a fraction, a space or a
  word. Before, `007` was accepted as 7 and the rest answered `ERR invalid
  expire time in setex` / `in expire`; a number past 64 bits answered that
  too.
- **Sixteen digits and more are accepted when they fit.** The seconds are
  refused as `ERR invalid expire time in setex` / `in expire` only when they
  cannot be turned into milliseconds and added to the clock, as in Redis.
  Before, every number of sixteen digits or more was refused.

## A malformed command is a 400

The command array is the client's, and an element of it may be any JSON
value. Answers that change, all of which were a 500 `InternalFailure` or an
evaluation error before unless said otherwise:

- **A null, list or object where a word belongs** is 400 `redis command
  arguments must be strings or numbers`: as the verb, the key, a value, or a
  trailing argument. This message is this emulator's own; no reading of
  Upstash's answer for it has been taken.
- **Where seconds or an increment belong** (`SETEX`, `EXPIRE`, `INCRBY`,
  `DECRBY`), it is `ERR value is not an integer or out of range`, as any text
  that is not an integer is.
- **`INCRBY` / `DECRBY` read their number as Redis reads an integer**, the
  same spelling as the seconds above. `INCRBY n soon` was accepted and counted
  as zero, and `INCRBY n 007` as seven; both are now `ERR value is not an
  integer or out of range`. So is counting on a stored value in another
  spelling (`007`), which was counted with before.
- **Counting past 64 bits** is `ERR increment or decrement would overflow`,
  and `DECRBY` of the most negative number `ERR decrement would overflow`, as
  in Redis.

## An argument sent as a JSON number is its decimal digits

A client may send a command argument as a JSON number, and `@upstash/redis`
does: `incrby("n", 1000000)`, `expire("k", 2592000)`. Redis is sent the
number's text. A whole number up to 2^53 either way is now spelled in digits
wherever it is read: as a key, a value, seconds or an increment.

Before, a number from a million up was spelled in exponent form (`1e+06`):
`SET n 1000000` then `GET n` answered `1e+06`, `INCRBY n 1000000` added
nothing, and `EXPIRE k 2592000` answered `ERR invalid expire time in expire`.

A fraction keeps its own spelling (`1.5`) and is not an integer. A number past
2^53 is decoded to a float before the bundle sees it, so its digits are not
all there to spell; it keeps the float's spelling and is not read as an
integer. Sent as a string it is exact.
