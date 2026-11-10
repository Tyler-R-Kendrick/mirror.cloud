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
