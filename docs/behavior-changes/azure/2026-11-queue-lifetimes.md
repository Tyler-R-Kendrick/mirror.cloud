# Queue Storage: a TTL or visibility timeout runs its whole length (2026-11)

A message TTL (`messagettl`) or visibility timeout (`visibilitytimeout`) of N
seconds now runs N seconds from the instant the request set it.

Before, `azure.queue` kept and compared these deadlines in whole clock
seconds, so the part of the second already spent when the request arrived was
taken off the lifetime. A message put with `messagettl=1` at 12:00:00.990 was
expired at 12:00:01.000, ten milliseconds later, and a message put with
`visibilitytimeout=1` was dequeueable as early. Every lifetime was short by up
to a second. It showed where a client puts a short-lived message and reads it
straight back.

- **Put Message**: `messagettl` and `visibilitytimeout` count from the put.
- **Get Messages**, **Peek Messages**, **Get Queue Metadata**
  (`x-ms-approximate-messages-count`), **Update Message** and **Delete
  Message** see a message as expired, or as visible again, at that instant and
  not before.
- **Get Messages** and **Update Message**: the visibility timeout they set
  counts from the dequeue or the update.

The reported times (`InsertionTime`, `ExpirationTime`, `TimeNextVisible`,
`x-ms-time-next-visible`) are unchanged: whole seconds, as the service reports
them, with the fraction dropped. A message can therefore outlive its reported
`ExpirationTime` by under a second, never fall short of its TTL.
