# Governance

mirror.cloud is Apache-2.0. The covenants in [`COVENANT.md`](./COVENANT.md) are product requirements, not paperwork.

## Scope covenant

The protocol layer is generated for every service in the vendored spec set. Behavior is layered only where declared as `emulate` tier. Everything else is `mock` (schema-valid, deterministic, labeled) or `proxy` (off by default).

## Gating covenant

No capability is gated. No account. No token. No license check.

## Changes

A change that would violate a covenant is out of scope, regardless of demand. Record disagreements between this document and a vendored spec in `docs/INTERFACE_NOTES.md`; the vendored spec wins for wire behavior.
