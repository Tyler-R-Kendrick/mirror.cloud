# Covenant

This is binding project policy, published at founding. It cannot be retrofitted.

1. **No capability will ever require an auth token, an account, or a license key.** Dummy credentials `test`/`test` work. Any credentials work. Signatures are parsed, never verified.
2. **No telemetry, no phone-home, no usage counting.** Diagnostics stay local and user-visible (`/_mirror/journal`, `mirror doctor`).
3. **Scope is spec-complete by generation, behavior-complete only where declared.** `docs/SUPPORT.md` is generated from the model and reports emulate-tier and mock-tier separately. A raw service count that conflates them is a covenant violation.
4. **Any future commercial offering may monetize only hosting, collaboration, and support** — never the runner, never a protocol, never a service.

Shared / hosted deployments must not be exposed to untrusted networks. Isolation is network-level and account+region namespacing. An auth token will never be added to "solve" this.
