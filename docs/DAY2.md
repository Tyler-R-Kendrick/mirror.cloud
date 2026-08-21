# Day-2 operations

## Spec updates

```bash
make specs-sync    # refresh pinned vendor trees into specs/
make generate      # regenerate protocol packages; must be byte-idempotent
mirror spec diff   # API-surface delta
```

`mirror spec add <service>` appends to `specs/mirror.set` and you regenerate. That is how service #31 arrives without a feature request.

A snapshot taken under one spec-lock hash refuses to restore under a different one.

## Snapshots

```bash
mirror up --persist .mirror/persist s3
mirror snapshot save --name golden
mirror snapshot load --name golden --persist .mirror/persist
```

Named snapshots are per-directory files under `.mirror/snapshots/`. Account isolation is the existing account+region namespace: load the same fixture, then call with a distinct `X-Mirror-Account-Id` or access key.

## Drift

Record real traffic with proxy mode against a stand-in (or, explicitly enabled, a real endpoint), then:

```bash
mirror drift --emulated /tmp/emu.json --recorded /tmp/rec.json
```

## Upgrades

Rebuild the static binary. Persist dir is a tar of store+blobs+lock; restore fails closed on lock mismatch so you cannot silently serve a new spec against old state.
