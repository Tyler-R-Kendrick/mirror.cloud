# Releasing

How a tag becomes downloadable binaries, and how to check one you downloaded.
The build is [`scripts/release-archive.sh`](../scripts/release-archive.sh), run by
[`.github/workflows/release.yml`](../.github/workflows/release.yml).

## From a tag to assets

```sh
git tag -s v0.1.0 && git push origin v0.1.0
```

A pushed tag matching `v*` starts the `release` workflow. Use
`vMAJOR.MINOR.PATCH`, optionally with a `-suffix`; the script refuses a tag that
is not `v` and a digit followed by letters, digits, `.`, `_` and `-` before it
builds anything (no `+`: GitHub may rename an asset holding one, and the name in
`SHA256SUMS` would no longer match). A tag with a hyphen (`v0.1.0-rc1`)
publishes a prerelease. Tag a commit that is already on `main`: the workflow
stops before publishing if the tag's commit is not an ancestor of `main`.

1. `build`: one job per platform runs `scripts/release-archive.sh archive`, with
   `SOURCE_DATE_EPOCH` set to the tagged commit's time and Go pinned to an exact
   version (`GO_VERSION` in the workflow).
2. `checksums`: checks that the tagged commit is on `main`, then
   `scripts/release-archive.sh sums` writes one `SHA256SUMS`.
3. `publish`: the only job with `contents: write`. It creates a draft release
   holding the four archives and `SHA256SUMS`, then publishes it, each step
   only while the tag still points at the commit `checksums` verified (a tag
   moved in between is refused). No asset is
   ever overwritten; if the job fails, delete the draft release and re-run it.

| Asset | Platform |
|---|---|
| `mirror_<tag>_linux_amd64.tar.gz` | Linux, x86-64 |
| `mirror_<tag>_linux_arm64.tar.gz` | Linux, arm64 |
| `mirror_<tag>_darwin_arm64.tar.gz` | macOS, Apple silicon |
| `mirror_<tag>_windows_amd64.zip` | Windows, x86-64 |
| `SHA256SUMS` | checksums of the four archives |

Each archive holds exactly two files: `LICENSE` and the binary (`mirror`, or
`mirror.exe`). `mirror version` prints the tag without its `v` (`0.1.0`), the
form it printed before there were releases (`0.1.0-dev`).

To try the workflow without releasing, run it by hand (Actions, release, Run
workflow, choose the branch or tag in "Use workflow from") and give a label such
as `v0.0.0-dryrun`. It builds the selected commit under that label and keeps the
archives and `SHA256SUMS` as artifacts of the run; no release is made and no tag
has to exist.

## SHA256SUMS

The output of `sha256sum`, one line per archive, sorted by file name (bytewise),
nothing else in the file:

```
<64 hex digits><two spaces><file name>
```

## Verifying a download

```sh
tag=v0.1.0
base=https://github.com/tyler-r-kendrick/mirror.cloud/releases/download/$tag
curl -fsSLO "$base/mirror_${tag}_linux_amd64.tar.gz"
curl -fsSLO "$base/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS       # macOS: shasum -a 256 -c
tar -xzf "mirror_${tag}_linux_amd64.tar.gz" mirror
./mirror version                                    # 0.1.0
```

`SHA256SUMS` comes from the same place as the archive, so it catches a damaged
download, not a replaced release. A repository that depends on a binary should
keep the archive's hash in its own source, taken from a release it reviewed, and
check that instead:

```sh
echo "<sha256 of the archive>  mirror.tar.gz" | sha256sum --check -
```

## Reproducing a build

On a Linux machine with GNU tar, gzip and Info-ZIP zip, and the same Go version
as `GO_VERSION`:

```sh
git checkout v0.1.0
SOURCE_DATE_EPOCH="$(git log -1 --format=%ct)" \
  scripts/release-archive.sh archive v0.1.0 linux amd64 out
```

prints the archive's sha256 and writes it to `out/`. What goes into the bytes:

- the binary is built with `CGO_ENABLED=0 -trimpath -buildvcs=false -s -w` and
  the version set by the linker, so it holds no build path, no checkout state
  and no clock; `GOENV=off`, unset `GOFLAGS`, `GOEXPERIMENT`, `GOFIPS140`,
  `GODEBUG` and the other per-architecture levels, `GOAMD64=v1` and
  `GOARM64=v8.0` keep your Go settings out of it;
- the tar is ustar, members sorted by name, every mtime `SOURCE_DATE_EPOCH`,
  owner and group `0` without names, modes `0755` and `0644`; the gzip header
  has no name or time; the zip has no extra attributes and the same mtime.

`test/release` builds the host platform's archive twice on every CI run, the
second time with an empty build cache (so the compile is repeated, not replayed)
and with `GOFLAGS`, `GOAMD64`, `GOARM64`, `GOEXPERIMENT`, `GOFIPS140` and a `go env` file set
to values that would change the binary, and checks that the hashes match, that
the archive holds exactly the binary and `LICENSE`, and that the binary reports
the tag. That shows the build is repeatable on one machine. It has not been compared against a hosted runner's
output; until it is, compare the extracted binary's hash rather than the
archive's when checking a release against a local build, because the gzip and
zip bytes also depend on the tools' versions.
