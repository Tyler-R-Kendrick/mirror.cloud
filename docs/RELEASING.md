# Releasing

How a tag becomes downloadable binaries, and how to check one you downloaded.
The build is [`scripts/release-archive.sh`](../scripts/release-archive.sh), run by
[`.github/workflows/release.yml`](../.github/workflows/release.yml).

## Before the first tag

The workflow checks that a tag still names the commit it built, once before
the draft release is made and again just before it is published. That narrows
the window in which someone with write access could move the tag, but cannot
close it, and nothing in a workflow can stop a tag being moved or deleted
after the release is out. So before tags are trusted, a repository
administrator must turn on one of these in the repository's settings:

- a tag ruleset targeting `refs/tags/v*` that blocks updates and deletions
  (Settings, Rules, Rulesets, New tag ruleset); or
- immutable releases, which lock a published release's tag and assets.

This is a setting of the repository, not something the code can enforce.

## Cutting a release

Two ways, and they end in the same place.

**By merging, which is the usual way.** Nothing is edited to release. When `ci`
passes on a commit that merged to `main`, [`cut-release.yml`](../.github/workflows/cut-release.yml)
reads the commit messages since the last tag (a squash-merged pull request's title
is one) as [Conventional Commits](https://www.conventionalcommits.org/) and picks
the next version with [`scripts/cut-release.sh`](../scripts/cut-release.sh):

| Since the last tag | Next version |
|---|---|
| `feat!:` or `fix(x)!:` (a `!` before the colon), or a `BREAKING CHANGE:` line | major (minor while the major is 0) |
| `feat:` | minor (patch while the major is 0) |
| `fix:` or `perf:` | patch |
| only `docs:`, `ci:`, `chore:`, `test:`, `refactor:`... | no release |

After a prerelease tag the next version is the next of its series, whatever the
commits: `v0.0.1-alpha.1` is followed by `v0.0.1-alpha.2`, `v1.0.0-rc9` by `-rc10`.
A prerelease is left by naming the version, with a `Release-As: 0.1.0` line (the
`v` is optional) in the body of a commit message, which wins over the table; the
newest such line since the last tag is used. A commit that matches nothing above
releases nothing, which is not an error, so a pull request title that is not
Conventional is simply not released until a later one is.

It then creates the tag on that commit and calls `release.yml`. It does nothing when
CI failed, when there is no commit since the last tag, when the tag it works out
already exists, or when `main` has moved on since that commit (the newest commit's
run decides, and its range includes every commit since the last tag, so a commit
whose own run was skipped is not lost). It never moves or recreates a tag, and
refuses a `Release-As` that is not a tag of the form below. A release that fails
part-way is retried by re-running the failed jobs of that `cut-release` run in the
Actions tab: the tag is already there, so it is the release that runs again.

**By tag.** A maintainer pushes the tag, as below. A tag made by hand is built and
promoted exactly as one made by merging.

### Who can release

A tag is made by pushing it, which needs write access to the repository, and the
tag ruleset above is what protects it. The merge path moves that decision: the tag
is made by the workflow, so *anyone who can merge to `main` can cause a release*,
by merging a `feat:` or `fix:`, or with a `Release-As:` line. Two things narrow it,
and both are settings of the repository that the code cannot turn on for you:

- **Who may change how a release is made.** [`.github/CODEOWNERS`](../.github/CODEOWNERS)
  names an owner for the workflows, scripts and `Dockerfile.release` that make,
  build and publish a release, and for `profiles/`, which decides what the `core`
  images hold. It is enforced only where the branch protection rule or ruleset for
  `main` has *Require review from Code Owners* on (Settings, Branches or Rules); without
  it, it is a list of who is asked.
- **Who may let a build out.** A release is promoted through three GitHub
  environments, `edge`, `staging` and `production`, which exist from the first run
  and have no rules. Under Settings, Environments, put *Required reviewers* (or a
  wait timer) on `production` and every release waits there for approval, after it
  has been built and checked and before anything is published. That is the control
  that stands between a merge and a release; the CODEOWNERS file does not.

If neither is set, a merge releases by itself with nothing to stop it, which is
what "automatic" means here. The tag ruleset must allow the Actions bot to create
tags, or the `plan` job fails when it tries, which shows as a red `cut-release` run
and releases nothing.

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

Everything is built once, and then moved through the environments rather than
rebuilt:

1. `build`: one job per platform runs `scripts/release-archive.sh archive`, with
   `SOURCE_DATE_EPOCH` set to the tagged commit's time and Go pinned to an exact
   version (`GO_VERSION` in the workflow).
2. `checksums`: checks that the tagged commit is on `main`, then
   `scripts/release-archive.sh sums` writes one `SHA256SUMS`.
3. `edge`: builds the two container images from those archives, runs them, and
   pushes them under a candidate tag, `sha-<12 digits of the commit>`, which nothing
   follows. See "Container images".
4. `staging`: pulls both candidates back by digest and checks them again
   (`scripts/image-check.sh`): version, services, and that the binary in the image
   is the archive's.
5. `production`: does nothing but wait for the environment's reviewers, if it has
   any.
6. `promote` and `publish`, in parallel; neither builds anything. `promote` gives
   the candidates' digests their version tags (and `latest` and `core` unless the
   tag has a hyphen). `publish` is the only job with `contents: write`. It creates a draft release
   holding the four archives and `SHA256SUMS`, then publishes it, each step
   only while the tag still points at the commit `checksums` verified (a tag
   moved in between is refused). No asset is
   ever overwritten; if the job fails, delete the draft release and re-run it.

A dry run stops after `edge`, before anything is pushed.

| Asset | Platform |
|---|---|
| `mirror_<tag>_linux_amd64.tar.gz` | Linux, x86-64 |
| `mirror_<tag>_linux_arm64.tar.gz` | Linux, arm64 |
| `mirror_<tag>_darwin_arm64.tar.gz` | macOS, Apple silicon |
| `mirror_<tag>_windows_amd64.zip` | Windows, x86-64 |
| `mirror_<tag>_<platform>_core.tar.gz` (`.zip` on Windows) | the same, with only the services in [`profiles/core.txt`](../profiles/core.txt) |
| `SHA256SUMS` | checksums of all eight archives |

## The `core` archives and smaller builds of your own

The full binary embeds the models and behavior of every service (7.5 MB of its
29.5 MB) and decodes them all at start. A `_core` archive is the same build with
only the services in `profiles/core.txt`, measured on linux/amd64:

| | full | `core` (12 services) |
|---|---|---|
| download (`.tar.gz`) | 14.1 MB | 7.8 MB |
| binary | 29.5 MB | 20.8 MB |
| `mirror up` until it listens | 4.6 s | 0.8 s |
| resident memory after start | 415 MB | 59 MB |

For a different set, build it: `make build-slim SERVICES="aws.s3 aws.sqs"`
(or `SERVICES=@your-list.txt`), or
`docker build --build-arg SERVICES="aws.s3 aws.sqs" .`. Both run
`scripts/slim-overlay.py`, which hides the other services from the build without
changing a source file. A service left out answers nothing, and a call from a
kept service into one that was left out (S3 notifying a queue, SNS invoking a
function) fails at that call and names it. `mirror services` lists what a binary
serves. A release builds a profile with `MIRROR_PROFILE=core`; the archive is
reproducible like the others.

Each archive holds exactly two files: `LICENSE` and the binary (`mirror`, or
`mirror.exe`). `mirror version` prints the tag without its `v` (`0.1.0`), the
form it printed before there were releases (`0.1.0-dev`).

To try the workflow without releasing, run it by hand (Actions, release, Run
workflow, choose the branch or tag in "Use workflow from") and give a label such
as `v0.0.0-dryrun`. It builds the selected commit under that label and keeps the
archives and `SHA256SUMS` as artifacts of the run; no release is made and no tag
has to exist.

## Container images

A tag also publishes two multi-arch (linux/amd64, linux/arm64) images to the
GitHub Container Registry, built from the archives above and not from source, so
each holds the binary `SHA256SUMS` covers:

| Image | Holds |
|---|---|
| `ghcr.io/<owner>/mirror:<version>`, `:latest` | every service |
| `ghcr.io/<owner>/mirror:<version>-core`, `:core` | the services in `profiles/core.txt` |

`<version>` is the tag without its `v`. A tag with a hyphen (`v0.1.0-rc1`) gets
its version tags and not `latest` or `core`. Both are `FROM scratch` plus the
binary (about 30 MB and 21 MB on disk, half that to pull) and run
`mirror up --bind 0.0.0.0:4566`:

```sh
docker run --rm -p 127.0.0.1:4566:4566 ghcr.io/<owner>/mirror:<version>-core
```

How to run, configure and consume them is [USING.md](USING.md).

`edge` checks the archives against `SHA256SUMS`, then runs what it is about to push
(`scripts/image-check.sh`): the image must report the tag as its version, serve the
services its profile lists (the full image, more than a hundred), and hold the
archive's binary. It pushes under `sha-<commit>` and `sha-<commit>-core`; those tags
are candidates, can be moved by a re-run of the same commit, and are not a release.
`staging` pulls them back by digest and runs the same check. `promote` then adds the
version tags to those digests, so the image released is the image tested, and never
overwrites one: a version already in the registry is refused, and neither image is
tagged if either exists. Run by hand, the workflow does all of `edge` except the
login and the push.

The images carry `org.opencontainers.image.source`, which links the package to
this repository. The packages of the first release (`v0.0.1-alpha.1`) are public:
the registry served them without a login. Whether a new package is public is the
registry's decision and the owner's setting, not this workflow's, so check it after
a release; one that is private needs a repository admin to change its visibility
(the package's settings, "Change visibility").

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
