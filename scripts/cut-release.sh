#!/usr/bin/env bash
# Decide whether RELEASE asks for a release that does not exist yet and, if it
# does, create its tag.
#
#   GH_REPO=owner/name GH_TOKEN=... scripts/cut-release.sh
#
# Run in a checkout of the commit CI has just passed on main
# (.github/workflows/cut-release.yml). Writes `tag=<tag>` to $GITHUB_OUTPUT, empty
# when there is nothing to release, and creates the tag only when all of these hold:
#
#   - RELEASE names exactly one version, and it is a tag release-archive.sh accepts;
#   - the checked-out commit is still the tip of main, so a CI run that finished
#     late does not tag a commit main has already moved past (the newest commit's
#     run decides);
#   - that tag does not exist. A tag is never moved or recreated, so every later
#     run of CI on main, which still reads the same RELEASE, finds the tag and does
#     nothing.
#
# A tag made with the workflow token starts no workflow of its own, so
# cut-release.yml calls release.yml with it.
set -euo pipefail

TAG_RE='^v[0-9][0-9A-Za-z._-]*$' # the same as scripts/release-archive.sh
out=${GITHUB_OUTPUT:-/dev/stdout}

say() { echo "cut-release: $*" >&2; }
none() { say "$1"; echo "tag=" >> "$out"; exit 0; }

sha=$(git rev-parse HEAD)

# The lines of RELEASE that are not blank or comments.
lines=$(sed -e 's/#.*//' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' RELEASE 2> /dev/null | grep -v '^$' || true)
[ -n "$lines" ] || none "RELEASE names no version"
if [ "$(wc -l <<< "$lines")" -ne 1 ]; then
  echo "::error::RELEASE names more than one version: $(tr '\n' ' ' <<< "$lines")"
  exit 1
fi
tag=$lines
if ! [[ $tag =~ $TAG_RE ]]; then
  echo "::error::RELEASE names '$tag', which is not a version tag ($TAG_RE)"
  exit 1
fi

main=$(git ls-remote origin refs/heads/main | cut -f1)
[ "$main" = "$sha" ] || none "main is at ${main:-nothing}, not $sha; the newest commit's CI decides"

if [ -n "$(git ls-remote --tags origin "refs/tags/$tag")" ]; then
  none "$tag already exists, and a tag is never moved"
fi

obj=$(gh api "repos/$GH_REPO/git/tags" -f tag="$tag" -f message="$tag" -f object="$sha" -f type=commit --jq .sha)
gh api "repos/$GH_REPO/git/refs" -f ref="refs/tags/$tag" -f sha="$obj" > /dev/null
say "created $tag at $sha"
echo "tag=$tag" >> "$out"
