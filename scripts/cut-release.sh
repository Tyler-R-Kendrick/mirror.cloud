#!/usr/bin/env bash
# Work out the next version from the commits on main since the last tag and, if
# there is one, create its tag.
#
#   GH_REPO=owner/name GH_TOKEN=... scripts/cut-release.sh
#
# Run in a full checkout (tags included) of the commit CI has just passed on main
# (.github/workflows/cut-release.yml). Writes `tag=<tag>` to $GITHUB_OUTPUT, empty
# when there is nothing to release.
#
# The version comes from the commit subjects (squash-merged PR titles) since the
# nearest tag, as Conventional Commits:
#
#   feat!: / fix(x)!: or a BREAKING CHANGE: line   major (minor while the major is 0)
#   feat:                                          minor (patch while the major is 0)
#   fix: / perf:                                   patch
#   anything else (docs, ci, chore, test, ...)     no release
#
# Next to a prerelease tag (v0.0.1-alpha.1) the next version is the next of its
# series (v0.0.1-alpha.2): a prerelease is left by a `Release-As: 0.1.0` line in a
# commit message, which names the version outright and wins over the rules above.
#
# The tag is created only when the checked-out commit is still the tip of main, so
# a CI run that finished late does not tag a commit main has moved past (the
# newest commit's run decides), and only when the tag does not exist: a tag is
# never moved or recreated, and a second run on the same commit finds nothing
# since its tag and does nothing.
#
# A tag made with the workflow token starts no workflow of its own, so
# cut-release.yml calls release.yml with it.
set -euo pipefail

TAG_RE='^v[0-9][0-9A-Za-z._-]*$' # the same as scripts/release-archive.sh
out=${GITHUB_OUTPUT:-/dev/stdout}

say() { echo "cut-release: $*" >&2; }
none() { say "$1"; echo "tag=" >> "$out"; exit 0; }
fail() { echo "::error::cut-release: $*"; exit 1; }

sha=$(git rev-parse HEAD)
main=$(git ls-remote origin refs/heads/main | cut -f1)
[ "$main" = "$sha" ] || none "main is at ${main:-nothing}, not $sha; the newest commit's CI decides"

if last=$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2> /dev/null); then
  range="$last..HEAD"
else
  last=v0.0.0 range=HEAD
fi
[ "$(git rev-list --count "$range")" -gt 0 ] || none "no commit since $last"

subjects=$(git log --format=%s "$range")
bodies=$(git log --format=%b "$range") # newest first

if as=$(grep -m1 -E '^Release-As:[[:space:]]*v?[0-9]' <<< "$bodies"); then
  tag="v$(sed -E 's/^Release-As:[[:space:]]*v?//; s/[[:space:]]*$//' <<< "$as")"
  [[ $tag =~ $TAG_RE ]] || fail "Release-As names '$tag', which is not a version tag ($TAG_RE)"
else
  if grep -qE '^[a-z]+(\([^)]*\))?!:' <<< "$subjects" || grep -qE '^BREAKING[ -]CHANGE:' <<< "$bodies"; then
    level=major
  elif grep -qE '^feat(\([^)]*\))?:' <<< "$subjects"; then
    level=minor
  elif grep -qE '^(fix|perf)(\([^)]*\))?:' <<< "$subjects"; then
    level=patch
  else
    none "no feat, fix or perf commit since $last"
  fi

  v=${last#v}
  core=${v%%-*}
  [[ $core =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "the last tag, $last, is not vMAJOR.MINOR.PATCH[-pre]; name the next version with a Release-As: line"
  if [ "$v" != "$core" ]; then
    # A prerelease: the next of the same series, counting its last number.
    pre=${v#*-}
    if [[ $pre =~ ^(.*[^0-9])?([0-9]+)$ ]]; then
      tag="v$core-${BASH_REMATCH[1]}$((BASH_REMATCH[2] + 1))"
    else
      tag="v$core-$pre.1"
    fi
  else
    IFS=. read -r major minor patch <<< "$core"
    if [ "$major" = 0 ]; then
      case $level in major) level=minor ;; minor) level=patch ;; esac
    fi
    case $level in
      major) tag="v$((major + 1)).0.0" ;;
      minor) tag="v$major.$((minor + 1)).0" ;;
      patch) tag="v$major.$minor.$((patch + 1))" ;;
    esac
  fi
fi

if [ -n "$(git ls-remote --tags origin "refs/tags/$tag")" ]; then
  none "$tag already exists, and a tag is never moved"
fi

obj=$(gh api "repos/$GH_REPO/git/tags" -f tag="$tag" -f message="$tag" -f object="$sha" -f type=commit --jq .sha)
gh api "repos/$GH_REPO/git/refs" -f ref="refs/tags/$tag" -f sha="$obj" > /dev/null
say "created $tag at $sha, after $last"
echo "tag=$tag" >> "$out"
