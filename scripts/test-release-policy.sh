#!/usr/bin/env bash
set -euo pipefail

SCRIPT_UNDER_TEST=${1:-scripts/release.sh}
SCRIPT_UNDER_TEST="$(cd "$(dirname "$SCRIPT_UNDER_TEST")" && pwd)/$(basename "$SCRIPT_UNDER_TEST")"
RELEASE_TIMESTAMP=202608131430
ROOT=$(mktemp -d)
REMOTE_ROOT=$(mktemp -d)
MIRROR_ROOT=$(mktemp -d)
trap 'rm -rf "$ROOT" "$REMOTE_ROOT" "$MIRROR_ROOT"' EXIT
mkdir -p "$ROOT/release"

cat > "$ROOT/Cargo.toml" <<'TOML'
[workspace]
members = []

[workspace.package]
version = "0.0.15"
TOML

cat > "$ROOT/release/breaking-changes.md" <<'DOC'
# Compatibility ledger

| Version | Area | Migration |
|---|---|---|
| `Unreleased` | - | No pending compatibility changes <!-- version-contract:unreleased-row --> |
| `0.0.15` | - | No pending compatibility changes |

## Unreleased

<!-- Record implemented but unsealed compatibility changes here. -->

## 0.0.15

<!-- No compatibility changes in this release. -->
DOC

run_release() {
    ZERO_REPO_ROOT="$ROOT" ZERO_RELEASE_TIMESTAMP="$RELEASE_TIMESTAMP" bash "$SCRIPT_UNDER_TEST" "$@"
}

expect_fail() {
    if run_release "$@" >/tmp/zero-release-policy.out 2>&1; then
        echo "Expected failure: $*" >&2
        cat /tmp/zero-release-policy.out >&2
        exit 1
    fi
}

cd "$ROOT"
git init -q
git branch -m develop
git config user.name "Release Policy Test"
git config user.email "release-policy@example.invalid"
git init --bare -q "$REMOTE_ROOT/origin.git"
git init --bare -q "$MIRROR_ROOT/mirror.git"
git remote add origin "$REMOTE_ROOT/origin.git"
git remote add mirror "$MIRROR_ROOT/mirror.git"
git add Cargo.toml release/breaking-changes.md
git commit -qm "initial"
git tag -a v0.0.15 -m v0.0.15
git push -q origin v0.0.15
git push -q mirror v0.0.15

DEVELOP_PREVIEW=$(printf 'n\n' | run_release 0.0.16 2>&1)
[[ "$DEVELOP_PREVIEW" == *"Cargo version: 0.0.15 -> 0.0.16-dev.202608131430"* ]]
[[ "$DEVELOP_PREVIEW" == *"Tag: v0.0.16-dev.202608131430"* ]]
[[ "$DEVELOP_PREVIEW" == *"Remotes: mirror origin"* ]]

[[ "$(run_release --next dev)" == "0.0.16-dev.202608131430" ]]
expect_fail --next rc
grep -Fq "a new release line must publish dev before rc" /tmp/zero-release-policy.out
expect_fail 0.0.16-dev --start-development
expect_fail 0.0.16-dev.10 --start-development
expect_fail 0.0.15 --seal-only
expect_fail 0.0.16-rc.2 --seal-only
grep -Fq "new release candidates must use '-rc.YYYYMMDDHHMM'" /tmp/zero-release-policy.out

printf 'y\n' | run_release 0.0.16 >/dev/null
git --git-dir="$REMOTE_ROOT/origin.git" rev-parse --verify refs/heads/develop >/dev/null
git --git-dir="$REMOTE_ROOT/origin.git" rev-parse --verify refs/tags/v0.0.16-dev.202608131430 >/dev/null
git --git-dir="$MIRROR_ROOT/mirror.git" rev-parse --verify refs/heads/develop >/dev/null
git --git-dir="$MIRROR_ROOT/mirror.git" rev-parse --verify refs/tags/v0.0.16-dev.202608131430 >/dev/null

expect_fail --next dev
grep -Fq "development version '0.0.16-dev.202608131430' already exists" /tmp/zero-release-policy.out
git tag -d v0.0.16-dev.202608131430 >/dev/null
expect_fail --next dev
grep -Fq "development version '0.0.16-dev.202608131430' already exists" /tmp/zero-release-policy.out
git tag -a v0.0.16-dev.202608131430 -m v0.0.16-dev.202608131430
RELEASE_TIMESTAMP=202608131431
[[ "$(run_release --next dev)" == "0.0.16-dev.202608131431" ]]
printf 'y\n' | run_release 0.0.16 --no-push >/dev/null
RELEASE_TIMESTAMP=202608131432
[[ "$(run_release --next dev)" == "0.0.16-dev.202608131432" ]]
expect_fail 0.0.16
grep -Fq "previous release tag 'v0.0.16-dev.202608131431' is missing from remote 'mirror'" /tmp/zero-release-policy.out
git checkout -q --detach v0.0.16-dev.202608131430
run_release --verify-tag v0.0.16-dev.202608131430 >/dev/null
git checkout -q develop
RELEASE_TIMESTAMP=202608131530
[[ "$(run_release --next rc)" == "0.0.16-rc.202608131530" ]]
run_release 0.0.16-rc.202608131530 --seal-only
git add Cargo.toml release/breaking-changes.md
git commit -qm "release: v0.0.16-rc.202608131530"
git tag -a v0.0.16-rc.202608131530 -m v0.0.16-rc.202608131530

RELEASE_TIMESTAMP=202608131531
[[ "$(run_release --next rc)" == "0.0.16-rc.202608131531" ]]
[[ "$(run_release --next stable)" == "0.0.16" ]]
expect_fail 0.0.16-beta.1 --seal-only
expect_fail 0.0.16-rc.202608131530 --seal-only

run_release 0.0.16-rc.202608131531 --seal-only
git add Cargo.toml release/breaking-changes.md
git commit -qm "release: v0.0.16-rc.202608131531"
git tag -a v0.0.16-rc.202608131531 -m v0.0.16-rc.202608131531
run_release 0.0.16 --seal-only
run_release --check

RELEASE_TIMESTAMP=202608131532
expect_fail 0.0.16-rc.202608131532 --seal-only
expect_fail 0.0.15 --seal-only

CLEANUP_INPUT=(
    v0.0.15
    v0.0.16-dev.202608131430
    v0.0.16-dev.202608131431
    v0.0.16-rc.202608131530
    v0.0.16-rc.202608131531
    v0.0.16
    v0.0.17-dev.202608131600
)
RC_CLEANUP=$(run_release --cleanup-tags v0.0.16-rc.202608131531 "${CLEANUP_INPUT[@]}")
[[ "$RC_CLEANUP" == $'v0.0.16-dev.202608131430\nv0.0.16-dev.202608131431' ]]
[[ "$RC_CLEANUP" != *'v0.0.16-rc.202608131530'* ]]
STABLE_CLEANUP=$(run_release --cleanup-tags v0.0.16 "${CLEANUP_INPUT[@]}")
[[ "$STABLE_CLEANUP" == $'v0.0.16-dev.202608131430\nv0.0.16-dev.202608131431\nv0.0.16-rc.202608131530\nv0.0.16-rc.202608131531' ]]
[[ -z "$(run_release --cleanup-tags v0.0.16-dev.202608131431 "${CLEANUP_INPUT[@]}")" ]]

echo "Release policy tests passed."
