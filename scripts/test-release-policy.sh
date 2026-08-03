#!/usr/bin/env bash
set -euo pipefail

SCRIPT_UNDER_TEST=${1:-scripts/release.sh}
SCRIPT_UNDER_TEST="$(cd "$(dirname "$SCRIPT_UNDER_TEST")" && pwd)/$(basename "$SCRIPT_UNDER_TEST")"
ROOT=$(mktemp -d)
REMOTE_ROOT=$(mktemp -d)
trap 'rm -rf "$ROOT" "$REMOTE_ROOT"' EXIT
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
    ZERO_REPO_ROOT="$ROOT" bash "$SCRIPT_UNDER_TEST" "$@"
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
git remote add origin "$REMOTE_ROOT/origin.git"
git add Cargo.toml release/breaking-changes.md
git commit -qm "initial"
git tag -a v0.0.15 -m v0.0.15

DEVELOP_PREVIEW=$(printf 'n\n' | run_release 0.0.16 2>&1)
[[ "$DEVELOP_PREVIEW" == *"Cargo version: 0.0.15 -> 0.0.16-dev.1"* ]]
[[ "$DEVELOP_PREVIEW" == *"Tag: v0.0.16-dev.1"* ]]

[[ "$(run_release --next dev)" == "0.0.16-dev.1" ]]
[[ "$(run_release --next rc)" == "0.0.16-rc.1" ]]
expect_fail 0.0.16-dev --start-development
expect_fail 0.0.15 --seal-only
expect_fail 0.0.16-rc.2 --seal-only

run_release 0.0.16-dev.1 --start-development
git add Cargo.toml
git commit -qm "release: v0.0.16-dev.1"
git tag -a v0.0.16-dev.1 -m v0.0.16-dev.1

[[ "$(run_release --next dev)" == "0.0.16-dev.2" ]]
[[ "$(run_release --next rc)" == "0.0.16-rc.1" ]]
run_release 0.0.16-rc.1 --seal-only
git add Cargo.toml release/breaking-changes.md
git commit -qm "release: v0.0.16-rc.1"
git tag -a v0.0.16-rc.1 -m v0.0.16-rc.1

[[ "$(run_release --next rc)" == "0.0.16-rc.2" ]]
[[ "$(run_release --next stable)" == "0.0.16" ]]
expect_fail 0.0.16-beta.1 --seal-only
expect_fail 0.0.16-rc.3 --seal-only

run_release 0.0.16-rc.2 --seal-only
git add Cargo.toml release/breaking-changes.md
git commit -qm "release: v0.0.16-rc.2"
git tag -a v0.0.16-rc.2 -m v0.0.16-rc.2
run_release 0.0.16 --seal-only
run_release --check

expect_fail 0.0.16-rc.3 --seal-only
expect_fail 0.0.15 --seal-only

echo "Release policy tests passed."
