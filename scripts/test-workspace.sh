#!/usr/bin/env bash
# Use one feature graph for edit-time regression and final workspace qualification.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tests=()
jobs=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --test)
            [[ $# -ge 2 && -n "$2" && "$2" != -* ]] || { echo "--test requires a target name" >&2; exit 2; }
            tests+=(--test "$2")
            shift 2
            ;;
        --jobs)
            [[ $# -ge 2 && "$2" =~ ^[1-9][0-9]*$ ]] || { echo "--jobs requires a positive integer" >&2; exit 2; }
            jobs=(--jobs "$2")
            shift 2
            ;;
        --help|-h)
            cat <<'HELP'
Usage: scripts/test-workspace.sh [--jobs N] [--test TARGET ...]

No --test: full workspace suite, including unit tests and doctests.
--test: edit-time integration regression only; never full qualification.
Both modes use --workspace --all-features and the same test profile.
--jobs limits Cargo compilation workers; it does not limit test threads.
Ignored external/privileged tests remain separate qualification.
HELP
            exit 0
            ;;
        *) echo "unknown option: $1 (see --help)" >&2; exit 2 ;;
    esac
done
cd "$root"
export RUST_MIN_STACK=16777216
if [[ ${#tests[@]} -gt 0 ]]; then
    echo "TEST_SCOPE focused (integration targets only; not full qualification)"
else
    echo "TEST_SCOPE full (workspace, unit, integration and doctests)"
fi
args=(cargo test --workspace --all-features --no-fail-fast)
# Bash 3.2 requires guarding expansion of empty arrays under nounset.
if [[ ${#jobs[@]} -gt 0 ]]; then args+=("${jobs[@]}"); fi
if [[ ${#tests[@]} -gt 0 ]]; then args+=("${tests[@]}"); fi
exec bash "$root/.github/scripts/test-workspace.sh" "${args[@]}"
