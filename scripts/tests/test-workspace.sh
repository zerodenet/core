#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/zero-test-entry.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
export ZERO_TEST_REAL_PYTHON=$(command -v python3)
mkdir "$scratch/bin"
cat > "$scratch/bin/cargo" <<'MOCK'
#!/usr/bin/env bash
if [[ ${1:-} == metadata ]]; then
    echo '{"packages":[],"workspace_members":[]}'
    exit "${ZERO_TEST_METADATA_EXIT:-0}"
fi
printf '%s\n' "$@" > "$ZERO_TEST_ARGS"
printf '%s\n' "$RUST_MIN_STACK" > "$ZERO_TEST_STACK"
echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'
exit "${ZERO_TEST_EXIT:-0}"
MOCK
chmod +x "$scratch/bin/cargo"
export PATH="$scratch/bin:$PATH"
export ZERO_TEST_ARGS="$scratch/args" ZERO_TEST_STACK="$scratch/stack"
entry="$root/scripts/test-workspace.sh"
bash "$entry" > "$scratch/full.log"
printf '%s\n' test --locked --workspace --all-features --no-fail-fast > "$scratch/expected"
cmp "$scratch/args" "$scratch/expected"
[[ $(cat "$scratch/stack") == 16777216 ]]
grep -q '^TEST_SCOPE full ' "$scratch/full.log"
grep -q '^TEST_START .*Z$' "$scratch/full.log"
grep -q '^TEST_END .* exit=0 seconds=' "$scratch/full.log"
bash "$entry" --jobs 2 --test endpoint_contracts --test proxy_control > "$scratch/focused.log"
printf '%s\n' test --locked --workspace --all-features --no-fail-fast --jobs 2 --test endpoint_contracts --test proxy_control > "$scratch/expected"
cmp "$scratch/args" "$scratch/expected"
grep -q '^TEST_SCOPE focused ' "$scratch/focused.log"
set +e
ZERO_TEST_EXIT=7 bash "$entry" > "$scratch/failure.log"
status=$?
set -e
[[ "$status" == 7 ]]
grep -q '^TEST_END .* exit=7 seconds=' "$scratch/failure.log"
set +e
bash "$root/.github/scripts/test-workspace.sh" bash -c '
    echo "thread native_case panicked at fixture.rs:17:3:"
    echo "actual original close source was retained"
    echo ""
    echo "failures:"
    echo "    native_case"
    echo "error: test failed"
    exit 7
' > "$scratch/nocapture.log"
status=$?
set -e
[[ "$status" == 7 ]]
grep -q '::error .*actual original close source was retained' "$scratch/nocapture.log"
# A later timeout must not erase the first actual protocol/provider source.
set +e
bash "$root/.github/scripts/test-workspace.sh" bash -c '
    echo "thread first_case panicked at first.rs:17:3:"
    echo "first original provider failure"
    for ((i=0; i<1000; i++)); do echo "parallel noisy trace $i with enough text to overflow the annotation tail"; done
    echo "thread last_case panicked at last.rs:29:3:"
    echo "last original timeout"
    echo "failures:"
    echo "    first_case"
    echo "    last_case"
    echo "test result: FAILED. 0 passed; 2 failed;"
    echo "error: test failed"
    exit 7
' > "$scratch/multiple-failures.log"
status=$?
set -e
[[ "$status" == 7 ]]
grep -q '::error .*first original provider failure' "$scratch/multiple-failures.log"
grep -q '::error .*last original timeout' "$scratch/multiple-failures.log"
rm "$scratch/args"
set +e
ZERO_TEST_METADATA_EXIT=13 bash "$entry" > "$scratch/layout-failure.log" 2>&1
status=$?
set -e
[[ "$status" != 0 && ! -e "$scratch/args" ]]
! grep -q '^TEST_START ' "$scratch/layout-failure.log"
cat > "$scratch/bin/python3" <<'MOCK'
#!/usr/bin/env bash
if [[ ${1:-} == */check-test-layout.py ]]; then exec "$ZERO_TEST_REAL_PYTHON" "$@"; fi
exit 44
MOCK
chmod +x "$scratch/bin/python3"
set +e
ZERO_TEST_EXIT=7 bash "$entry" > "$scratch/renderer-failure.log" 2>&1
status=$?
set -e
[[ "$status" == 7 ]]
grep -q 'failure summary unavailable; command exit=7' "$scratch/renderer-failure.log"
cat > "$scratch/bin/tee" <<'MOCK'
#!/usr/bin/env bash
cat > /dev/null
exit 9
MOCK
chmod +x "$scratch/bin/tee"
set +e
bash "$entry" > "$scratch/log-failure.log" 2>&1
status=$?
set -e
[[ "$status" == 9 ]]
grep -q '^TEST_END .* exit=9 seconds=' "$scratch/log-failure.log"
for option in '--jobs 0'  '--test' '--unknown'; do
    set +e
    # Intentional argument splitting for this fixed invalid-input fixture.
    bash "$entry" $option > "$scratch/invalid.log" 2>&1
    status=$?
    set -e
    [[ "$status" == 2 ]]
done
echo 'workspace test entry: scopes, stack, workers, timing, command, log and renderer errors passed'
