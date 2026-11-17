#!/usr/bin/env bash
# Keep the failing test details visible through the checks API as well as job logs.
set -euo pipefail
round2_capture=false
if [[ ${ZERO_ROUND2:-false} == true && -z ${ZERO_TEST_ARGS:-} ]]; then
    round2_capture=true
    mkdir -p .round2-evidence/tests
    log_file=$(mktemp .round2-evidence/tests/workspace.XXXXXX.log)
else
    log_file=$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/zero-workspace-tests.XXXXXX")
    trap 'rm -f "$log_file"' EXIT
fi
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
started_seconds=$SECONDS
echo "TEST_START $started_at"
if [[ "$round2_capture" == true ]]; then
    python3 .github/scripts/round2-test-evidence.py start "$log_file" "$@"
fi
set +e
"$@" 2>&1 | tee "$log_file"
command_status=("${PIPESTATUS[@]}")
set -e
status=${command_status[0]}
if [[ "$status" -eq 0 ]]; then status=${command_status[1]}; fi
echo "TEST_END $(date -u +%Y-%m-%dT%H:%M:%SZ) exit=$status seconds=$((SECONDS - started_seconds))"
if [[ "$round2_capture" == true ]]; then
    python3 .github/scripts/round2-test-evidence.py finish "$log_file" "$status"
fi
if [[ "$status" -eq 0 ]]; then
    exit 0
fi
python3 - "$log_file" <<'PY' || echo "failure summary unavailable; command exit=$status" >&2
from pathlib import Path
import sys
import re

text = re.sub(r"\x1b\[[0-9;]*m", "", Path(sys.argv[1]).read_text(errors="replace"))
lines = text.splitlines()
# Keep early sources and late summaries when parallel --nocapture output or
# --no-fail-fast produces more than one annotation budget. A tail-only slice
# discarded the original cached-driver error before a later timeout appeared.
def bounded(items, limit):
    items = list(dict.fromkeys(items))
    raw = "\n".join(items).encode()
    if len(raw) <= limit:
        return raw
    half = (limit - 40) // 2
    return raw[:half] + b"\n... original log excerpt omitted ...\n" + raw[-half:]

panics = []
facts = []
summary = []
for index, line in enumerate(lines):
    if line.startswith("thread ") and "panicked at" in line:
        panics.append("\n".join(lines[index:index + 8]))
    if line.startswith("error[") or line.startswith("error: could not compile"):
        facts.append("\n".join(lines[index:index + 8]))
    elif any(marker in line for marker in (
        "session failed", "sources=", "original shutdown cause[",
        "operation failed stage=", "TUN state deadline", "TUN state rejected",
        "process terminating with fatal error",
    )):
        facts.append(line)
    if (line == "failures:" or line.startswith("test result: FAILED")
            or line.startswith("error: test failed")
            or re.match(r"^    [A-Za-z_][A-Za-z_0-9:]*$", line)):
        summary.append(line)
message = b"\n".join((bounded(panics, 12000), bounded(facts, 9000), bounded(summary, 3000)))
if not message.strip():
    message = bounded(lines[-80:], 24000)
# Actions truncates each annotation to 4 KiB; split before escaping workflow syntax.
parts = [message[i:i + 3000].decode(errors="replace") for i in range(0, len(message), 3000)]
for index, part in enumerate(parts, 1):
    escaped = part.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error title=Workspace tests failed ({index}/{len(parts)})::{escaped}")

PY
exit "$status"
