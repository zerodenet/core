#!/usr/bin/env bash
# Keep the failing test details visible through the checks API as well as job logs.
set -euo pipefail
log_file=$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/zero-workspace-tests.XXXXXX")
trap 'rm -f "$log_file"' EXIT
set +e
"$@" 2>&1 | tee "$log_file"
command_status=("${PIPESTATUS[@]}")
set -e
status=${command_status[0]}
if [[ "$status" -eq 0 ]]; then
    exit "${command_status[1]}"
fi
python3 - "$log_file" <<'PY'
from pathlib import Path
import sys
import re

text = re.sub(r"\x1b\[[0-9;]*m", "", Path(sys.argv[1]).read_text(errors="replace"))
lines = text.splitlines()
# Preserve every Rust failure block even when --no-fail-fast continues other targets.
selected = []
collecting = False
for line in lines:
    if line == "failures:":
        collecting = True
    if collecting:
        selected.append(line)
    if collecting and "error: test failed" in line:
        collecting = False
if not selected:
    selected = lines[-80:]
message = "\n".join(selected).encode()[-24000:]
# Actions truncates each annotation to 4 KiB; split before escaping workflow syntax.
parts = [message[i:i + 3000].decode(errors="replace") for i in range(0, len(message), 3000)]
for index, part in enumerate(parts, 1):
    escaped = part.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error title=Workspace tests failed ({index}/{len(parts)})::{escaped}")

PY
exit "$status"
