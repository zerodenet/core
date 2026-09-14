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

lines = Path(sys.argv[1]).read_text(errors="replace").splitlines()
# Rust reports failed test names, panic locations, and the rerun command at the end.
message = "\n".join(lines[-80:])[-16000:]
message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
print(f"::error title=Workspace tests failed::{message}")
PY
exit "$status"
