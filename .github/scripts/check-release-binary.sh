#!/usr/bin/env bash
set -euo pipefail

BINARY=${1:?release binary path required}
# Guard against unstripped or un-LTO'd release binary bloat across all 5 targets.
MAX_BYTES=$((96 * 1024 * 1024))
BYTES=$(wc -c < "$BINARY")
if (( BYTES <= 0 || BYTES > MAX_BYTES )); then
    echo "::error::Release binary size is $BYTES bytes; allowed: 1-$MAX_BYTES bytes"
    exit 1
fi
echo "Release binary: $BINARY ($BYTES bytes; limit $MAX_BYTES)"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    echo "Binary size: $BYTES bytes (limit $MAX_BYTES bytes)." >> "$GITHUB_STEP_SUMMARY"
fi
# Every release target runs on its native runner. This is only a startup check.
"$BINARY" --version
