#!/usr/bin/env bash
# Run the original native test without adding a process supervisor or stack override.
set -euo pipefail
ulimit -c unlimited
exec "$@"
