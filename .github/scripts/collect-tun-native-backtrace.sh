#!/usr/bin/env bash
# Keep symbolic stacks only. Full process memory never becomes a CI artifact.
set -euo pipefail
directory=${RUNNER_TEMP:?}/zero-tun-cores
output=${RUNNER_TEMP:?}/zero-tun-backtraces
mkdir -p "$output"
shopt -s nullglob
# Linux %e is the crashing task's comm, which can be tokio-rt-worker.
for core in "$directory"/core.*; do
    name=$(basename "$core")
    sudo gdb --batch \
        -ex 'set print frame-arguments none' \
        -ex 'set backtrace limit 96' \
        -ex 'thread apply all bt' \
        target/x86_64-unknown-linux-gnu/debug/zero "$core" \
        > "$output/$name.txt" 2>&1 || true
    sudo rm -- "$core"
done
