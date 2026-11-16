#!/bin/sh
# Explicit optional tool build; never invoked by Cargo, CI or Zero startup.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
out_dir=${1:-"$script_dir/../../target/macos-es-preflight"}
mkdir -p "$out_dir"
xcrun --sdk macosx clang -std=c11 -fblocks -Wall -Wextra -Werror \
    -mmacosx-version-min=10.15 "$script_dir/preflight.c" \
    -lEndpointSecurity -o "$out_dir/zero-es-preflight"
printf '%s\n' "Built unsigned capability probe: $out_dir/zero-es-preflight"
printf '%s\n' 'No entitlement was granted; no process-tree provider was enabled.'
