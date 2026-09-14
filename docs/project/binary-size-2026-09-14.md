# Release binary size — 2026-09-14

## Scope

Keep the full `full,status-api,connector` feature set. Do not remove protocols,
TLS fingerprints, or legacy TLS cipher support. Keep release `opt-level = 3`
and panic unwinding: engine hook isolation uses `catch_unwind`.

## Measured baseline

The published `v0.0.2-dev.202609140303` Linux musl archive contains a 67,431,504-byte
`zero` (64.3077 MiB), exceeding the older ZBoard installer limit of 67,108,864
bytes. Archive compression does not affect that extracted-file limit.

A controlled symbol-stripping comparison used the existing Intel macOS release
binary built from `36efa617` (`v0.0.2-dev.202609140128` plus CI diagnostics).
Both files below contain exactly the same compiled code and features:

| File | Bytes | MiB |
| --- | ---: | ---: |
| Original | 63,012,288 | 60.0932 |
| After native symbol stripping | 48,311,776 | 46.0737 |

This removes 14,700,512 bytes (23.33%) without recompiling. Startup and the
reported compiled feature set are retained. These are macOS measurements, not
an estimate of the Linux result. Removing symbols reduces symbolic debugging
information available from the distributed binary.

## Implementation

- Workspace release profile: `strip = "symbols"`; retain the default LTO setting.
- Retain default throughput optimization, codegen parallelism, and unwinding.
- Release packaging checks the actual executable is nonempty and no larger
  than 64 MiB, then runs `--version` on the native build runner.
- Keep the existing five targets, musl static-link checks, Wintun packaging,
  archive checksums, and successful-CI publication gate.

## Verification

The workflow policy checks and YAML parsing passed. The binary-size gate
accepted the stripped binary and rejected empty/oversized fixtures before
execution. No Rust test suite or performance benchmark was run.

A trial full-feature ThinLTO build produced 49,267,112 bytes (46.9848 MiB)
on Intel macOS and passed startup, feature/capability declaration comparison,
and the VLESS example configuration validation. This did not beat stripping
alone, so ThinLTO was removed from the proposed release profile.

The old macOS baseline predates small CI, Mieru, and ECH fixes. The current-source
stripping-only rebuild supplies the final direct comparison against the ThinLTO
trial. Linux/Windows release sizes still require their platform builds.

## Final current-source build

Intel macOS, `full,status-api,connector`, source `57ada776` plus this
release-profile change: **48,279,112 bytes (46.0425 MiB)**.
That is 23.38% below the existing unstripped baseline and
988,000 bytes below the same-source ThinLTO trial.

The complete build succeeded in 11m 39s; the ThinLTO trial took 13m 22s.
These are individual local runs, not a controlled build-time benchmark.
The final size gate, startup, compiled-feature/capability comparison and VLESS
example configuration validation passed. No test suite was executed.
