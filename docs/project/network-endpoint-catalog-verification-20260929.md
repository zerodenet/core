# Network endpoint catalog: first-stage verification

Date: 2026-09-29. Checkout: develop, baseline c71a7c22, with pre-existing DNS/TCP edits.
Host: macOS x86_64. This record concerns local code and validation, not deployment.

Historical first-stage record: independent control was added subsequently. Current P2 scope
and gates are tracked in [control verification](network-endpoint-control-verification-20260929.md).

## Implemented scope

- Canonical endpoint configuration and a deduplicated legacy resource catalog.
- Engine-owned configuration admission and registered read-only resource observation.
- HTTP and common JSON endpoint list/get/details contracts; public peer identity and detail schema.
- Outbound-only Packet ingress chooses correlated TCP/UDP conversion, rejects unsafe native
  return paths, and retains the explicit stack-owned Echo translation adapter.
- Disabled configuration does not prepare its physical listener/device. Independent runtime
  lifecycle commands remain unsupported.
- Typed configuration apply materializes endpoint projections before establishing its expected
  acknowledgement. Late duplicate reload notifications preserve the DNS candidate owned by
  an acknowledged configuration transaction.

## Local gates

| Gate | Result |
| --- | --- |
| cargo fmt --all -- --check | Passed after the transaction repair |
| cargo check --workspace --all-features | Passed after the transaction repair |
| RUST_MIN_STACK=16777216 cargo test --workspace --all-features --no-fail-fast | Passed: 321 targets, 2254 passed, 0 failed, 155 ignored |
| cargo clippy --workspace --all-targets --all-features | Passed, exit 0 in 342.7 seconds |
| cargo build --release --features wireguard | Passed, exit 0 in 890.5 seconds |
| target/release/zero --version | Passed; release profile and wireguard feature reported |

Before the final transaction repair, focused resource, direction, capability-list and runtime
boundary tests passed (194 tests). Earlier workspace attempts exposed and corrected test-file
boundary violations, an outdated exact capability-list expectation, and a DNS transaction race.
Those interrupted/failed attempts are not recorded as a full-suite pass.

The DNS race has a deterministic regression that replays an already acknowledged snapshot
before the configuration owner commits DNS. Both this regression and the existing failed-bind
retry passed in the final full suite. The full command ended with exit 0 in 1623.9 seconds;
the 155 ignored tests/examples are not passes. Their prerequisites and any required external,
platform, privileged or reference-binary acceptance remain separately tracked.

The timestamped command log is `/Volumes/tool/tmp/zero-endpoint-management-recovery-20260929.log`.
Only the final recovery run above is the full-suite acceptance result for the first-stage source.
All listed gates completed successfully. The build is a local artifact; no installed client
core was replaced and no release was published.

## Remaining development and acceptance

- P2: independent set_state/set_directions/restart/clear_overrides; temporary intent preservation,
  source-file transactions, dependencies, rollback and acknowledgement of ended business/tasks.
- P3: instance/intent revisions, counters, events, started/failed state, inbound peer health and
  authenticated remote/source facts.
- P4: complete carrier acceptance, runtime schema export, diagnostic contracts and a second
  neutral test endpoint with executable lifecycle/data-plane operations.
- Canonical listening-only WireGuard peers; existing legacy inbound configuration remains usable.
- P5: external A/B payload, real TUN, platform and long-running acceptance. Ignored tests are
  not passes; successful handshakes are not payload acceptance.

PacketRoute closure still ends a held data path. It is not an endpoint stop command, nor does
this first stage implement an independent public per-path close-management API.

See [catalog contract](network-endpoint-catalog-v1.md) and
[management plan](network-endpoint-management-plan.md).
