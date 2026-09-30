# Endpoint control: P2 slice verification

Date: 2026-09-29. Checkout: develop, baseline c71a7c22, with pre-existing DNS/TCP edits.
Host: macOS x86_64. Scope: local kernel implementation, not release or installation.

## Implemented and exercised

- Registered neutral control capability and acknowledged set_state/set_directions/restart/
  clear_overrides commands over the existing configuration transaction.
- Runtime intent preservation across reload, canonical source-file persistence, rollback,
  conditional revisions, idempotent commands and no reuse of discarded revision tokens.
- Confirmed listener/raw-IP driver completion; revoked resource flows include relay hops.
- One resource stop releases its UDP listener while another remains running; failed start
  restores disabled intent and permits a subsequent retry.
- Outbound-only authenticated replies continue, remote new business is rejected, and an
  established TCP flow closes after stop acknowledgement.
- A dropped request cannot abandon its staged transaction. Explicit legacy role linkage
  controls one shared resource; source-file management requires canonical configuration.
- Same-config/different-intent snapshots cannot cross-acknowledge an operation. Existing
  duplicate-reload DNS transaction protection remains covered.

## Local gates

Status: all local gates passed on the final source. The full management plan and external
acceptance remain incomplete as listed below.

| Gate | Result |
| --- | --- |
| cargo fmt --all -- --check | Passed, exit 0, 14.2 seconds |
| cargo check --workspace | Passed, exit 0, 13.9 seconds, no warnings |
| Focused workspace test command, all features | Passed: 9 targets, 235 passed, 0 failed, 1 ignored; exit 0, 168.1 seconds |
| RUST_MIN_STACK=16777216 cargo test --workspace --all-features | Passed: 325 targets, 2267 passed, 0 failed, 155 ignored; exit 0, 1017.2 seconds |
| cargo clippy --workspace --all-targets --all-features -- -D warnings | Passed, exit 0, 234.3 seconds, warnings denied |
| cargo build --release --features full,status-api,wireguard | Passed, exit 0, 368.5 seconds |
| target/release/zero --version | Passed, exit 0, 1.7 seconds; WireGuard compiled |
| git diff --check | Passed, exit 0, 0.1 seconds; repeated after documentation edits |

Focused targets: workspace_architecture (26), runtime_boundary (188), endpoint_control (5),
core_capabilities (1), endpoint_directions (3), endpoint_management (4),
endpoint_management_recovery (2), endpoint_management_payload (1), wireguard_inbound
(5 passed, 1 ignored). The counts in parentheses are passed tests.

The final full workspace stage ran from 2026-09-29T23:23:46+08:00 to
2026-09-29T23:40:43+08:00. All 2479 Rust/TOML/lockfile hashes still matched the test-start
manifest after all gates; documentation edits are outside that source manifest.

Release compilation completed at 2026-09-29T23:50:46+08:00. This is a local dirty-worktree
validation build, retaining product version `0.0.3-dev.202609281319`; no new release label
was created. Binary: `/Volumes/tool/rust/zero/target/release/zero`.
SHA-256: `c6a3963bef638c73d887b92003795fe6b5824c79f3c8838fc339e819d967dfa4`.
The gate runner ended at 2026-09-29T23:50:48+08:00 with exit 0.

The command log is `/Volumes/tool/tmp/zero-endpoint-control-gates-20260929.log`. It records
per-stage start/end timestamps, child exit status and duration; silence is not completion.
Source hashes are recorded in `/Volumes/tool/tmp/zero-endpoint-control-sources-20260929.json`.

Earlier attempts are not a pass: the first runtime test found a fixture that omitted explicit
bidirectional permissions; the first gate found an accessor gated behind raw-IP in a generic
listener path. Both were corrected. The failed default-check log is preserved separately at
`/Volumes/tool/tmp/zero-endpoint-control-gates-20260929-attempt1.log`.

Two later workspace attempts were deliberately interrupted to correct source-file intent
idempotence and enforce the same live-contraction guard for configuration reload. Neither is
full-suite acceptance; logs are preserved as attempt2/attempt3 respectively. The last interrupted
run completed 120 targets without a reported failure before the guard repair.

Attempt4 failed the architecture target because generic registration hooks named a concrete
protocol in conditional lint attributes. The hooks now use protocol-neutral lint reasons.
Attempt5 failed the existing linked WireGuard role test because the permission guard treated
legacy role unlinking as retained-role permission contraction. Topology changes now continue
through the existing listener/device reconcile; retained-role permission contraction stays
guarded. Both failures were repaired and their targets are included in the final focused run.
Logs are preserved as attempt4/attempt5; neither attempt is full-suite acceptance.

## Remaining development versus acceptance

Development: live direction contraction with independent business cancellation; complete
dependency ordering/failure coordination; generation and lifecycle/error/start-time facts;
statistics/events and additional peer facts; canonical listening-only peers; carrier/schema/
diagnostic extensions and a second executable neutral test endpoint. Public independent
PacketRoute administration remains separate from the existing internal close operation.

Acceptance: real A/B traffic and DNS/cache behavior during stop/start, real TUN, external
fault recovery, long-running and cross-platform tests. Ignored targets are not passes.
WireGuard remains opt-in; local lifecycle tests do not close production/security gates.

No commit, push, release or installed-client core replacement was performed.
See [control contract](network-endpoint-control-v1.md) and
[management plan](network-endpoint-management-plan.md).

## 2026-09-30 follow-up (new source, separate from the historical gate result)

The current dirty worktree additionally records active TCP/UDP Flow counts, applied
generation/start/error facts, endpoint state events, and authenticated peer source
facts. This follow-up permits live **inbound** contraction: the new admission policy
blocks new remote business, existing inbound Flows are cancelled and confirmed by
the control command, while the same listener and outbound business remain active.
Live outbound contraction still returns unsupported until Packet return routes and
client-stack work can be revoked without interrupting the inbound role.

The existing stats sampler now emits one `endpoint.stats_sampled` event per bounded
page of 64 endpoints every 10 seconds. Samples include configuration revision,
report current Flow counts and retain
null for unimplemented Packet, byte, packet and drop counters. This is a partial P3
event, not complete traffic accounting. No peer key material enters the payload.

This follow-up was validated separately from the historical 2026-09-29 gates.

| 2026-09-30 gate | Result |
| --- | --- |
| Focused endpoint API, Engine, management and two-device direction tests | Passed: 13, 4, 5 and 3 tests respectively |
| cargo fmt --all -- --check | Passed |
| cargo check --workspace | Passed |
| RUST_MIN_STACK=16777216 cargo test --workspace --all-features | Passed, exit 0: 325 targets, 2270 passed, 0 failed, 155 ignored; 6249.46 seconds |
| cargo clippy --workspace --all-targets --all-features -- -D warnings | Passed, exit 0; 233.86 seconds |
| cargo build --release --features full,status-api,wireguard | Passed, exit 0; 714.53 seconds |
| target/release/zero --version | Passed; build profile release, WireGuard compiled |
| git diff --check | Passed |

The full workspace test log is
`/Volumes/tool/tmp/zero-endpoint-followup-workspace-20260930.log`; the Clippy
log is `/Volumes/tool/tmp/zero-endpoint-followup-clippy-20260930.log`.
The release log is `/Volumes/tool/tmp/zero-endpoint-followup-release-20260930.log`.
The local binary is `/Volumes/tool/rust/zero/target/release/zero`, retaining
product version `0.0.3-dev.202609281319`; its SHA-256 is
`f8291ef120d2390a0a2bca6f62c9aaf3cf6cd1d3b44f456c2939ebbf5290226d`.
This dirty-worktree build was not published or installed. Ignored tests are not
accepted by this result. Real A/B, long-running, fault and cross-platform
acceptance remain separate.
