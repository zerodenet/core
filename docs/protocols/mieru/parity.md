# Mieru official implementation parity

This is a capability and qualification ledger, not a claim of full official parity.
The implementation remains `partial`. Source interoperability does not qualify an
installed release or a production deployment.

## Implementation baseline and supplementary probes

The sole implementation comparison baseline is official **v3.33.0**, matching
the `mieru` package and lockfile version **3.33.0**. Every remaining feature gap
below is compared with that fixed source, not with the latest release.

| Role | Version | Immutable official commit |
| --- | --- | --- |
| Implementation baseline | v3.33.0 | `48ddb69d5d343d76c9004c5054ee36609579ba13` |
| Previously tested supplementary compatibility probe | v3.36.1 | `316cc6606c287d45a321b99ac86a1f2e7f2b785a` |

`scripts/prepare-mieru-interop.py --version <version> --output <path>` builds a
reference probe from a clean checkout matching the selected commit. Its default
comes from the Mieru package version; baseline CI uses that default. The probe
calls official exported APIs and reports its version and commit. Explicitly
selecting another pinned version only adds compatibility evidence.

`python3 scripts/prepare-mieru-interop.py --check-baseline` checks that the package
version, generated lockfile and immutable reference mapping agree. The repository
ignores `Cargo.lock`; baseline CI explicitly runs `cargo generate-lockfile` before
the check, so a clean checkout does not rely on a local-only lockfile. The
protocol-owned `mieru-config` package is checked against the same version. The earlier v3.36.1
default was inconsistent with the package baseline and has been corrected.
Local script checks passed for the current baseline, rejecting a lock mismatch,
rejecting an unpinned package version, rejecting an extra version as the baseline,
and following a deliberately changed package version in an isolated fixture.
This correction changes reference tooling and documentation, not protocol
behavior; it does not add a new data-plane test result to the runs below.

## Correctness acceptance

The current follow-up covers these independently reviewable boundaries:

- UDP flush must not report success for unacknowledged data when the peer closes.
  Graceful local close and crossed close must preserve the data acknowledgement
  requirement; duplicate close must remain harmless.
- UDP advertised receive capacity limits additional transmissions, while the
  congestion window limits total in-flight packets. A zero receive window must
  prevent new data without blocking necessary control traffic.
- A pending client OPEN must not hold the pool lock and block other OPENs.
  Concurrent initial dialing, capacity limits, cancellation, credential and
  generation isolation, and reload retirement must remain correct.
- Both UDP reader APIs must preserve partial frame bytes across cancellation,
  return one packet at a time after coalescing, reject truncated EOF, and retain
  pending bytes when a session is handed to its responder.

On reload, existing logical streams retain their carrier guards. Requests already
waiting on a retired pool slot may finish through an uncached carrier built from
their original request context; these carriers never enter the new cache. The
four-carrier limit applies to a current cached identity, not to all carriers
retiring across reload generations.

## Previous hardening validation, 2026-09-10

These results predate the CUBIC, MTU, traffic-pattern and receive-policy changes.
They do not validate those new changes. Their current acceptance is recorded below.

The follow-up source passed `cargo fmt --all`, `cargo check --workspace`, and
`cargo clippy --workspace --all-targets -- -D warnings`. Its focused protocol
suite passed 27 tests with no failures or ignored cases:

```sh
RUST_MIN_STACK=16777216 cargo test -p mieru --all-features --lib --test underlay --test udp_framing
```

These cover the packet driver, receive-window calculation, UDP frame readers,
and outbound underlay lifecycle/concurrency.

The full workspace command completed with exit 101: **1,605 passed, 7 failed,
98 ignored**.

```sh
RUST_MIN_STACK=16777216 cargo test --workspace --all-features --no-fail-fast
```

The seven failures were confined to unchanged `zero-connector` tests:

- `event_dispatcher`: 9 passed, 3 failed. The failures were
  `outbox_only_workset_makes_progress_for_every_sink`,
  `dispatcher_compacts_a_large_outbox_and_restarts_without_redelivery`, and
  `outbox_only_workset_still_delivers_fact_events`.
- `webhook_dispatcher`: 13 passed, 4 failed. The failures were
  `dispatcher_spills_backlog_to_disk_and_pages_a_bounded_working_set`,
  `durable_stalled_webhook_is_cancelled_on_shutdown_and_recovered`,
  `dispatcher_recovers_unacknowledged_webhook_delivery_from_outbox`, and
  `transient_samples_are_delivered_best_effort_but_never_persisted`.

These assertions timed out waiting for delivery or workset progress. Their root
cause is unresolved; this is not a clean first-run workspace gate. The ordinary
workspace invocation leaves external-reference interop cases ignored, so
dual-version Mieru interoperability requires separate explicit runs.

After both interop runs, the same two Connector test binaries were rerun directly
from `crates/connector`, with the same stack setting and default test concurrency.
The same failures reproduced: **event_dispatcher: 9 passed / 3 failed;
webhook_dispatcher: 13 passed / 4 failed**. Binary SHA-256 values remained
unchanged during the reruns. No Connector source, assertion timeout or test
thread setting was changed. The workspace gate therefore remains failed; these
results must not be summarized as an intermittent first-run failure followed by
a clean retry. Reproduction details and hashes are in
`/tmp/mieru-parity-connector-retest.json`, with logs in
`/tmp/mieru-parity-event-dispatcher-retest.log` and
`/tmp/mieru-parity-webhook-dispatcher-retest.log`.

Both explicit official-reference runs passed, using the immutable versions
listed above: **v3.33.0: 4 passed; v3.36.1: 4 passed**, with no failures or ignored
cases. Each version exercised official TCP/UDP underlays into Zero, and Zero
TCP/UDP pooled outbounds into the official server. The cases verify concurrent
TCP and UDP logical sessions, stalled/closed-session isolation, one-underlay
reuse and subsequent requests. They do not constitute a throughput benchmark or
long-running recovery qualification.

```sh
RUST_MIN_STACK=16777216 MIERU_REFERENCE_BIN=/path/to/pinned-reference cargo test -p zero-proxy --all-features --test mieru_official_interop -- --ignored --nocapture
```

Local run records are `/tmp/mieru-parity-validation.json`,
`/tmp/mieru-parity-workspace.log`, `/tmp/mieru-parity-interop-3330.log`, and
`/tmp/mieru-parity-interop-3361.log`. The workspace baseline workflow has not been
run on remote CI during this follow-up.

## Transport comparison against v3.33.0

The official checkout was rechecked as clean at
`48ddb69d5d343d76c9004c5054ee36609579ba13`. This is a source comparison;
performance effects still require measurement.

| Area | Official v3.33.0 evidence | Current Zero boundary |
| --- | --- | --- |
| UDP congestion control | [CUBIC implementation](https://github.com/enfein/mieru/blob/v3.33.0/pkg/congestion/cubic.go#L31), wired into [sessions](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/session.go#L166) | [CUBIC](../../../protocols/mieru/src/packet/congestion.rs) now implements the reference curve, ACK growth, loss reduction and timeout restart. Acceptance pending below. |
| UDP recovery policy | [Retransmission and backoff](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/session.go#L728) | [Retry and timeout handling](../../../protocols/mieru/src/packet/reliability.rs#L177) has different duplicate-ACK accounting, backoff and retry limits. Basic ACK/reorder/retransmission support exists; recovery policy is not equivalent. |
| Heartbeat timing | [Five-second interval with random jitter](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/session.go#L834) | [Fixed two-second idle ACK/heartbeat](../../../protocols/mieru/src/packet/reliability.rs#L248); jitter remains absent. |
| Carrier selection and retirement | [Factor-based carrier selection](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/mux.go#L695), [traffic-volume retirement](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/mux.go#L721), [idle scheduler](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/scheduler.go#L39) | [Pool](../../../protocols/mieru/src/client/pool.rs) implements Zero's bounded policy: least-loaded selection, growth when every carrier reaches 75% of its own session capacity, four carriers per identity, 30-minute age retirement with active-stream draining, and reload retirement. It deliberately does not copy the official presets or traffic-volume rule one-to-one. |
| Slow consumers | [TCP receive-queue wait](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/session.go#L930), [UDP receive buffering](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/session.go#L966) | [TCP queue handling](../../../protocols/mieru/src/inbound/multiplex/reader.rs) now uses configurable per-session bounded backlog with a default 30s progress deadline. UDP retains bounded receive-window backpressure. See [tradeoffs](policies.md). |
| MTU and segmentation | [MTU setting](https://github.com/enfein/mieru/blob/v3.33.0/pkg/appctl/proto/clientcfg.proto#L67), [MTU-based payload sizing](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/segment.go#L43) | Runtime `mtu` selects UDP segmentation and padding budgets, including peer IP-family overhead. Default 1400, range 1280–1500. Public legacy ReliableSession::new retains its 1100-byte default; runtime uses configured construction. |
| Traffic-pattern execution | [Original traffic-pattern settings](https://github.com/enfein/mieru/blob/v3.33.0/pkg/appctl/proto/base.proto#L130), [packet padding](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/underlay_packet.go#L631) | Runtime traffic-pattern settings now drive nonce prefixes, UDP first/all packet selection, padding and TCP encrypted-write fragmentation. See [configuration](policies.md). |

The advertised receive window as additional send capacity is already implemented
in [Zero](../../../protocols/mieru/src/packet/reliability.rs#L220), matching
[v3.33.0](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/session.go#L1229).
It is no longer an open gap.

## Application mapping and Zero integration boundaries

These must not be presented as missing Mieru wire handshakes or as evidence that
the official application supports arbitrary Zero relay chains.

| Area | Current mapping and remaining difference |
| --- | --- |
| HandshakeMode | Official [STANDARD/NO_WAIT](https://github.com/enfein/mieru/blob/v3.33.0/pkg/appctl/proto/clientcfg.proto#L127) controls local SOCKS request/response pipelining. Zero implements the standard request/response path in [tunnel.rs](../../../protocols/mieru/src/tunnel.rs#L35), without the optional NO_WAIT application behavior. This is not a second Mieru wire-handshake format. Official OPEN can additionally carry initial payload; Zero UDP OPEN currently queues it separately. |
| Multiplexing configuration | The official application exposes [DEFAULT/OFF/LOW/MIDDLE/HIGH](https://github.com/enfein/mieru/blob/v3.33.0/pkg/appctl/proto/clientcfg.proto#L114). Zero lacks that mapping; the underlying carrier-selection difference is listed above. |
| Multiple endpoints and port bindings | Official [profiles](https://github.com/enfein/mieru/blob/v3.33.0/pkg/appctl/proto/clientcfg.proto#L57) expand multiple servers and port bindings into endpoints, then [select an endpoint when dialing](https://github.com/enfein/mieru/blob/v3.33.0/pkg/protocol/mux.go#L632). A Zero Mieru outbound has one server/port/transport. Multiple Zero inbounds/outbounds provide partial composition, not identical per-profile pool scheduling. |
| TCP relay pooling | Official v3.33.0 injects a SOCKS5 dialer into its Mux. Zero now lazily creates and pools a Mieru TCP final-hop carrier by complete relay-prefix identity and egress generation. The pool carries both TCP CONNECT and UDP ASSOCIATE logical sessions. Separate integration cases keep two TCP streams or two UDP associations active while the first SOCKS5 hop observes exactly one physical TCP connection. |
| Native UDP through a proxy | Official [socks5UDPAssociate](https://github.com/enfein/mieru/blob/v3.33.0/pkg/appctl/proto/clientcfg.proto#L87) supplies a packet dialer. Zero now accepts an adapter-built packet-path/datagram carrier for a native Mieru UDP final hop; the SOCKS5 UDP ASSOCIATE to Mieru chain is covered by a two-packet integration test. A byte-stream-only hop remains explicitly unsupported because it does not provide the required datagram contract. |

## Qualification still outstanding

- Comparative throughput, sustained weak networks, long-running operation,
  server restart and network changes have not been qualified.
- Per-underlay diagnostics need further assessment and improvement; this is an
  operational assessment, not an additional wire-format defect count.
- Remote CI, release artifacts and installed-node verification remain separate
  from local source tests. The reproduced Connector failures still block the
  workspace gate.

The earlier implementation history and validation evidence are in
[multiplex.md](multiplex.md). These local relay-chain cases do not replace the
remaining official application-mapping and long-running qualification work above.

## Capability implementation acceptance

CUBIC, runtime MTU/traffic patterns and bounded TCP consumer backlog are validated
separately from the previous hardening run. Current completed checks:

- Mieru unit tests: 44 passed, including CUBIC, a virtual 1.5s RTT link with
  one to three dropped DATA packets, MTU wire budgets, traffic patterns,
  consumer deadlines, cache limits and scheduling fairness.
- Pinned official v3.33.0 interop: 8 passed, covering inbound/outbound TCP/UDP,
  default and custom MTU/traffic profiles, concurrent sessions and carrier reuse.
  The reference reports commit `48ddb69d5d343d76c9004c5054ee36609579ba13`.
- Config integration tests: 4 passed. Earlier package-focused validation passed
  78 tests before the two additional consumer regression cases.

The first interop run found a real backlog dispatch defect: healthy TCP sessions
could exceed the frame cap while freed delivery slots waited for a periodic
drain. Per-session opportunistic drain and bounded cooperative scheduling fixed
it without increasing receive limits; both failed interop cases now pass.

Final `cargo fmt --all`, `cargo check --workspace` and
`cargo clippy --workspace --all-targets -- -D warnings` passed. The full suite
with all features and a 16 MiB test-thread stack compiled successfully, then was
stopped after the user challenged the verification scope and cost. At interruption,
39 completed tests had passed and none had failed; the Mieru package tests in this
full-suite invocation had not run yet. This is an incomplete workspace gate, not
a passing full run, and the historical Connector failures were not rechecked by
this invocation.

Local evidence: `/tmp/mieru-capabilities-reader-regression.log`,
`/tmp/mieru-capabilities-interop-3330.log`,
`/tmp/mieru-capabilities-check.log`, `/tmp/mieru-capabilities-clippy.log`,
`/tmp/mieru-capabilities-workspace.log` and
`/tmp/mieru-capabilities-validation.json`.

## Relay carrier and pool acceptance, 2026-09-10

This follow-up completed the two prioritized relay-carrier paths without changing
the v3.33.0 baseline:

- A native Mieru UDP final hop can consume an adapter-built datagram carrier. Two
  simultaneous SOCKS5 UDP associations remained isolated while the first hop
  observed one physical datagram carrier.
- A Mieru TCP final hop defers prefix dialing until its protocol pool decides a
  new carrier is needed. Both two concurrent TCP CONNECT streams and two
  concurrent UDP ASSOCIATE sessions reused one physical TCP relay carrier.
- Pool growth and retirement remain Zero-owned policy: least-loaded selection,
  75% scale-out threshold, four carriers per identity, 30-minute age retirement,
  active-stream draining, and reload generation isolation.

The focused local acceptance passed: Mieru underlay **10/10**, SOCKS5 TCP
**19/19**, SOCKS5 UDP **26/26**, runtime boundary **185/185**, and export
contract **10/10**. `cargo check -p zero-proxy --all-features` also passed.
These are source-level local results; official-reference interoperability,
remote CI, release artifacts, installed builds and long-running recovery were
not rerun by this follow-up.
