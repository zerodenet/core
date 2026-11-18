# Resource configuration source review

2026-11-17, main checkout. This is an implementation/source review with the
selected default-stack runtime follow-up below. It does not qualify every
resource domain, low-resource condition or platform. TEMP-WG remains open;
the refactor is not marked complete.

The reviewed surface includes 119 top-level RuntimeOptionsConfig fields and
112 effective accessors. These are not 112 independent resource budgets:
the surface also contains routing policy, paths, nested config and probe URLs;
TLS and ordinary File accessors can describe the same configured field with
different omitted defaults. The review also follows event_log_capacity and
udp_upstream_idle_timeout_seconds, which have no effective accessor.

For each domain the relevant questions are admission before actual allocation,
retention across failed/cancelled work, the original retirement condition, and
target-policy installation/publication. A reference to an accessor proves none
of those conditions by itself. The existing owners and transactions remain;
this review introduces no allocator, resource scheduler or replacement SPI.

## Implementation corrections

1. UDP idle policy was cached at Engine construction. Existing Engine clones,
   new UDP activity and runtime output could therefore keep the old value after
   a successful apply. The getter now uses the published snapshot; control and
   status serialization use their captured snapshot. Explicit library builder
   overrides retain their existing semantics. Preparing or rejecting a candidate
   does not install its policy. An already scheduled timeout remains that
   operation's deadline; the next deadline uses the then-current policy.
2. Event-network Epochs charge both Host carrier originals and component task
   registrations. Application pre-retirement and dispatcher replacement used
   to classify only the carrier total. Both totals now select the existing
   paused application preparation, so original native cleanup and Scope
   joins/ACKs precede installation even when the target cap grows. Failure
   continues through the same retained application attempt and compensation.
3. Valid very large UDP retention values could overflow a monotonic Instant in
   association and continuity scheduling. All those scheduling sites now use
   checked_add. A deadline beyond the clock's range has no schedulable expiry;
   it keeps its finite association slot until explicit cancellation/cleanup.
   It never fabricates expiry, releases capacity or certifies native close.
   The unbounded continuity attach helper is now a standalone test fixture;
   production retains attach_bounded and the actual Source limit.

## Domain ownership and replacement paths

| Domain / configured dimensions | Actual admission and retained owner | Failure, retirement and update boundary |
| --- | --- | --- |
| Shared carrier originals, component task total, navigation jobs/requests, QUIC queue packets/bytes | [HostOriginalBudgets](../crates/runtime/src/original_budgets.rs), [recipe accounting](../crates/runtime/src/component/recipe/accounting.rs), [carrier totals](../crates/platform/tokio/src/controlled_carrier/totals.rs) | Concrete original and task registration books retain pending/retiring entries. Recipe accounting identifies affected consumers; catalog quiescence and confirmed StopOld precede installation. Application event-network consumers participate for both shared totals. |
| Process jobs, jobs per component generation | [ProcessBudget](../crates/platform/tokio/src/owned_process/budget.rs), [process declarations](../crates/runtime/src/component/instance/resources/native/process.rs) | Native children and macOS TUN helpers share one book. Failed/pending declarations and unreleased ACKs count. Matching native receipts and original Scope/actor completion precede reuse and limit changes. |
| Direct originals/tasks, taskless native streams, MUX carriers, VMess gRPC originals, Mieru carriers | [Direct assembly](../crates/proxy/src/adapters/direct.rs), [assembly registration](../crates/proxy/src/register.rs), [pooled drivers](../crates/transport/src/pooled_driver/managed.rs) | Cold options declare real finite original/task limits. Complete recipe identities and current weak generations select actual pools; transferred views, original controllers and driver joins settle independently. Existing provider admission gates remain. |
| gRPC pool and adapter-wide carrier/stream totals | [VLESS gRPC assembly](../crates/proxy/src/adapters/vless/component/grpc.rs), actual protocol-owned pools and [native initializer frame](../crates/transport/src/grpc/owned/setup.rs) | Per-pool and total bounds apply together. Cached/binding variants retain the same pool and retirement owner; changed limits participate in actual dependency replacement. The concrete TLS/H2 preparation future is prepaid against the same Host metadata root before boxing and freed before its credit; cancellation retains the original native/controller and Scope obligations. |
| Accepted sockets, datagram associations/peers, per-peer packet queues, pending sends, bind attempts, target sockets, File sessions, plugin probes | [Source limits](../crates/proxy/src/adapters/source_budgets.rs), [source selection](../crates/proxy/src/register.rs) | Actual protocol defaults are preserved when omitted. Limits enter declarations/identities, native reservations and same-Source workers. Source-local change classification drives original Source retirement before rebinding. |
| Per-carrier tasks/resources/in-flight requests; HY2 H3 queues; Metadata probes/attempts/sends and QUIC drivers/users | [work limits](../crates/proxy/src/adapters/inbound_work_budgets.rs), [Metadata classification](../crates/proxy/src/adapters/metadata.rs) | Protocol-selected declarations and accepted Scope retain their finite work. Pending tasks/resources remain charged through real joins and receipts; classification rebuilds affected Sources without granting new parallelism to sequential protocols. |
| Native DNS originals, queries, blocking jobs, H2 drivers; shared DNS wire backing bytes | [native DNS assembly](../crates/proxy/src/component/native_dns.rs), [DNS wire candidate](../crates/proxy/src/runtime/handle/configuration/wire_budget.rs) | Node/bootstrap and Default use the actual query owner/budget. Blocking/native originals and returned-but-live wire buffers remain retained. The wire candidate joins final publication; old DNS/native owners settle before replacement. |
| Raw-IP TCP/UDP/DNS/ICMP work, System TUN TCP DNS/task limits, return routes/conversations/observations, Packet return/transmit/read/metadata storage, Windows rings, network UDP receive queues | [raw work limits](../crates/proxy/src/runtime/inbound_operation/raw_ip/limits.rs), [stack options](../crates/proxy/src/runtime/raw_ip/options.rs), [reconcile barrier](../crates/proxy/src/runtime/orchestration/reconcile/budget_barrier.rs) | Shared Packet roots retain allocations and metadata; stack queues enforce count/byte bounds. Source and device identities capture changed policy. System helpers/native rings/reader tasks keep their actual receipts. TEMP-WG is not a general budget or replacement exemption. |
| File snapshots, TLS cert/key/OCSP refresh, configuration/journal backing | [runtime configuration Source](../crates/runtime/src/configuration/source.rs), [TLS Source assembly](../crates/proxy/src/register.rs) | Ordinary File defaults remain 8 MiB and TLS material defaults 16 MiB. Actual prepared plans freeze their bound. Blocking/native File close and refresh-worker joins are distinct; effective-bound changes rebuild affected TLS Sources. |
| SS TCP salts, legacy replay bytes, UDP replay/senders/associations/return bindings/authenticated codecs, local/adapter pending responses | [security proposal](../crates/runtime/src/configuration/ss_security_budget.rs), [SS assembly](../crates/proxy/src/adapters/shadowsocks/component.rs), [replacement classification](../crates/proxy/src/adapters/security_budgets.rs) | Actual adapter security state and response books remain shared. Capacity pressure does not evict valid security history. Pending/failed response custody and actual native users settle before policy replacement. |
| HY2 entries, UDP logical sessions and pending payload bytes | [HY2 outbound assembly](../crates/proxy/src/adapters/hysteria2/outbound.rs) | Actual pool entries/UDP sessions consume the configured bounds; complete options/identity carry them through reuse and replacement. Native Metadata/QUIC receipts remain separate from logical session return. |
| Control connections/subscriptions, request/response bytes, command/frame counts, IPC/HTTP/SSE/gRPC limits and pipe workspaces | [ControlBudget limits](../crates/runtime/src/configuration/control_budget/limits.rs), [control transaction](../crates/proxy/src/runtime/handle/configuration/control_budget.rs) | One Host book supplies frontend and local-connection credits. Decoding/workspace/encoded output is prepaid. Pending output and original commands retain custody; candidate publication checks the original live book and retained command Scope. |
| Event history capacity/statistics bytes and subscriber registrations/events/bytes/filter storage | [Engine history/subscriber books](../crates/engine/src/observability/event_log/budget/limits.rs), [Engine publication](../crates/engine/src/runtime/configuration.rs) | Live/dequeued/retiring event custody is distinct from history rows. Subscriber/history proposals are rechecked under original guards before metadata mutation. History trimming does not refund a transferred replay's independent custody. |
| Event network tasks/native originals/blocking lookups; logging queues/storage | [event Epoch](../src/application/services/network/epoch.rs), [application transaction](../src/application/services/configuration.rs), [logging integration](../src/application/services/logging.rs) | Actual tasks/lookups/native originals and logging delivery retain their owners. Shared-root changes use early preparation; local event limits rebuild the actual Epoch. Target delivery stays paused until publication; original errors survive compensation. |
| Principal quota storage and persisted snapshots | [quota memory](../crates/runtime/src/configuration/source/quota/memory.rs), [configuration publication](../crates/proxy/src/runtime/handle/configuration/publication.rs) | Actual live maps, keys, serialized backing and writer/fault custody share the Source memory root. The same candidate participates in final publication; native File and writer/Scope retirement remain required independently. |
| Direct UDP sockets, upstream idle/continuity retention, diagnostics operations, Packet route entries | [bound UDP provider](../crates/proxy/src/runtime/udp_dispatch/socket/bound.rs), [UDP lifetime](../crates/proxy/src/runtime/udp_flow/registered/upstream/runtime/association/model.rs), [Host probes](../crates/runtime/src/host.rs), [Packet routes](../crates/engine/src/runtime/packet_route.rs) | Actual original/association/probe admission is finite and on demand. A timer/caller return never certifies native cleanup. Current published policy controls new idle deadlines; count changes retain original admission/replacement checks. |

## Control and metering consumers

| Consumer | Source-reviewed execution boundary |
| --- | --- |
| HTTP queries/responses and SSE | [prepared queries](../src/http_adapter/router/prepared.rs) use actual guarded config/runtime/traffic/endpoint views and the same ControlBudget. SSE output keeps its actual frame/byte custody through write/return. |
| Unix and Windows IPC | [connection query](../src/ipc/connection/query.rs) and its runtime/traffic/endpoint producers use that Host book; Windows native pipe workspaces have a separate configured dimension in the same owner. |
| gRPC | [message preparation](../crates/grpc/src/messages/runtime_query.rs), [traffic views](../crates/grpc/src/messages/traffic_query.rs) and output records retain the original connection, RPC/frame jobs and byte leases. Tonic handoff is distinct from physical H2 closure. |
| Embedded ProxyHandle commands | [embedded execution](../crates/proxy/src/runtime/handle/command/embedded.rs) uses the Host's existing command owner and current frontend custody, rather than an independent executor or control budget. |
| EngineHandle and FFI | [FFI](../crates/ffi/src/lib.rs) intentionally retains its Engine-only API scope; it does not start a Proxy or own HTTP/IPC/gRPC connections. Engine domain limits and published snapshots apply. Caller-owned C payload/string memory is not reported as governed by the hosted frontend budget. The ABI is unchanged. |
| Flow/Inner/Outer, session, endpoint/peer and host-interface metrics | [traffic meters](../crates/engine/src/observability/traffic/meter.rs), [query freezing](../crates/engine/src/traffic_query.rs) and frontend producers retain existing identities, independent planes and reset baselines. Guarded row/workspace/output allocation avoids materializing an unbounded DTO before prepayment. Native bytes and business bytes are not copied across planes as a substitute for actual observations. |

## Regression sources and pre-acceptance status

Added regressions cover idle policy across retained Engine clones, unpublished
or stale candidates, rejected config, embedded restore, explicit library
overrides, unrepresentable retained deadlines and finite N1 continuity. The
application regression uses a real native TCP original and its actual event
Epoch to check shared-task-budget preparation before Host policy installation.

Existing Source-limit, Metadata, inbound work, control budget/publication,
subscriber custody, encoded-query, shared task-root and Packet-memory regression
sources remain applicable. At the end of this source-review snapshot, no
code-level test targets had been run. The subsequent workspace acceptance
pass is recorded separately below. The actual binary build and selected
native runtime work below do not establish
whole-runtime equivalence, platform/privileged or low-resource qualification.
TEMP-WG runtime acceptance remains open.

## Default-stack native runtime follow-up

The 2026-11-17 candidate was built with
`cargo +1.99.0 build --offline --bin zero --all-features -j4`, using the existing
incremental build cache. Its SHA-256 is
`ea2f780e31641e738a428301559c3d73887a8d528bb2af302cabce0e2aea18d5`;
the actual Cargo.lock SHA-256 is
`102425654e9c7bf2f5526634816d2664a3bb2d6e9470d4f3dcfba0a380c66b90`.
The frozen source manifest and lock hash are recorded separately. Later
documentation and formatting-only edits are not represented as compiled inputs;
their separate patch remains beside the runtime evidence.

Real macOS Zero processes used public SOCKS/IPC, actual bound TCP/UDP echo
services and a native Xray v26.3.27 WireGuard peer. Runtime RUST_MIN_STACK was
unset; no worker-stack increase, VM, TUN or system-network mutation was used.

| Actual scenario | Observed result |
| --- | --- |
| Standalone and linked WireGuard, each over SOCKS UDP and SOCKS-to-SS relay | All four modes delivered 48-byte and 1800-byte UDP payloads and actual TCP echoes. Each completed three MTU applies and two outer-process stop/restart/reconnect cycles. |
| UDP idle policy publication | 30, u64::MAX and 60 were applied, read back through public IPC and followed by real traffic. |
| Actual occupied UDP bind during linked replacement | Both linked modes rejected the candidate, confirmed its acquired-original cleanup, rebuilt the last-good graph and resumed UDP/TCP. The focused recovery run also completed both modes independently. |
| First application UDP packet before any remote WireGuard response | Both linked modes retained and delivered the original first packet after the actual peer started, with zero application retries, then delivered the larger payload. |
| Startup interrupted while the actual SOCKS greeting awaited a response | The focused process exited normally in about 21 ms; the full run independently completed this case. The retained original/provider reports were consumed separately. |

The full run recorded 58 successful UDP and 29 TCP round trips; focused
recovery/first-packet runs added 24 UDP and 10 TCP round trips. All 26 processes
from these runs exited with status zero. Actual component-provider close
reports, outer wire-registration release and Source StopOld records were
reviewed independently; process exit and port reuse are not close receipts.
The eight disconnect cycles each recorded an initial UDP timeout before a
subsequent application request succeeded. Recovery is demonstrated; lossless
delivery through the disconnect window is not claimed.

The exposed implementation failures were corrected in their existing owners:
large nested Datagram/DNS/recovery futures are boxed on the same caller;
empty unowned timer placeholders no longer obstruct the owned WireGuard data
queue; provisional native close operations remain pending until their actual
outcome; ended dispatchers return their response lease before peer cleanup
waits for it; unused wire launches are released after failed binds; recovery
selects fresh staged generations before retired current generations and
refreshes the restored listener's plan; actual device preparation observes
the same startup/reload/recovery/network phase cancellation.

Real errors, Unknown outcomes, original provider receipts, transferred-user
return and Scope joins remain separate gates. These selected runs do not close
TEMP-WG-OUTER-UDP-01 or the remaining resource-domain qualification work.

Workspace formatting encountered existing missing cfg(test) modules at
`crates/proxy/src/adapters/vless/component/framed.rs` and
`protocols/http/tests/component/standard.rs`. The changed Rust files were
formatted directly with the workspace's Rust 2021 edition and
`skip_children=true`; those removed fixtures were not recreated. This is not
a successful workspace-format or code-test claim.


## Workspace acceptance follow-up (2026-11-17, in progress)

The user authorized full code-level checks and tests. The working checkout now
includes the nonconflicting upstream nightly version `0.0.3-dev.202611170228`.
Rust `1.100.0` matches the current ordinary CI toolchain. Its current local lock
SHA-256 is `17f08eeea245c1745271bb9c920506cb0da416057c3d731d9beed511551a6bff`.
Removing unused dependencies pruned only orphan packages; the nightly merge
changed workspace package versions without changing external dependency versions.

The integration layout guard (317 sources, 95 targets), test-entry regressions,
original-log evidence collector, runtime dependency guards, `thumbv7em-none-eabi`
no_std checks, release contract and release-script regressions passed on their
recorded inputs. Formatting and whitespace checks passed after fixture repairs.
Strict all-target/all-feature Clippy and the full workspace suite are still in
progress: failed diagnostic passes are retained as failures, and a queued suite
is not a successful test run. A final exact-input result must supersede this
in-progress record before delivery.

Earlier Rust 1.100 native candidates demonstrated real protocol interoperability,
WireGuard restart/replacement and first-payload delivery, HTTP keep-alive/CONNECT,
standard DNS borrowing, Connector retries and accepted-resource limits. Those
binaries precede the latest cleanup and nightly merge. Their original provider
receipts remain separate from process exits; they are not proof for the current
working source. TEMP-WG-OUTER-UDP-01 remains open.
