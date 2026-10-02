# Traffic observation V1

Kernel contract; clients render and retain curves. Connector optionally filters
and delivers the same `zero.event.v1` events. Neither component reconstructs the
kernel's authoritative cumulative totals. This contract does not introduce usage,
billing or quota semantics.

## Sources and ownership

| Scope | Flow | Inner / outer | Immediate activity |
|---|---|---|---|
| global | Existing EngineStats live monotonic upload/download claims; failed registered flows | Actual raw-IP device boundary totals; outer unavailable | TCP / UDP logical flows; observed Packet pins |
| inbound tag | Existing observed RX/TX business boundary, while active and after completion; failed registered flows | TUN host IP I/O; admitted raw-IP ingress and its responses; outer unavailable | TCP / UDP logical flows; observed Packet pins |
| outbound tag | Live final-executor Flow observations, while active and after completion; failed registered flows | Correlated raw-IP outbound role; observed physical/logical carrier boundaries per hop | TCP / UDP logical flows, including relay membership; observed Packet pins |
| explicit endpoint_id | Flow direction claims deduplicated across inbound/outbound use of the same resource | WireGuard neutral raw-IP device boundary | TCP / UDP flows deduplicated by ID; observed Packet pins |
| stable peer_id | Protocol-supplied inbound/outbound Flow bindings with deduplicated business direction claims; failed registered flows | WireGuard identified inner packets and outer datagrams | Bound TCP / UDP flows; observed Packet pins |

Only explicit `endpoints` create resource/peer statistical scopes. Legacy
WireGuard configuration keeps its existing endpoint catalog but is not silently
promoted into the new explicit-resource identity contract. Normal inbound and
outbound tag scopes remain available. Built-in direct execution has outbound tag
`direct`.

Configured ingress roles include the `runtime.tun` tag. A control-command-started
TUN registers its actual role for the ingress operation lifetime, even when the
tag is not present in configuration. Overlapping registrations share one meter;
reload retains live registrations. The last teardown retires an unconfigured
role, and a later same-tag operation starts a fresh epoch. Configured roles remain
queryable after stop. TUN inner measurements start when its I/O observer attaches. RX observes an actual
device read; TX observes a completed packet write, rather than admission into an
async queue. macOS/Linux use the neutral channel bridge; Windows observes native
Wintun receive/send operations. Partial/failed writes do not count as successful
TX packets. Fatal I/O failures and observed discarded queued packets are counted.
Platform framing (for example macOS address-family prefixes) is excluded. These
measurements do not count packets already read before observation attached. TUN
outer metrics are inapplicable and remain unavailable.

Protocol code supplies stable, validated public peer identities and authentication
facts. Its adapter projects them into neutral raw-IP operations. Atomic meter
handles and per-session claims contain no protocol parsing. Engine owns current
scope inventory, aggregate counters, baselines, snapshots, reset transactions and
event projections. Proxy owns I/O instrumentation and sampling task lifecycle.
No metering/query/reset dependency on Connector is introduced.

`StatsSnapshot` stays compatible: global business direction is the per-session
maximum of mirrored boundaries; legacy `per_outbound` remains completed-only.
Its labels follow the current configuration; retiring a tag prunes that index and late completions do not
resurrect it. Global business/usage counters are retained.
New role counters reuse those observation boundaries without waiting for close.
They retain existing negotiation/boundary byte semantics, rather than becoming
physical carrier or billing measurements. An outbound Flow is attributed to the
final executor; its bytes are never copied onto every relay hop as wire traffic.

Native Packet forwarding does not manufacture a Flow or increment Flow billing
bytes. It is observed at TUN/endpoint device boundaries. A shared
endpoint is counted once per device-boundary observation, regardless of how many
inbound/outbound aliases or business paths refer to it.

### Outbound Inner and per-hop Outer providers

`traffic_outbound_inner_role_io_v1` declares the neutral raw-IP role provider
when raw-IP runtime support is compiled. `traffic_outbound_carrier_io_v1`
declares prepared carrier observation. These are provider capabilities, not a
promise that every configured tag already has every metric: inspect each
snapshot's `available_metrics`, `source_roles` and `accounting_basis`.

- Outbound Inner TX counts IP fragments accepted by the selected raw-IP device.
  Packet queue admission alone does not count. Per-flow provenance follows TCP
  SYN/ACK/FIN/RST, control packets, retries, UDP fragments, native Packet forwarding
  and the opt-in Echo translation adapter.
- Outbound Inner RX counts matched replies after reassembly at the local TCP/UDP
  stack or native Packet return boundary. It includes matched TCP control/error
  packets, UDP replies/errors and translated Echo replies. A fragment received
  at the endpoint is already included in endpoint/peer Inner RX; the role's
  post-reassembly packet count has a different boundary and need not sum to that
  device count. Unmatched device traffic is not assigned to an arbitrary alias.
- Native Packet return accounting uses the pure IP conversation key, not only
  the source IP. The shared device retains at most 4096 observation associations,
  expiring after 300 seconds idle. At observation capacity, forwarding continues
  and the affected role RX metrics become unavailable for that counter-source
  lifetime; a partial count is not reported as a complete total. Existing return
  route admission limits remain independent. Closing the device clears associations. A rejected overlapping ingress cannot
  replace an accepted conversation's observer. This is return correlation, not
  a second statistics registry.
- Endpoint/global Inner remains one observation per actual device boundary.
  Inbound and outbound role snapshots are independent projections; do not sum
  role aliases to reconstruct device totals. Outbound role Inner drops/errors
  remain unavailable without a complete role-specific rejection source.
- The first proxy hop's Outer observes actual socket I/O below transport and
  protocol encapsulation. Later TCP hops observe their logical stream carrier
  after the prefix has opened, before that hop's transport/protocol wrapping.
  UDP codec hops observe encoded datagrams supplied to the preceding packet
  carrier. Thus every hop records its own carrier bytes, including its framing,
  instead of copying final-executor Flow bytes onto all hops. A logical hop is
  not a second physical host socket and does not include preceding-hop headers.
  Scope identity remains the outbound tag: reuse of a tag across paths or hop
  occurrences aggregates its owned carrier observations. There is no additional
  per-route-hop registry, and these totals are not unique host-wire usage.
- Prepared protocol paths reuse the same observer in native TCP/UDP sockets,
  QUIC/mKCP datagram factories, port rotation, connected datagram relay drivers,
  stream relay connectors and protocol-owned pools. Pooled carriers count once
  at their owner tag. Authentication, transport handshake, keepalive, protocol
  retransmission and encapsulation are included when they traverse that observed
  carrier; host IP/TCP/UDP headers and host TCP retransmission are excluded.
- Stream Outer exposes bytes and surfaced read/write errors. Datagram Outer
  additionally exposes actual datagram counts, including GSO/GRO normalization.
  Pending/WouldBlock calls and queued relay admission are not successful sends.
  If the same statistical tag also uses stream I/O (including SOCKS control
  streams), its Outer packet metrics become unavailable for that counter-source
  lifetime; a partial UDP count must not masquerade as a complete packet total.
  Clearing statistics does not change this coverage limitation.
- WireGuard device Outer remains in endpoint/peer scopes. Shared handshake,
  keepalive and other unassigned device control traffic are not divided among
  outbound aliases; WireGuard outbound-role Outer stays unavailable. A separate
  outer proxy hop can expose its own observed carrier scope. Built-in direct
  execution has no new synthetic protocol-carrier provider. Externally owned
  browser transports and virtual reverse targets without an owned carrier do
  not declare a physical socket measurement. Preparing an observer alone does
  not enable Outer bytes; an observed carrier must actually attach.

Native Packet paths retain their prepared role observer in the existing bounded
conversation pin. Subsequent packets clone that handle without another statistics
registry lookup or observer allocation. Expiry/ingress teardown releases it,
including cached unavailable providers. New Flow/stream/datagram carriers prepare
their handles at connection/carrier construction; hot writes only update atomics.

The providers reuse `traffic_stat`, paginated `traffic_stats`, the `stats.reset`
command, and `stats.scopes_sampled` / `stats.reset` events. Available role/counter planes
participate in the existing independent baseline/epoch reset. Active flows,
resource generations, global business totals, quotas and configuration remain
unchanged. Connector delivery is optional and does not own these counters.

### Metric semantics

- Bytes are bytes; packets are complete observations at the stated boundary.
- Flow global/resource: `bytes_up` / `bytes_down`. Roles: `rx_bytes` / `tx_bytes`.
  Flow packet counts and Flow drops remain unavailable: a TCP read/write chunk is
  not an IP packet. Flow `errors` counts a registered session completed with
  `SessionOutcome::Failed`, once for the global, bound roles, endpoint and peers.
  A mirrored resource is counted once. Cancelled/Blocked sessions and requests
  rejected before session registration are not failed-session errors.
- Endpoint/peer Inner RX: authenticated, source-allowed plaintext IP packets before reassembly.
  Inner TX: packets accepted into the tunnel, after required fragmentation, before
  encryption. Queued packets awaiting handshake count at this boundary once.
- Endpoint/peer Outer RX: UDP datagram payload received at the protocol carrier boundary,
  including rejected/unidentified datagrams at the endpoint. Peer RX requires
  authenticated identity. Peer RX therefore need not sum to endpoint RX.
- Endpoint/peer Outer TX: successfully handed-off UDP payload, including handshake, rekey,
  cookie responses where identifiable, keepalive and protocol retries. It excludes
  host IP/UDP headers and encapsulation performed by an outer proxy carrier.
- `dropped_packets`: explicit runtime boundary rejections (decode/source/fragment
  validation where observed). `errors`: surfaced boundary I/O/encode/timer failures.
  These are not all operating-system, network-loss or opaque protocol internal
  queue drops. Those broader loss measurements are unavailable.
- EndpointCounters' legacy `dropped_packets` projects inner boundary rejections;
  use the new per-plane counters to keep inner/outer separate.
- Global Inner is the sum of **actual device-boundary observations**, not the
  global business byte total and not a query-time sum of endpoint snapshots. The
  same business packet traversing both TUN and WireGuard is observed once at each
  distinct device. Linked WireGuard inbound/outbound aliases share one physical
  observation and do not double count that device. Resetting a device/role/peer
  never resets the global baseline.
- Raw-IP inbound role Inner RX counts admitted remote input after reassembly
  when already handled outbound replies have been excluded. TX counts accepted
  response fragments. The role does not take credit for a shared device's
  outbound requests or handshake traffic. Its drop/error metrics remain
  unavailable where no role-specific failure boundary is observed.
- Peer Flow provenance travels in existing bounded TCP connection state and UDP
  datagram queues. UDP flow identity includes the opaque peer identity. A TCP
  flow retains the authenticated SYN identity; another identity cannot reuse or
  reset that tuple. The Engine only binds identities published by the current
  validated protocol source; it never derives them from outer source addresses.
  Flow `source_roles` lists the roles actually attached to this source (not
  allowed directions). An empty list on other planes does not declare absence
  of traffic. Peer Flow counters/activity become available when that provider
  first binds a tracked flow; absent providers stay null.
- `inner` and `outer` are never added together. RX/TX describe resource receive/send;
  inbound/outbound describe execution roles, not interchangeable directions.
- `null` is unavailable. `0` is an available, measured zero. `available_metrics`
  and `resettable_metrics` explicitly enumerate what the scope can expose.

Packet activity counts bounded native/translated conversation pins to a selected
Packet endpoint, not protocol UDP sockets or fabricated connections. Flow pins
are excluded. Pins expire after 600 seconds idle (TUN cleanup at most 30 seconds
later); ingress teardown also releases them. If no pin provider has observed the
resource, activity is unavailable. Native/translated Packet pins now retain
global, inbound/outbound roles, explicit resources and available authenticated
peer memberships, deduplicated by statistical identity. Input identity is part
of the conversation key, and a changed target peer replaces its old memberships.
Removing/expiring pins releases activity without altering cumulative bytes or
observation epochs. Flow pins do not increment Packet activity. Direct Echo
probe correlation is not included in this pin-provider count. Unregistered host
L3 paths, OS-wide loss, and TCP carrier packet measurements remain unavailable.

## Queries

Reuse `QueryRequest` with `QueryService`, IPC, FFI JSON or gRPC JSON payloads.
HTTP additionally accepts **POST /api/v1/query** with the same typed request,
Read permission and normal `ApiResponse` envelope. Existing GET /api/v1/stats and
endpoint queries retain their shapes; endpoint snapshots add nullable `stats_epoch`
and `stats_epoch_started_at_unix_ms`, and project the same inner/outer counters.

Single scope:

```json
{"traffic_stat":{"scope":{"kind":"endpoint","endpoint_id":"endpoint:wg-a"}}}
```

Batch/paged (default 64, limit clamped to 1..256):

```json
{"traffic_stats":{"offset":0,"limit":64,"scopes":[]}}
```

An empty `scopes` list selects all current identities. To select roles/peers:

```json
{"traffic_stats":{"scopes":[
  {"kind":"inbound","tag":"socks-in"},
  {"kind":"outbound","tag":"wg-a"},
  {"kind":"peer","endpoint_id":"endpoint:wg-a","peer_id":"wireguard:<canonical public key>"}
]}}
```

Discover endpoint_id / peer_id from returned traffic_stats scopes or endpoint
details; treat them as opaque identities rather than synthesizing protocol IDs.
Peer statistical scopes become available when a validated runtime source is
published. A configured but never-started peer need not have a statistical scope.

At most 256 selected identities. Use `next_offset`; subsequent pages should echo
`expected_core_instance_id`, `expected_config_revision` and
`expected_registry_revision` from the first page. A changed inventory returns
`conflict`; restart pagination. Removed identities return `not_found`.

Each snapshot includes scope, core_instance_id, config_revision, applicable
resource generation, stats_epoch, epoch start, capture start, sample wall time and
sampled_at_monotonic_ns. The monotonic clock is local to the scope's counter source;
compare it only for the same instance/scope/epoch. Resource generation is the
existing applied endpoint lifecycle generation. It is independent of the
observation period and does not promise a new device for every config edit.

For available counters, derive bytes/second as:

```
(current_counter - previous_counter) * 1_000_000_000 / monotonic_elapsed_ns
```

Discard the baseline on instance/epoch change, unavailable metrics, nonpositive
time or counter decrease; requery on generation change. Wall milliseconds place
points on a chart; the monotonic interval avoids wall-clock-adjustment rates.
Counters are atomic individually, not one simultaneous whole-network read.
`capture_started_at_unix_ms..sampled_at_unix_ms` declares the capture window.
Activity is captured separately and is not a resettable cumulative value.

## Reset command

Admin permission, existing CommandService / POST /api/v1/commands / IPC / gRPC:

```json
{"method":"stats.reset","params":{
  "expected_core_instance_id":"<queried instance>",
  "operation_id":"optional-correlation-id",
  "targets":[{
    "scope":{"kind":"endpoint","endpoint_id":"endpoint:wg-a"},
    "expected_stats_epoch":"<queried epoch>",
    "expected_generation":3
  }]
}}
```

Require 1..256 explicit, nonduplicate identities. Supply the exact observed epoch
and, when nonnull, generation. Validate every target before changing any baseline.
Each scope resets **all available cumulative metrics**; metric subsets are not
supported. Unknown parameters such as `metrics` are rejected. A scope with no
available cumulative metrics returns `unsupported`.

A global target only resets the global observation view. It never implicitly
resets inbound/outbound/endpoint/peer scopes. An explicit batch is atomic with
respect to observation period management; large inventories require explicit
batches. There is no wildcard or implicit cascade.

Internal counters remain monotonic. Management serializes snapshot baseline
selection, reset and source publication; hot writers continue using atomics.
Each counter's baseline linearizes at its own atomic load inside the declared
capture window, so concurrent traffic can already be nonzero in the acknowledgement.
No bytes are lost by zeroing an underlying counter. A multi-counter reset is not
claimed to be a globally simultaneous packet instant.

Reset does not restart/stop resources, close connections, change directions,
configuration or permissions, reset activity/health, or touch connection cumulative
values, billing/usage/quota/audit state. Resetting one resource leaves all other
views unchanged.

`CommandResponse.result` contains operation_id, core_instance_id and the actual
new snapshots/epochs/start times. HTTP adds its existing outer ApiResponse wrapper.
The operation_id and request envelope ID remain correlation identifiers; neither
is a deduplication key. **Epoch compare-and-swap is retry protection**: replay of
an old successful request returns conflict and does not reset again. After a lost
acknowledgement, query the scope and event replay to recover; do not automatically
retry with a newly obtained epoch, which would request another reset.

Errors use existing codes and envelopes: permission_denied; conflict with fields
such as params.expected_core_instance_id, params.targets[0].expected_stats_epoch or
expected_generation; not_found; invalid_argument; unsupported. Runtime adapter
acknowledged commands also serialize with configuration/endpoint apply.

## Sampling, replay and Connector

Proxy runtime automatically samples legacy stats/flow updates every second.
`stats.scopes_sampled` carries one current `TrafficListSnapshot` page of at most
64 scopes per second, round robin. Under 64 scopes, each curve has roughly a
one-second interval; larger inventories have a longer per-scope interval. Use the
actual snapshot timestamps, not an assumed one-second denominator. Missed timer
intervals are skipped; no catch-up burst or per-packet event exists.

`endpoint.stats_sampled` projects the same resource counters/epochs, one bounded
page every ten seconds. `stats.sampled` keeps its legacy business payload.
Engine-only embeddings explicitly call the sampling methods; automatic sampling
is declared false there. In-process event subscription exists without Connector.
HTTP/IPC/gRPC subscription availability follows the compiled transport capabilities.

`stats.reset` is published in the same serialized period transaction, with the
acknowledgement's scope snapshots. Clients drop the affected curve baselines and
restart them using those epochs. Event IDs include a fresh observation epoch;
reusing a correlation operation_id cannot alias a distinct reset. Sample event IDs
also have unique kernel-generated identities. Counters can advance after the reset snapshot;
activity is preserved.

Events keep the existing `zero.event.v1` envelope, sequence, instance/config
metadata, bounded history and replay-gap indicators. Live subscribers use bounded
nonblocking delivery; missing samples do not block I/O. On disconnect, sequence
gap, restart or epoch change, query a fresh batch and rebuild baselines. Capture
and delivery are separate: an older captured sample can still be queued when a
reset acknowledgement arrives. A query or reset acknowledgement establishes the
current period; discard queued samples from superseded epochs/instances and
non-increasing monotonic sample times within the current period. An unknown epoch
requires a fresh query before replacing that baseline. Event sequence is delivery
order, not a simultaneous counter-capture clock. Kernel retains no unlimited time
series. Statistics events have an 8 MiB serialized
envelope budget in replay, in addition to the configured event-count limit;
eviction removes a prefix and preserves gap detection. Each live subscriber has
a 1 MiB serialized statistics event budget in addition to its 1024-slot queue. Filtering
occurs before enqueue. A full queue sheds observations; consuming or dropping
the queued value releases its reservation. These budgets are not a claim about
all other event types or external Connector outbox storage.

Connector configuration uses its existing channel registry and filtering:

```json
{"api":{"event_sinks":[{
  "type":"webhook",
  "tag":"traffic-observer",
  "url":"https://receiver.example/complete-webhook-address",
  "events":["stats.scopes_sampled","stats.reset"]
}]}}
```

Use the normal Connector sink registration/configuration, delivery acknowledgement,
retry/outbox and dead-letter contract. Delivery does not grant command permissions
or create a second management, billing or client-specific API.


### Transport framing and event payloads

- HTTP POST `/api/v1/query` returns `ApiResponse.result` as a direct
  `TrafficSnapshot` or `TrafficListSnapshot`.
- In-process QueryService and gRPC JSON payloads retain the QueryResponse
  `traffic_stat` / `traffic_stats` discriminator.
- FFI preserves its existing serialized Rust Result wrapper: `Ok` contains that
  QueryResponse, while `Err` contains the API error. FFI has no new event callback
  in this change; its host can poll these snapshots.
- IPC uses its normal line frame and preserves that QueryResponse inside
  `ApiResponse.result`:

```json
{"type":"query","id":"traffic-1","request":{"traffic_stats":{"offset":0,"limit":64}}}
```

IPC subscription:

```json
{"type":"subscribe","id":"traffic-events","events":["stats.scopes_sampled","stats.reset"]}
```

HTTP SSE: `/api/v1/events/stream?types=stats.scopes_sampled,stats.reset`.
Use the existing `since` / `Last-Event-ID` recovery contract. IPC consumers without
transport replay use a fresh paged statistics query on reconnection.

| Event | Typed payload | Consumer action |
|---|---|---|
| stats.scopes_sampled | TrafficListSnapshot | Update only the included scopes; derive rates within the same epoch |
| stats.reset | StatsResetSnapshot | Clear curve baselines/history only for the returned scopes |
| endpoint.stats_sampled | Existing endpoint sample payload, with counters and epoch fields | Compatibility projection of the same endpoint meter |

Events carry the normal `schema_id=zero.event.v1`, event_id, event_type,
core_instance_id, config_revision, sequence and payload. Statistics event metadata
uses the captured statistics revision; its payload identity/epoch remains the
consumer's authority. A page is not a declaration that other scopes disappeared.

## Capability discovery

Look for traffic_observation_v1, traffic_period_reset_v1 and
traffic_scopes_sampling_v1, plus `capabilities.traffic_statistics`, rather than
product version. The structured declaration includes query/command/event names,
Admin requirement, exact reset policy, scope kinds, page/reset limits, sampling
strategy/interval, retry policy, capture consistency and bounded-history policy.
Individual snapshots remain authoritative for metrics available in that build
and currently attached source. WireGuard remains an opt-in protocol feature;
peer counters require its protocol source. No protocol-private facts are simulated
when that source is absent.

Structured discovery example (abridged; read actual capabilities):

```json
{"traffic_statistics":{
  "contract_version":1,
  "queries":["traffic_stat","traffic_stats"],
  "reset_command":"stats.reset",
  "reset_permission":"admin",
  "resettable_scopes":["global","inbound","outbound","endpoint","peer"],
  "reset_policy":"all_available_cumulative_no_cascade",
  "events":["stats.scopes_sampled","stats.reset"],
  "automatic_sampling":true,
  "sample_interval_ms":1000,
  "sample_page_size":64,
  "maximum_page_size":256,
  "maximum_reset_targets":256,
  "sampling_strategy":"round_robin_bounded_pages"
}}
```

## Verification

Implementation tests cover live and completed TCP/UDP role claims, mirrored
resource deduplication, active reset, quota continuity, epoch CAS, concurrent writes
and resets, atomic batch rejection, scope isolation, identity retirement and
pagination. Loopback WireGuard tests compare actual native Packet and derived
TCP/UDP Flow boundaries, endpoint/peer counters and live post-reset I/O. HTTP tests
cover query/reset permissions and retry errors. Unobserved metrics listed above
stay unavailable.

Historical qualification for the preceding implementation, 2026-09-30 UTC
(does not qualify the follow-up below):

- `./scripts/test-workspace.sh`: exit 0; 146 test harnesses, 2295 passed,
  0 failed, 155 ignored, 0 filtered; 2053 seconds (34m 13s), including compilation.
  Log: `/Volumes/tool/tmp/zero-traffic-full-final.log`.
- Focused final engine/WireGuard regression: 45 + 8 passed, 0 failed.
- Integration layout: 270 source files, 88 registered targets, 0 errors.
- Layout checker tests: 5 passed; workspace-test wrapper self-tests passed.
- Without Connector: `cargo check --no-default-features --features
  status-api,wireguard,socks5` passed. The same feature set's binary test
  `statistics_http_query_and_reset_enforce_permissions_and_epoch_preconditions`
  passed (1 selected, 56 filtered); it verifies actual HTTP query, permissions,
  reset and stale-epoch replay. This cropped graph has existing unused-import /
  dead-code warnings. Log: `/Volumes/tool/tmp/zero-traffic-no-connector.log`.

- `cargo clippy --workspace --all-targets --all-features -- -D warnings` passed
  (2m 16s). Log: `/Volumes/tool/tmp/zero-traffic-final-clippy.log`.
- `cargo check --workspace` passed (59.82s).
  Log: `/Volumes/tool/tmp/zero-traffic-final-check.log`.
- Final `cargo fmt --all --check`, integration layout and `git diff --check` passed.

Ignored external interoperability, privileged/platform and sustained-operation
tests are not production acceptance. No client changes, commit, push or release
are included in this change.


## Follow-up implementation, 2026-10-01

Additive discovery flags: `traffic_failed_flow_counters_v1`,
`traffic_peer_flow_bindings_v1`, `traffic_packet_pin_activity_v1`; supported
proxy platforms additionally declare `traffic_host_tun_io_v1`. These describe
mechanisms, not a promise that every source/metric is currently available.

Adds successful host TUN I/O, global raw-IP device-boundary totals, admitted
raw-IP ingress role counters, both-direction stable peer Flow bindings/activity,
registered Flow failure counters, and expanded existing Packet pin memberships.
No client, Connector delivery model, quota or billing state was changed.

The same query/reset/sample contracts cover the new available metrics; no new
statistics endpoint or protocol-specific command is required. For example,
POST `/api/v1/query` can request one page containing all current providers:

```json
{"traffic_stats":{"offset":0,"limit":64}}
```

Or select current identities without per-endpoint polling:

```json
{"traffic_stats":{"scopes":[
  {"kind":"global"},
  {"kind":"inbound","tag":"tun-in"},
  {"kind":"endpoint","endpoint_id":"endpoint:wg-a"},
  {"kind":"peer","endpoint_id":"endpoint:wg-a","peer_id":"<published-peer-id>"}
],"offset":0,"limit":64}}
```

Use actual catalog/query identities, not a guessed TUN tag or parsed opaque
resource ID. Inspect `available_metrics`, `accounting_basis`, and Flow
`source_roles`; derive curves within the snapshot's `stats_epoch`. Reset these
providers through the existing Admin `stats.reset` CAS transaction. Global is
an explicitly independent observation scope; it never cascades to its sources.

At the end of the 2026-10-01 implementation, remaining **provider limitations**
were: arbitrary
protocol/hop carrier socket instrumentation, a correct per-alias inner split
of shared outbound devices, and protocol-internal/OS-wide loss. TCP Flow packet
counts have no defined packet boundary. Peer identities for protocols lacking a
stable authenticated identity source stay unavailable. Privileged real-device
qualification and Windows execution require their own environments.

### Follow-up verification

The complete workspace/all-feature run executed all 146 harnesses, including
doctests: **2301 passed, 2 failed, 155 ignored**, exit 101, 2795 seconds
(2026-10-01T04:03:24Z through 04:49:59Z).
Log: `/Volumes/tool/tmp/zero-traffic-followup-full3.log`.

Both failures were test fixtures: an activity-only Packet scope incorrectly
expected reset success despite having no available cumulative metrics, and an
exact capability-list assertion omitted the four new declarations. The fixtures
were corrected; the complete affected targets were rerun:

- Proxy unit target: **278 passed, 0 failed, 1 ignored**.
  Log: `/Volumes/tool/tmp/zero-traffic-followup-corrected-proxy-unit.log`.
- Proxy control target: **33 passed, 0 failed**.
  Log: `/Volumes/tool/tmp/zero-traffic-followup-corrected-proxy-control.log`.
- WireGuard integration target: **9 passed, 0 failed**, rerun after moving its
  duplicated host helper into the shared suite module.
  Log: `/Volumes/tool/tmp/zero-traffic-followup-corrected-wireguard.log`.

On 2026-10-01 there was no second complete workspace run after these fixture corrections; the
first run must not be described as a single green gate. The only subsequent
production-source edit removed a redundant `Default` struct update after all
three activity fields had already been specified. A source digest check confirmed
that restoring just that no-effect line reproduces the full run's runtime source.

Checks recorded on 2026-10-01:

- Final `cargo fmt --all --check`, `git diff --check`, and integration layout:
  passed (270 source files, 88 registered targets, 0 layout errors).
- `cargo check --workspace`: passed (17 seconds after the initializer cleanup).
  Log: `/Volumes/tool/tmp/zero-traffic-followup-workspace-check.log`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
  Log: `/Volumes/tool/tmp/zero-traffic-followup-clippy.log`.
- Without Connector, `cargo check --no-default-features --features
  status-api,wireguard,socks5`: passed; the dependency graph excludes
  `zero-connector`. The cropped graph still emits unused/dead-code warnings.
  Log: `/Volumes/tool/tmp/zero-traffic-followup-no-connector-check.log`.
- The same cropped build's real HTTP query/reset permission and stale-epoch test:
  **1 passed, 56 filtered**.
  Log: `/Volumes/tool/tmp/zero-traffic-followup-no-connector-http.log`.
- `cargo check -p zero-tun --target x86_64-pc-windows-gnu`: passed (2m 32s).
  Log: `/Volumes/tool/tmp/zero-traffic-followup-windows-tun-check.log`.

The integrated tests cover active/completed TCP and UDP, stable peer provenance,
shared-resource claims, native Packet pins, source retirement, reset isolation,
concurrent reset preconditions, usage/quota preservation, and bounded slow-consumer
recovery. Windows cross compilation is not native Wintun execution; privileged
real TUN, external peers and sustained-operation acceptance remain separate.


## Outbound carrier and shared-device role follow-up, 2026-10-02

This follow-up implements the first two provider gaps recorded above. The source
and metric sections define the boundaries and current exclusions. It adds no
statistics transport, protocol-specific control command, Connector dependency or
client-side authoritative accumulator.

Clients may batch final Flow, shared-device role Inner and each carrier hop:

```json
{"traffic_stats":{"scopes":[
  {"kind":"outbound","tag":"first-ss"},
  {"kind":"outbound","tag":"final-socks"},
  {"kind":"outbound","tag":"wg-a"},
  {"kind":"endpoint","endpoint_id":"endpoint:wg-a"}
],"offset":0,"limit":64}}
```

Use tags and IDs returned by the active inventory. `first-ss` and `final-socks`
illustrate a configured relay, not mandatory protocol naming. Outer follows
actual hop I/O while Flow keeps its existing final-executor semantics. The same
`stats.reset` target/epoch CAS can independently clear an outbound's available
Inner/Outer observations while its live connections and endpoint counters remain.

New coverage regression includes real TCP/UDP relay hops, real QUIC datagrams,
WireGuard active TCP/UDP and bidirectional role separation, raw-IP fragment and
control-packet provenance, correlated UDP overflow, rejected overlapping ingress,
bounded observation pressure, partial stream writes, connected relay queue
admission, batched GSO/GRO and TLS handshake/ciphertext/raw-handoff observation.
Verification results are recorded separately from implementation claims below.

### Verification recorded on 2026-10-02

- Final complete gate, `bash scripts/test-workspace.sh --jobs 2`: **146 harnesses,
  2335 passed, 0 failed, 155 ignored**, exit 0, 3428 seconds. The script ran the
  workspace/all-feature unit, integration and doctests with the required 16 MiB
  test-thread stack. UTC start/end: `2026-10-01T16:59:07Z` / `17:56:15Z`.
  Log: `/Volumes/tool/tmp/zero-stats-outbound-full-final-20261002.log`.
- This final gate includes the admitted-path observer cache and its expiry,
  unavailable-provider and ingress-peer isolation regression. Proxy unit tests:
  **287 passed, 1 ignored**. WireGuard integration: **9 passed**.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed,
  105 seconds. Log: `/Volumes/tool/tmp/zero-stats-outbound-clippy-final-20261002.log`.
- `cargo check --workspace`: passed, 121 seconds.
  Log: `/Volumes/tool/tmp/zero-stats-outbound-check-final-20261002.log`.
- Without Connector, `cargo check --no-default-features --features
  status-api,wireguard,socks5`: passed, 76 seconds. The same graph's real HTTP
  query/reset permission and stale-epoch test: **1 passed, 56 filtered**, exit 0;
  test build/execution took 130 seconds. The cropped graph still emits 303
  unused/dead-code warnings; they were not suppressed. Normal dependency-tree
  inspection confirms `zero-connector` is absent.
  Logs: `/Volumes/tool/tmp/zero-stats-outbound-no-connector-check-20261002.log`,
  `/Volumes/tool/tmp/zero-stats-outbound-no-connector-http-20261002.log` and
  `/Volumes/tool/tmp/zero-stats-outbound-no-connector-tree-20261002.log`.
- `cargo fmt --all --check`, `git diff --check` and integration layout: passed
  (274 source files, 88 registered targets, 0 layout errors).

An earlier complete-gate attempt was intentionally interrupted during compilation
to remove per-packet observer preparation (220 seconds, exit 130); it is not a
successful gate. Log: `/Volumes/tool/tmp/zero-stats-outbound-full-20261002.log`.
The final source gate above supersedes that attempt. Ignored official-binary,
privileged-TUN and sustained/external-network qualification cases remain
unexecuted. Loopback regression is not production acceptance. This follow-up
does not install a kernel, change a client or publish a release.

## Local discards and optional host interface observation

The loss follow-up retains one Engine statistics registry, period baseline,
Query/Command/Event envelope and bounded replay log. It does not redefine Flow
usage or claim complete operating-system/remote-network loss.

### Observed local discards

`traffic_local_drop_reasons_v1` and
`traffic_statistics.local_drop_reasons=true` declare reason-aware observation.
Each plane adds `drop_reasons` (sparse `{reason, packets}` counters),
`drop_reasons_resettable` and
`drop_coverage="observed_local_boundary_discards_only"` where observed. Reasons
are a fixed, allocation-free data-plane enum, mapped to API values at the proxy
boundary:

- `unspecified`: a legacy observer did not supply a narrower reason.
- `queue_full` / `queue_closed`: an owner actually discarded a packet, not merely
  returned backpressure or transferred the packet into a retry queue.
- `invalid_packet`, `source_rejected`, `fragment_rejected`, `policy_rejected`:
  explicit runtime/protocol boundary refusal. Protocol-private crypto/replay
  details remain protocol-owned and are not guessed from an empty action list.
- `io_failure`: a known packet could not be encoded/sent at its boundary.
- `no_route` / `hop_limit`: a packet was consumed without a forwarding path,
  or its forwarding hop limit was exhausted.

Covered observations include raw-IP input/encode/carrier/source/fragment/policy
rejections (including protocol-reported WireGuard AllowedIPs source rejects),
native/translated Packet return queue refusal, TUN packet routing
and bridge failure/drain, client UDP receive queue saturation/teardown, raw-IP
queued-output teardown, and TCP control retry queue final discard/drain. A
matched reply stays consumed even when its receive queue is full; it must not
become a newly admitted inbound flow. Initial TCP output saturation followed by
successful retry is not a discard. Output queue APIs returning ownership do not
claim a drop; the consuming owner records a final discard. Attribution capacity
pressure that preserves forwarding only marks coverage unavailable.

Reasons are observations, not a promise that every internal queue or protocol
state machine is instrumented. A missing reason is not an observed zero. A
reason remains present with zero after `stats.reset`. Partial role coverage can
have reason counters while `counters.dropped_packets` remains null. Do not sum
reason counters with `dropped_packets`, or sum duplicate global/endpoint/role/peer
projections. Per-counter captures are monotonic and individually atomic;
concurrent writes can make aggregate/reason snapshots differ within the existing
capture window. Reset captures each baseline under the existing period boundary,
without clearing source counters, live flows, devices, usage or quota state.

### Host interface scope and platform sources

The optional Cargo feature `host-network-stats` (root and zero-proxy) enables a
read-only provider independent of Connector. Default builds do not enable it.
On supported platforms, Proxy advertises
`traffic_host_interface_statistics_v1` and
`traffic_statistics.host_interface_sampling=true`, adds `host_interface` to
resettable scopes, and advertises `stats.host_interfaces_sampled`.
The existing traffic statistics contract version remains 1; discovery is
additive. Unsupported platforms do not advertise this provider.

Host interface observations have scope `{"kind":"host_interface","name":"en0"}`
(or a published Linux interface name), exactly one `plane="host"`, no execution
roles, and null Flow/Packet activity. The Host plane is separate from Flow,
Inner and Outer and includes all applications using that interface. It is not a
WireGuard, endpoint, peer, per-hop, business-usage or system-wide total. Physical
and virtual interfaces can observe the same packet at different boundaries;
never sum them into one purported network-loss total.

| Source | Observed fields | Unavailable / accounting qualification |
|---|---|---|
| Linux `/proc/net/dev` | RX/TX bytes, packets, receive/transmit errors and dropped packets | `rx_dropped_packets` is the procfs drop column, which folds `rx_missed_errors` into receive drops; current process network namespace |
| macOS `NET_RT_IFLIST2` / `if_data64` | 64-bit RX/TX bytes/packets/errors; `ifi_iqdrops` as receive queue drops | TX drop packets null; receive queue drops do not claim all OS receive loss |
| Other platforms | No provider advertised | Host counters are unavailable; no fabricated interface identities or zeros |

The Host plane uses `rx_dropped_packets`, `tx_dropped_packets`, `rx_errors`,
`tx_errors`; its legacy undirected `dropped_packets` and `errors` remain null.
Each counter is an OS-published fact with its source-specific `accounting_basis`,
not a harmonized estimate of remote loss. Sources:
[Linux interface statistics](https://www.kernel.org/doc/html/latest/networking/statistics.html)
and [Apple interface structures](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/net/if_var.h).

The provider reads on the existing blocking sampling worker at one-second ticks;
there is no data-path syscall, subprocess, disk write or per-packet JSON. Reads
are bounded to 256 interfaces / 256 KiB, and macOS retries a changing inventory
at most three times. An invalid/oversized/failed batch suspends current host
metrics (null) rather than publishing partial totals or treating a missing page
as resource deletion. A successful complete inventory retires removed labels.
Interface index change, counter decrease/wrap or metric schema change creates a
new observation generation and stats_epoch. Sub-second removal/recreation with
indistinguishable OS identity/counters cannot be proven by a sampled provider.

Initial capture establishes a raw-counter baseline, excluding traffic before
Zero observed the source. Snapshots expose `source_sampled_at_unix_ms` and
`source_sampled_at_monotonic_ns` alongside query capture timestamps. The monotonic
source timestamp belongs to this observation generation; use it for Host rate
deltas and ignore repeat snapshots with the same source timestamp. A failed read
retains the last source timestamp and marks counters unavailable; a successful
read restores availability. Recovery within one source generation retains its
observation epoch and includes accumulated source deltas across the gap.

### Client query, reset and event examples

Legacy default pages and `stats.scopes_sampled` still exclude Host scopes, so old
clients never receive unknown scope/plane variants through those paths. New
clients can query one published name or opt into mixed paginated inventories:
Typed default queries omit `include_host_interfaces=false` during serialization,
so they remain accepted by older kernels with strict query field validation.
Clients must discover support before sending the opt-in field or Host scope.

```json
{"traffic_stat":{"scope":{"kind":"host_interface","name":"en0"}}}
```

```json
{"traffic_stats":{"include_host_interfaces":true,"offset":0,"limit":64}}
```

For selected interfaces, use the existing `scopes` array. Pagination identity
checks (`expected_core_instance_id`, `expected_config_revision`,
`expected_registry_revision`) remain applicable. Query does not synchronously
poll the operating system; source timestamps make freshness explicit.

Admin `stats.reset` accepts the queried host identity with its returned
`expected_stats_epoch`, `expected_generation` and `expected_core_instance_id`.
It returns the confirmed snapshot/new epoch and publishes the existing
`stats.reset` event. It neither clears OS counters nor restarts an interface,
endpoint or flow, nor cascades into any other scope. A host reset uses the last
successfully ingested OS sample as its observation baseline, not a packet-exact
wall-clock OS cut. The confirmed snapshot retains that source timestamp; the
next sample reports deltas from that baseline. Unsupported/unavailable-only
scopes reject reset. Stale/deleted targets use existing Conflict/NotFound errors;
request retry retains the existing epoch-CAS semantics.

`stats.host_interfaces_sampled` carries the existing `TrafficListSnapshot` page
schema, with Host scopes only, at most 64 per tick and an independent round-robin
cursor. Selected-identity pages use direct registry lookups, avoiding a scan of
unrelated endpoint/peer labels on every host sampling tick. Thus 256 interfaces
take up to four ticks to appear. It uses the existing
bounded subscriber/history byte budgets, sequence numbers, replay and filters;
slow consumers cannot block I/O. Connector's generic known-event filter accepts
this event and delivers the existing `zero.event.v1` envelope. No protocol-
specific command, delivery path or second statistics API is introduced.

After restart/generation/epoch change, requery and establish a fresh curve
baseline. After a subscription gap, query current snapshots; cumulative Host
counts recover without replaying every sample. Counters remain unavailable for
unobserved socket queues, driver-specific drops, protocol-private queues and
remote loss. TX minus RX, retransmission, ping timeout or connect failure is
never substituted for an exact dropped-packet count.

### Loss follow-up validation (2026-10-02)

The final runtime source passed the full workspace/all-feature gate. Earlier
interrupted or failed attempts are not counted as qualification. Subsequent
changes only clarified this document.

| Check | Actual result |
|---|---|
| `bash scripts/test-workspace.sh --jobs 2` | Exit 0; 146 test programs, 2352 passed, 0 failed, 155 ignored; 3004 seconds, including compilation and doctests |
| `cargo clippy --workspace --all-targets --all-features --jobs 2 -- -D warnings` | Exit 0; 9m 16s |
| `cargo check --workspace --jobs 2` | Exit 0; 2m 46s |
| Root check with `--no-default-features --features status-api,wireguard,socks5,host-network-stats` | Exit 0; dependency tree contains no `zero-connector` |
| Same root feature set, `--bin zero statistics_http_query_and_reset_enforce_permissions_and_epoch_preconditions` | 1 passed; permissions and epoch preconditions enforced without Connector |
| `zero-proxy --no-default-features --features socks5,wireguard,host-network-stats --test proxy_control host_traffic::` | 1 passed; real macOS source, sampling event, query and reset without Connector |
| `zero-platform-tokio --features host-network-stats --target x86_64-unknown-linux-gnu` check | Exit 0; Linux cross-compilation, not Linux runtime acceptance |
| Format, diff whitespace and test layout | Passed; 276 integration source files aggregated into 88 targets, 0 layout errors |

Full gate timestamps: `2026-10-01T19:47:59Z` to
`2026-10-01T20:38:03Z`. Local evidence logs are under `/Volumes/tool/tmp/`:
`zero-loss-full-accepted-20261002.log`, `zero-loss-clippy-20261002.log`,
`zero-loss-default-check-20261002.log`, `zero-loss-no-connector-check-20261002.log`,
`zero-loss-no-connector-tree-20261002.log`,
`zero-loss-no-connector-http-20261002.log`,
`zero-loss-no-connector-host-20261002.log` and `zero-loss-linux-check-20261002.log`.
These are local verification artifacts, not shipped API state.

The focused no-Connector graph still emits unused/dead-code warnings (including
303 from zero-proxy); its successful build is not a warning-free qualification.
The strict Clippy result above applies to the complete all-feature graph.

Covered regressions include live/reset Flow continuity and usage/quota isolation,
scope isolation and stale/concurrent reset preconditions, host source failure and
recovery, generation/epoch changes and label retirement, bounded inventory and
event pagination, subscriber gaps, WireGuard source rejects, UDP queue refusal
and teardown, and TCP retry versus final discard. The platform tests read real
64-bit macOS counters and exercise the Linux procfs parser.

Ignored external-reference/privileged/browser tests, actual Linux runtime,
long-duration production behavior and unobservable network/driver/protocol loss
remain outside this acceptance evidence. No client modification, commit, push,
release or running-client installation was performed for this follow-up.
