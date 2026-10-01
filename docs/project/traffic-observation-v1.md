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
| outbound tag | Live final-executor Flow observations, while active and after completion; failed registered flows | Shared-device inner role split and per-hop carrier unavailable | TCP / UDP logical flows, including relay membership; observed Packet pins |
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
- Outer RX: UDP datagram payload received at the protocol carrier boundary,
  including rejected/unidentified datagrams at the endpoint. Peer RX requires
  authenticated identity. Peer RX therefore need not sum to endpoint RX.
- Outer TX: successfully handed-off UDP payload, including handshake, rekey,
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
L3 paths, per-hop carriers, shared outbound-role inner measurements, OS-wide
loss, and TCP packet measurements remain unavailable.

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

Remaining **provider limitations**, rather than fabricated zeros: arbitrary
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

There was no second complete workspace run after these fixture corrections; the
first run must not be described as a single green gate. The only subsequent
production-source edit removed a redundant `Default` struct update after all
three activity fields had already been specified. A source digest check confirmed
that restoring just that no-effect line reproduces the full run's runtime source.

Current-source checks:

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
