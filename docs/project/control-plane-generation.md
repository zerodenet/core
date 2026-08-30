# Control-plane generation and recovery contract

Zero identifies runtime facts with two independent values:

- `core_instance_id` is generated when an `Engine` is constructed and remains
  stable for that engine instance. A restart creates a new value.
- `config_revision` starts at `1` and advances once after a configuration is
  fully committed. Rejected applies and applies that roll back listeners or
  application services do not advance it.

Health and runtime snapshots expose both values. Configuration snapshots expose
`config_revision`. A combined status export reads the configuration and runtime
projection from one immutable engine snapshot, so its revisions always agree.
`config.apply` and `config.apply_runtime` acknowledgements are returned only
after reconciliation and include the committed instance and revision.

## Events and cursors

Every event retained by the engine carries `core_instance_id`,
`config_revision`, and a monotonically increasing `sequence`. The sequence is
global within one engine instance and resets for a new instance. A durable
consumer cursor is therefore the pair `(core_instance_id, sequence)`, never a
sequence alone.

`EventReplay.core_instance_id` identifies the instance serving a replay.
`has_gap` means that at least one event after the requested sequence is no
longer retained. Event delivery is at-least-once across reconnect/retry paths;
consumers should deduplicate by `(core_instance_id, sequence)` or by the
qualified `event_id`.

An asynchronous result keeps the revision of the snapshot under which the
operation started. It may therefore be lower than the engine's current
revision if a configuration commit overlaps the operation. This is intentional
and prevents an old result from being attributed to a newer configuration.

## Probe operation correlation

`policies.probe` accepts an optional `operation_id`. When omitted or empty, the
core generates one. The acknowledgement returns:

- `operation_id`: the effective operation that will produce the result;
- `coalesced`: whether the request joined an already running or already queued
  probe cycle;
- `core_instance_id` and `config_revision` at acknowledgement time.

If probe requests overlap, only the first distinct cycle runs. Later requests
receive `coalesced: true` and the first cycle's effective `operation_id`.
Startup and scheduled cycles receive core-generated operation identities.

`policy.probe.completed` is a complete terminal result. Its payload includes
the operation and generation identities, trigger, URL, start/completion times,
terminal status, selected member, and every member's health, latency, stable
error code, and diagnostic message. The envelope revision is the same captured
operation revision.

Synchronous `diagnostics.probe_target` and `diagnostics.probe_outbound` also
accept an optional `operation_id` and return it with the instance, captured
configuration revision, timestamps, terminal status, and result/error facts.
They do not emit a separate completion event.

`diagnostics.probe_outbound` is a neutral, synchronous single-outbound latency
operation. It resolves and probes the requested target against one captured
runtime snapshot, returns `operation_kind: diagnostic_outbound`, and reports
the enforced limit as `timeout_ms`. It shares only the bounded HTTP probe
executor with URLTest. It does not run URLTest policy logic, change a group's
selected member or member health, or emit `policy.probe.completed`. It also
bypasses the shared outbound-health quarantine and does not record success or
failure into the traffic circuit breaker. Both successful and failed results
make this contract explicit with:

```json
{
  "affects_policy_selection": false,
  "affects_outbound_health": false,
  "bypasses_outbound_health_quarantine": true
}
```

Clients can detect this guarantee before probing through the
`diagnostic_probe_health_isolation_v1` capability feature. Older cores without
that feature must treat manual probes as potentially health-affecting.

Failures use stable `error_code` values. Callers should branch on the code and
treat `error` as diagnostic text. The current codes are `invalid_probe_url`,
`target_not_found`, `target_resolution_failed`, `unsupported_target`,
`probe_unavailable`, `probe_timeout`, `empty_response`, `connection_failed`,
`probe_io_failed`, `invalid_probe`, `probe_protocol_failed`,
`outbound_unhealthy`, and the forward-compatible fallback `probe_failed`.

The core writes structured `started` and terminal `completed` records for this
operation. Both records carry `source=core`, method, operation kind,
`operation_id`, `core_instance_id`, captured `config_revision`, target, URL,
start time, and `timeout_ms`. The terminal record also carries completion time,
duration, reachability, terminal status, latency or stable error code, and
diagnostic error text. These are native core observations; controllers do not
need to synthesize completion from request logs.

## Reconnect reconciliation

After reconnect, a client should:

1. Query health and compare `core_instance_id` with its saved cursor and pending
   operations. If it changed, discard the old cursor and invalidate those
   operations.
2. Query the authoritative status/config/runtime snapshots and record their
   `config_revision`.
3. Replay from the saved sequence only when the replay instance matches. If
   `has_gap` is true, discard delta-derived local state and query authoritative
   snapshots again before consuming newer events.
4. Reject asynchronous results whose instance differs from the current health
   snapshot. Attribute a result only to the configuration revision carried by
   that result.

The API additions are additive and deserialize with defaults. Clients can
detect this contract through the `runtime_generation`, `operation_correlation`,
and `event_recovery` capability feature names. With an older core that lacks
them, clients must use the legacy conservative behavior: invalidate pending
operations after reconnect and refresh snapshots instead of trusting cursors.
