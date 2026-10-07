# URLTest selection tolerance

An `url_test` outbound group may configure selection hysteresis in
milliseconds:

```json
{
  "tag": "auto",
  "type": "url_test",
  "outbounds": ["node-a", "node-b"],
  "url": "http://example.com/",
  "interval_seconds": 300,
  "tolerance_ms": 50
}
```

`tolerance_ms` is optional and defaults to `0`, which preserves the previous
strict-lowest-latency behavior. The value is an unsigned 64-bit millisecond
duration.

After every startup, scheduled, or manual cycle, Zero finds the lowest-latency
healthy member. If the current member was healthy in the same cycle, it remains
selected unless:

```text
current_latency_ms > best_latency_ms + tolerance_ms
```

The boundary is strict: a difference exactly equal to the tolerance keeps the
current member. Equal measurements also keep the current member, regardless of
configured member order. An unhealthy or missing current member switches to
the best healthy member immediately. If every probe fails, Zero retains the
current selection. Before any selection exists, the measured best member wins,
with configured order breaking equal-latency ties.

The `policy.probe.completed` payload and the policy snapshot expose a nested
selection decision containing the previous/final selection, best candidate,
current and best latency, tolerance, switch flag, and one stable reason:

- `initial`
- `current_unhealthy`
- `better_beyond_tolerance`
- `within_tolerance`
- `no_healthy_member`

Changing `tolerance_ms` through configuration reload takes effect with the new
committed configuration generation. Manual and scheduled cycles use the same
selection function.

URLTest and `diagnostics.probe_outbound` share the neutral, process-bounded
outbound HTTP probe executor. Every policy probe actively dials its configured
candidate, including candidates in traffic-failure cooldown. Dialing alone does
not clear cooldown; a successful policy probe response restores the actual
connected leaf's eligibility. Failed probes update policy measurements, not the
generic carrier-failure counter. The policy path applies selection tolerance,
member snapshots, and `policy.probe.completed`.

A single-outbound diagnostic is read-only with respect to URLTest selection,
member measurements and carrier cooldown. It neither clears nor extends those
observations. Native completion logs distinguish the paths as
`operation_kind=policy_urltest` and `operation_kind=diagnostic_outbound`.

## Local failures and traffic health

Carrier observations and passive URLTest observations use the same
establishment-failure classification. DNS resolution failures, missing
local addresses, unavailable local networks, and socket/interface setup errors
must not count against a proxy node. Actual carrier connection failures still
participate in URLTest candidate eligibility. These observations never impose a
global dial prohibition.

TCP tunnel relays retain the failing endpoint (client or upstream) across read,
write, flush, and shutdown. Client cancellation, including an ordinary TCP reset
after HTTP CONNECT, is recorded as `client_transport` / `client_error`; local
address or network loss is `local_network` / `network_error`. These outcomes are
neutral for node health even if the outbound protocol already sent its address
header. Failure to send the client's acceptance response also completes the
session and releases passive recovery state as a neutral client failure.

Local failures therefore do not penalize a recovered carrier. Interrupted TCP
streams are not replayed. Fixed leaf routes, selectors, fallback, load-balance
and relay execution preserve their configured choices and actually dial even
when a carrier is in URLTest cooldown. A successful business connection clears
that carrier's observations immediately, without waiting for a speed test.

## Immediate selection after carrier failure

Five real carrier failures within 30 seconds make the candidate unavailable to
URLTest selection for 60 seconds, or until a real business connection or
successful policy probe restores it. Engine immediately checks affected URLTest
groups, choosing an available member by existing healthy-probe latency, then by
configured order for unmeasured candidates. It updates the selected-member
snapshot and emits `policy.selected`; it does not wait for another flow or the
scheduled probe interval. Actual probe timestamps, member measurements and
`policy.probe.completed` history are preserved.

The availability check follows selected nested selector leaves, considers
fallback/load-balance candidates without advancing rotation, and observes the
first carrier hop of relay groups. It neither switches a pinned selector nor
adds an unconfigured Direct path. If all configured members are unavailable,
URLTest retains its current selection and still attempts the actual connection.
A successful connection restores eligibility without forcing a switch back;
the next policy cycle uses the normal latency/tolerance decision.

Destination-specific passive relay failures remain scoped to that destination
and affect its new flows rather than globally replacing the group's selection.
They are checked before reserving passive half-open recovery. New URLTest flows
also check carrier eligibility to handle concurrent state changes. Probe
application filters carriers that failed later in the same probe cycle, and
retired configuration results cannot change the current generation's selection
or carrier observations.
