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
outbound HTTP probe executor, but not policy or traffic-health state. A URLTest
policy probe bypasses an existing traffic quarantine and never records generic
outbound success or failure; only its explicit policy path applies the result,
selection tolerance, member health/selection snapshots, and
`policy.probe.completed`. A single-outbound diagnostic also bypasses the
shared quarantine, but does not change URLTest or traffic-health state. Native
completion logs distinguish the paths as `operation_kind=policy_urltest` and
`operation_kind=diagnostic_outbound`.

## Traffic selection and shared quarantine

An explicit selector remains pinned to its chosen target. Within a URLTest
group, ordinary traffic first checks whether the current member's resolved
proxy leaf is globally available, then checks passive member health for the
destination. If either check rejects it, the resolver considers members with
healthy URLTest measurements by latency and then the remaining configured
members. A member is usable only when both the global leaf gate and passive
member gate allow it. If none is usable, resolution returns
`no_usable_urltest_member` with the group tag. A surrounding fallback group may
still choose another configured candidate.

The global gate is read-only during group selection. TCP candidate admission
reserves the actual traffic attempt. After the 60-second cooldown, only one
half-open attempt may run for a leaf; a failed or abandoned attempt restarts
the cooldown. If the leaf becomes quarantined between URLTest selection and
admission, traffic releases its passive reservation and resolves the group
once more. Policy probes may update the URLTest winner while a leaf remains
globally quarantined; ordinary traffic continues to honor the global gate.

## Local failures and traffic health

The shared traffic circuit breaker and passive URLTest observations use the
same establishment-failure classification. DNS resolution failures, missing
local addresses, unavailable local networks, and socket/interface setup errors
must not count against a proxy node. A rejection by the shared circuit breaker
is not a new failed connection and must not extend passive member quarantine.
Actual node connection failures still participate in traffic health.

TCP tunnel relays retain the failing endpoint (client or upstream) across read,
write, flush, and shutdown. Client cancellation, including an ordinary TCP reset
after HTTP CONNECT, is recorded as `client_transport` / `client_error`; local
address or network loss is `local_network` / `network_error`. These outcomes are
neutral for node health even if the outbound protocol already sent its address
header. Failure to send the client's acceptance response also completes the
session and releases passive recovery state as a neutral client failure.

This prevents local failures from creating a node quarantine that would reject
the first new connection after recovery. It does not replay an interrupted TCP
application stream or switch a manually selected node. An existing quarantine
caused by actual node failures retains the normal cooldown policy.
