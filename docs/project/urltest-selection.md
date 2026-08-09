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
outbound HTTP probe executor, but not policy state. Only a URLTest cycle applies
selection tolerance, updates member health/selection snapshots, and emits
`policy.probe.completed`. A single-outbound diagnostic is read-only with
respect to every URLTest group. Native completion logs distinguish the paths as
`operation_kind=policy_urltest` and `operation_kind=diagnostic_outbound`.
