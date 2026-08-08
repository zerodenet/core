# Traffic accounting contract

`StatsSnapshot.bytes_up` and `bytes_down` are monotonic cumulative counters for
one engine instance. They include bytes already observed for active TCP and UDP
flows as well as completed flows, so consumers may derive a rate from the
difference between consecutive snapshots.

User-direction totals count a relayed byte once even though transport
instrumentation observes both sides of the relay:

- upload is `max(inbound_rx_bytes, outbound_tx_bytes)`;
- download is `max(outbound_rx_bytes, inbound_tx_bytes)`.

Each active session claims only the increase of those maxima. Mirrored updates
at the second transport boundary therefore do not increment the global total a
second time. Session completion claims only any final unobserved difference and
does not re-add the completed record's full traffic.

`StatsSnapshot.per_outbound` preserves its completed-flow attribution contract.
Its `flows` and byte totals advance after the final outbound and outcome are
known. While a flow is active, its traffic is visible through the global
counters and active flow snapshots; it enters the per-outbound aggregate when
the flow completes.

Counters reset when a new engine instance starts. Until an explicit instance
identifier is negotiated, control-plane consumers must discard the previous
rate baseline when the health snapshot's start time changes rather than
subtracting samples across a restart.
