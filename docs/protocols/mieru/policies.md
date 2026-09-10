# Mieru transport options and policy choices

The implementation target remains official **v3.33.0**. Wire compatibility and
missing configurable capabilities are separate from choices about resource use.
The current source changes are pending the validation recorded in [parity.md](parity.md).

Both inbound and outbound `protocol` objects accept these fields:

```json
{
  "mtu": 1400,
  "traffic_pattern": {
    "seed": 3330,
    "tcp_fragment": { "enable": true, "max_sleep_ms": 0 },
    "nonce": {
      "type": "printable_subset",
      "apply_to_all_udp_packet": false,
      "min_len": 6,
      "max_len": 12
    }
  },
  "receive": {
    "stall_timeout_ms": 30000,
    "max_pending_bytes": 524288,
    "max_pending_frames": 64,
    "max_connection_pending_bytes": 4194304
  }
}
```

These fields supplement credentials, endpoints and `transport`; the example is
not a complete protocol object. Omitting them preserves configuration validity.

## MTU and traffic patterns

`mtu` defaults to 1400 and accepts 1280–1500. It denotes the outer IP packet size.
The UDP payload budget subtracts the actual peer's IPv4/IPv6 and UDP headers;
reliable data segmentation also reserves nonce, metadata and authentication tags.
Padding consumes only the remaining budget. A smaller local send MTU does not
reject an otherwise valid packet sent by a peer with a larger MTU.

`traffic_pattern` materializes omitted fields from a seed using the v3.33.0
algorithm. Optional `unlock_all` permits the broader generated profile. Explicit
fields override generated fields individually. `nonce.type` is `random`,
`printable`, `printable_subset` or `fixed`; fixed prefixes use
`custom_hex_strings`, each at most 12 bytes. An empty list means random. Nonce
lengths are at most 12; TCP fragment sleeps are at most 100 ms. TCP fragmentation
splits encrypted writes, while MTU segmentation splits native UDP payloads.
The UDP first-packet setting belongs to the carrier and is shared by codec clones.

Profiles apply to native TCP/UDP, inbound, pooled outbound and public stream APIs. A Mieru
TCP relay final hop pools its carrier by the complete relay-prefix identity and uses that pool
for both TCP CONNECT and UDP ASSOCIATE logical sessions. Native UDP relay uses the same protocol
pool when the preceding hop supplies a datagram carrier.

## Resource and recovery choices

| Area | Current decision | Benefit and cost |
| --- | --- | --- |
| UDP congestion | v3.33.0 CUBIC curve, slow start, loss reduction and timeout restart | Replaces simplified AIMD; comparative throughput still requires measurement. |
| Retransmission | RTT-based RTO, fast retransmission, doubling backoff, 3s backoff ceiling, ten transmission attempts | Bounds retained work and failure latency. Long blackouts can outlast this recovery budget; these constants are not claimed equivalent to the official policy. |
| Pool | Prefer the least-loaded carrier; grow after every live carrier reaches 75% of its own session capacity; at most four per identity and 512 identities; retire carriers from new work after 30 minutes and clear the pool on reload | Bounded, load-driven Zero policy for direct and relay carriers. Existing streams drain after age/reload retirement. It deliberately does not copy the official preset factors or traffic-volume retirement one-to-one. |
| TCP slow consumers | Per-session pending buffer and a progress deadline; no waiting on that consumer in the shared reader | Other sessions continue receiving while the stalled session has bounded memory and time. A burst exceeding the buffer cap can close that session before the deadline. |

The TCP receive policy counts both the eight-frame delivery queue and its pending
backlog. Defaults are 512 KiB / 64 queued frames per session and 4 MiB across the
carrier. The stream may additionally hold one frame already removed from the
delivery queue but only partly read by the application; cipher/read buffers and
outbound queues are separate. These limits do not claim total carrier memory is
4 MiB. A backlog with no observed consumption or successful forwarding
into the delivery queue for 30 seconds closes that session. Progress resets the
deadline. Buffer overflow closes the offending session immediately; no unbounded
queue or infinite consumer wait is introduced. Native UDP continues to advertise
its bounded receive window and apply reliable backpressure.

Incoming TCP data also drains that session's existing backlog into available
delivery slots. A backlogged session yields once before admission so a healthy
consumer can run under a continuously ready carrier. This avoids mistaking a
delay until the periodic drain for an application stall.

The timeout may be set to 1000–300000 ms; per-session queued bytes are 1–16 MiB,
frames 1–4096, and per-carrier pending bytes must cover the per-session limit and
not exceed 64 MiB. Increasing limits tolerates longer stalls at a memory cost.

MTU, traffic and receive options participate in outbound cache identity. A
request cannot reuse a carrier prepared under another profile.
