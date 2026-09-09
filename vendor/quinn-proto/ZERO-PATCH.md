# Quinn transport extension

Upstream: quinn-proto 0.11.14 (crates.io), MIT/Apache-2.0 licenses retained.

The patch adds defaulted, protocol-neutral Controller hooks: an explicit
byte/second pacing rate and a count of lost non-PMTU-probe packets. The pacer
uses the supplied rate independently of the congestion window; controllers
without a rate keep upstream pacing. BBR exposes its existing computed rate.
Exact packet feedback additionally identifies number spaces and reports each sent,
acknowledged, lost or discarded packet, followed by a batch-end event with updated
flight and RTT. Sends are reported per QUIC packet, including coalescing/GSO.
Legacy callbacks keep their order; controllers that do not consume the new events
retain upstream behavior. Shared BBR execution, Hysteria2 profiles, negotiation
and Brutal remain outside this dependency.

The local source is excluded from workspace membership. When updating Quinn,
reapply and review these hooks and run the dependency unit tests (including pacing and exact packet feedback) alongside
the workspace and Hysteria2 interoperability suites.

An optional receive-window policy supplies absolute credit updates from validated
receive/consume/close events and measured RTT. StreamsState retains QUIC offset
validation and latest-credit retransmission; a policy is built per connection,
and receive-half closure / rejected 0-RTT clears stream state. No tuning threshold,
growth multiplier or protocol field lives in this carrier. Without a policy,
the original fixed-window path remains active. Explicit connection-window overrides
are synchronized to the policy. Run all dependency unit tests, including
`receive_policy_grants_wire_credit_and_reclaims_closed_stream_state`.
