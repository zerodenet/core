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
