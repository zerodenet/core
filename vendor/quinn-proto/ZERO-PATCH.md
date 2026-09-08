# Quinn transport extension

Upstream: quinn-proto 0.11.14 (crates.io), MIT/Apache-2.0 licenses retained.

The patch adds two defaulted, protocol-neutral Controller hooks: an explicit
byte/second pacing rate and a count of lost non-PMTU-probe packets. The pacer
uses the supplied rate independently of the congestion window; controllers
without a rate keep upstream pacing. BBR exposes its existing computed rate.
Hysteria2 negotiation and Brutal remain outside this dependency.

The local source is excluded from workspace membership. When updating Quinn,
reapply and review these hooks and run the dependency pacing tests alongside
the workspace and Hysteria2 interoperability suites.
