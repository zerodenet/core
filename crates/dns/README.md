# DNS resolution scopes

`DnsSystem::resolve_direct_with_family(domain, zero_traits::AddressFamily)` resolves
real business targets selected for a Direct outbound. `Auto` preserves the global
`dns.policy.address_family` behavior. `OnlyIpv4` and `OnlyIpv6` override that default
for one lookup, sending only A or AAAA queries through the existing Direct DNS
server selection and fallback chain. The override never allocates Fake-IP.

An OS/System resolver still uses `lookup_host`; the family setting filters its
returned candidates and does not claim to control the OS resolver's wire queries.
Strict-policy IPv4-mapped IPv6 candidates are normalized to IPv4 before family
filtering. Automatic DNS results retain their original RR representation; the
socket dialing boundary canonicalizes addresses independently.

Success caches, negative results, and in-flight coalescing isolate the per-call
family constraint as well as role, committed DNS configuration generation, and
physical-egress generation. Reloaded lookups cannot join an old configuration's
flight or reuse its failure. Existing callers retain their old snapshot until
completion, and obsolete snapshots cannot write current cache/reverse-map state.

Business family constraints do not alter DNS upstream sockets, node/bootstrap
resolution, intercepted client DNS answers, or control-plane traffic. An explicit
DNS `detour` continues to enter the runtime's `DnsOutboundConnector` bridge. If the
selected detour is itself a constrained Direct tag, that tag's dial policy applies
to its own DNS transport socket in the proxy/platform layer.
