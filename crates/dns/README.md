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

## DoH connection recovery

HTTP/2 sessions are reused only within the same physical-egress generation and
configured detour. A network change, including TUN activation or withdrawal,
retires the previous pool identity even when the interface name stays the same.
Connections prepared across a generation change cannot enter the current pool
or carry a new DNS query. Native binding and route selection remain owned by
`zero-platform-tokio`; explicit detours still enter the existing runtime bridge.

A failed or cancelled exchange retires its own session. This includes cancellation
by the enclosing configured backend deadline, so a blackholed HTTP/2 session is
not reused indefinitely. Cancellation of an older session cannot invalidate its
replacement. Healthy sessions retain multiplexing and reuse; this does not add
automatic query replay, change backend deadlines or alter the configured DNS
dispatch and fallback policy.

## Localhost names

`localhost` and names below `.localhost` resolve locally to `127.0.0.1` and
`::1` as specified by RFC 6761. Matching is case-insensitive and accepts the
trailing root dot. These names never allocate Fake-IP, consult response caches,
or query configured/upstream or OS DNS. This applies to Default, Direct and Node
resolution, the System fallback, intercepted UDP/TCP queries and ECH lookups.
Other DNS record types return a local NOERROR/NODATA response.

Explicit IPv4-only/IPv6-only policies still filter local answers; preference
policies preserve their ordering and per-Direct strict family overrides remain
isolated. IPv6 loopback does not require an IPv6 Fake-IP pool. Lookalike names
such as `localhost.example` retain ordinary DNS behavior.

Legacy persistent mappings remain available for reverse lookup until expiry so
cached clients are not redirected to another domain. New localhost lookups use
loopback immediately and do not refresh those mappings. System applications
outside Zero also need the platform's normal localhost/hosts configuration.

## Local discovery names

Names ending in `.local` use link-local discovery (RFC 6762). A public DNS
backend cannot discover a device advertised on the client's local network.
Use the existing System backend to delegate these lookups to the OS resolver
and its platform discovery support. Merge a server entry and a first-match
dispatch rule into the existing DNS configuration:

```json
{
  "servers": { "local-system": { "type": "system" } },
  "dispatch": [
    { "condition": { "type": "domain", "values": ["local"] }, "server": "local-system" }
  ]
}
```

This is a configuration fragment; retain the other servers, default backend,
and rules. An explicit `dns.policy.direct_server` overrides dispatch for Direct
targets, so that setting must also permit the chosen local resolver. With
Fake-IP answers, add `*.local` to `dns.answer.exclude_domains` when local
clients need the device's real address. Keep local device traffic on a Direct
route, and use the client's system-proxy bypass settings for local discovery
when those applications should connect through the OS network stack.

System resolution requires OS discovery support and a reachable advertising
device. Repeated failures remain visible in flow results; the proxy aggregates
environmental warning logs over a 30-second window, including failures reported
again by the inbound listener, and reports the suppressed count on the next
warning.
