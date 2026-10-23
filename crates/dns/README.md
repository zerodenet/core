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

## Explicit host QUIC capability

A `DnsOutboundConnector` can supply a `DnsQuicBidiConnector` for direct DoQ.
The backend passes the actual bootstrap candidate, SNI, `doq` ALPN and original
attempt context, then keeps the existing length prefix, FIN and response checks.
After that capability is selected, its failure never triggers a naked UDP/QUIC
fallback. Query cancellation/retirement releases only that query's users; the
host separately retains and closes its native originals and actual drivers.
The default capability is absent during staged migration. DNS does not import
a transport runtime, raw QUIC handles or another Quinn owner implementation.

## Explicit owned UDP root library path

`DnsSystem::build_with_owned_udp` requires original host-issued
`RootDatagramGrant`s and `OwnedDnsQueryTasks` from the actual root registrar,
with an explicit finite query limit. The root's existing HostResources owns each
original native descriptor before I/O; DNS receives a unique I/O view. Name and
wire query coalescing register their original leader jobs exactly once in that
same Scope through a weak registrar. No task, runtime, native pool or production
default is added. Root grants validate the actual instance, original Scope,
current host lifecycle, exact declared peer and physical-egress generation;
public service-purpose metadata does not authorize a native open.

The UDP wire retry count, DNS transaction/source matching, backend deadlines,
cache keys and configured candidate order remain shared with existing DNS code.
A backend deadline is settled inside the owned socket continuation so its real
close runs before another backend can receive that query. Acquisition fallback
requires the same root/query provider's private settlement receipt. A successful
acquisition requires explicit original native close before fallback; Unknown or
close failure ends the attempt and stays charged. Returned errors retain their
original typed sources. Cancellation drops only the I/O view: the host retains
the original native owner until explicit cleanup. Query body completion, actual
Scope join, view return and native close remain separate facts.

This explicit UDP-only entry rejects other native transports, System OS lookup,
detours, dynamic ECH endpoints and truncated-UDP TCP fallback before those
unaccounted operations. Existing builders preserve those capabilities during
migration. Their owners need separate real grants before this path can replace
a production DNS root; neither query joins nor UDP receipts qualify the whole
resolver, the raw-IP gateway, or M4.

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
