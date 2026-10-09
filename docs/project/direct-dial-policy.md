# Direct dial constraints

## v0.0.3 scope

`OutboundConfig.dial` is a common optional object, initially executable only on
Direct outbounds. Its default is `address_family: auto`, without `interface` or
`source_ip`. Omission, an empty object, and the explicit default are equivalent;
default objects are omitted on serialization. Non-default dial configuration on
any other outbound protocol fails structural validation rather than being ignored.

The family values are `auto`, `only_ipv4`, and `only_ipv6`. An explicit family is
a hard constraint on destination candidates and sockets, including literal
addresses and every retry/recovery path. `auto` retains the existing DNS-family
behavior unless a source address constrains it. `source_ip` is a concrete IP
address, not a hostname, CIDR, endpoint, or scope-suffixed address. IPv4-mapped
IPv6 is canonical IPv4 at configuration, resolution and socket boundaries.
Wildcard, multicast and IPv4 broadcast sources are rejected. A source address
restricts `auto` to that source's family; conflict with an explicit family fails.

An interface is a name rather than an address or index. Empty names, surrounding
whitespace, NUL and slash are rejected; internal spaces remain valid for Windows
adapter names. Syntax validation deliberately does not inspect host interfaces or
local IP ownership. Runtime platform checks must verify that the source belongs
to the local host and, when an interface is supplied, to that interface. This
preserves the side-effect-free [validation boundary](config-validation-isolation.md).

[`examples/v0.0.3/direct-dial.json`](../../examples/v0.0.3/direct-dial.json)
provides two SOCKS listeners on `127.0.0.1:1080` and `127.0.0.1:1081`, routed by
inbound tag to independent IPv4-only and IPv6-only Direct outbounds. The latter
requires a working IPv6 destination and local IPv6 connectivity. The IPv4 listener
address controls how a client reaches Zero, not the outbound destination family.

To bind a source/interface, add actual local values to a selected Direct `dial`
object, for example `interface: eth0` and `source_ip: 192.0.2.10`. These two values
are illustrative placeholders, not a runnable configuration: replace both with
an existing local interface and an address owned by it. They are deliberately
absent from the runnable example.

## Ownership and immutable identity

- Config owns serde ADTs and structural validation. It projects validated values
  into `zero_traits::DialPolicy`; platform code never imports `zero-config`.
- `zero-traits` owns the runtime-neutral address-family and dial-policy contract,
  including mapped-address canonicalization and pure structural checks.
- Engine plans own each configured leaf's policy. Resolving a selector, fallback,
  URLTest or load-balance branch preserves the selected leaf's policy, rather than
  rebuilding it from the outer group or a later lookup by tag.
- `ResolvedLeafOutbound::Direct` carries its optional tag, owned policy and
  `dial_generation`. The implicit `direct` route action has the default policy and
  generation zero, independently of an explicitly configured outbound named
  `direct`. Proxy adapters capture this immutable execution identity.
- An engine-owned monotonic allocator assigns a fresh generation on policy change
  or tag removal/reintroduction. Unrelated reloads retain unchanged identities.
  A→B→A receives a fresh final identity. Staged rollback restores the old plan but
  never rewinds the allocator. Snapshot lookup exposes exactly that snapshot's
  policy and generation; current-engine lookup is for checking continued validity.

## Socket and resolver invariants

TCP and UDP must use the same neutral policy semantics. Candidate filtering must
run again immediately before socket creation/send. An empty allowed candidate
set must fail explicitly. Wrong-family literals, mapped addresses and fallback
candidates must never gain permission through retry. Socket failures must retain
explicit source/interface constraints instead of silently trying an unconstrained
socket. UDP source-port fallback may change only the port.

Explicit Direct family constraints override the global DNS-family default for
that business-target resolution only. Resolver role, server selection, fallback,
dispatch, timeout and rejection policies still apply. This must not change client
DNS responses, Fake-IP assignment, node resolution or DNS-upstream endpoint
families. A DNS detour selected through a Direct tag uses that tag's policy. The
System resolver can guarantee returned/dialed families, not which wire query
types the operating system sends. Success caches, failure caches and in-flight
coalescing must preserve request-policy and resolver-generation isolation.

TUN loop prevention and explicit user constraints must be intersected. A required
underlay, mark, local source or interface that cannot satisfy both must fail closed;
the explicit interface must not disappear on a system-route or loopback branch.
Platform validation and binding own interface lookup, ownership checks and OS
errors. No cross-layer config dependency or shell-based interface binding is needed.

UDP reuse must include immutable policy identity and effective socket facts, in
addition to association scope, family and egress generation. Async sends and
responses from stale identities must not reattach to a later identical-looking
configuration. Same-policy endpoint-independent mapping and exact-remote response
filtering must remain intact. Observability must report actual local/interface
facts rather than rerunning route selection after socket creation.

Non-default Direct dial policy does not imply support in native raw-packet or
ICMP forwarding. Those paths must reject a policy they cannot enforce. Auto
TCP/UDP selection may use the policy-capable Flow path; local ingress echo replies
are local endpoint responses, not outbound policy execution.

## Rust integration migration and verification

Rust `OutboundConfig` literals add `dial: Default::default()` for legacy behavior.
Exact Direct leaf patterns/constructions add `dial_policy` and `dial_generation`,
or `..` when only inspecting the tag. This adds no API/event schema generation.

Config regressions live in `crates/config/tests/outbound_dial.rs`; engine leaf,
group, reload, A→B→A and staged-rollback regressions live in
`crates/engine/tests/direct_dial.rs`. Runtime socket, DNS and platform tests must
add execution evidence beyond these structural tests. Cross-platform acceptance
requires the workspace checks and full test suite documented in `AGENTS.md`.
