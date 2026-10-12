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

Direct UDP sockets open only on demand. Distinct peers normally share the same
policy/association mapping; simultaneous logical flows to the same peer need
separate sockets so a reply has exactly one owner. Retiring a peer leaves a
tombstone while its socket still serves other peers. Each socket admits at most
256 distinct peers over its lifetime, including retired peers; reaching this
limit preserves existing mappings and sends new peers through another socket.
A socket closes when its last active reply owner retires.

### Fresh route checks with reusable probe storage

Every Direct UDP send still obtains a fresh OS route/source selection before
matching a reusable business socket. Egress generations fence published network
changes, but are not a complete observation of all OS route changes; there is no
TTL or generation-only cache of a route, source address or interface decision.

`zero-platform-tokio::egress::route_probe` reuses only native probe descriptors
on Linux/macOS, with at most one per family per calling OS thread. No payload is
sent, receive storage is bounded by a small socket buffer request, and there are
no per-destination entries, background tasks, timer wakeups or shared pool lock.
Each successful query clears the chosen source/peer and native route cache
before retaining its descriptor. Linux uses AF_UNSPEC disconnect; macOS uses
checked `disconnectx` followed by interface scope NONE, which releases the route
cache. A failed query drops the descriptor; a failed reset retains the current
query result but drops storage so the next call starts fresh. Mapped IPv4 and
other platforms retain the original one-shot path.

The native reset behavior was checked against the [pinned Darwin UDP implementation](https://github.com/apple-oss-distributions/xnu/blob/xnu-8796.141.3/bsd/netinet/udp_usrreq.c),
its [interface-scope route invalidation](https://github.com/apple-oss-distributions/xnu/blob/xnu-8796.141.3/bsd/netinet/in_pcb.c),
and [Linux UDP disconnect](https://github.com/torvalds/linux/blob/v6.12/net/ipv4/udp.c).
The probe never receives business policy, protocol or configuration ownership.

Explicit source/interface ownership is still enumerated on every selection.
Source capture reuses that same synchronous inventory snapshot instead of
enumerating twice; native socket opening independently revalidates live inventory.
Proxy compares borrowed policy/source/interface identity on a warm send, creating
an owned binding only for a newly opened socket. Family checks, strict-route mark,
TUN-source rejection, config/egress fences, exact reply ownership and post-wait
response revocation remain unchanged. This reduces query/storage work, not the
strength of route or ownership checks; it does not claim end-to-end CPU savings.

The ignored `compare_fresh_and_reused_route_probe_cost` test compares the original
one-shot probe with reused storage on Linux/macOS: five alternating-order rounds
of 4,000 queries per mode, using loopback and documentation-only IPv4 destinations.
It sends no payload and changes no host routes. Run it with the workspace feature
graph (`cargo test --workspace --all-features compare_fresh_and_reused_route_probe_cost
-- --ignored --nocapture`); compare medians within one run and record the build
profile and host load. This is a probe-stage microbenchmark, not a throughput,
whole-process CPU, power or CI pass/fail threshold.

Socket retirement is bounded by live flow ownership, not dispatcher lifetime.
There is no permanent used-port bitmap or retired socket registry. Once the last
owner retires, the socket and its queued receive data are dropped; a subsequent
socket may preserve the preferred source port or use the OS's normal ephemeral
allocation. This applies to default `auto` as well as constrained Direct policies,
so repeated retire/rebind cycles do not accumulate a lifetime port-exhaustion limit.

### Direct UDP receive storage and scheduling

The platform exposes native datagram polling with the same completion-only
`IoObserver` accounting as async receives. Pending readiness is neither a packet
nor an error. Its native one-waker contract requires one serialized receive owner
per socket; the existing async receive API remains available to other callers.

The proxy Direct socket set serializes receive callers, polling sockets in round
robin order without rebuilding a heap-allocated future set. Cancellation releases
the receive guard without reading ahead or spawning a response pump. Each poll
visits at most the current socket count. Untrusted or stale replies advance the
cursor and yield before retrying. The cursor survives cancelled waits and is
normalized when the socket set changes. This is O(sockets) polling, not a claim of
constant-cost readiness dispatch for arbitrarily large socket sets.

Normal runtime callers provide at least 65,535 bytes and receive directly into
their existing buffer. There is no additional payload buffer per native socket.
Short/empty caller buffers use at most one lazy 65,535-byte scratch buffer per
socket set: native I/O still consumes and observes a full datagram before the
existing copy/truncation contract returns it. This also preserves platform error
behavior for short caller buffers. Retiring the last socket releases scratch
storage. Socket receive queue sizes, peer admission limits, exact response owners,
retired-peer tombstones and pre/post-I/O policy/egress checks are unchanged.

Regression tests cover large/empty datagrams, short-buffer draining, scratch
release, cancelled and concurrent readers, unregistered peer pressure and
completion-only observation. A warmed thread-local test allocation probe compares
100 cancelled receive polls against the removed future-set bookkeeping. It uses
the system allocator only in the proxy unit test executable; it does not replace
the production allocator or measure whole-process CPU, RSS, power or throughput.

Hot-path egress generation reads use the live control handle directly, without
constructing a transport factory and node resolver just to read a number. Direct
policy checks compare a borrowed policy and its generation inside the current
Engine snapshot. The owning Engine retains implicit/tagged Direct semantics;
owned policy projection still serves preparation callers. Neither optimization
caches decisions across reloads or weakens a fence.

Each exact response owner also has a revocable token for replies already read by
the process. Removing that owner or its socket revokes the token; policy/egress
generation changes invalidate it even before cache cleanup. Reusing the same
local port cannot revive an old token. Direct response delivery checks validity
before download shaping and again after the wait, immediately before starting the
response writer. Bytes already handed to an in-flight writer cannot be retracted.

These checks isolate known process-owned state: stale asynchronous work, queued
old-socket replies and replies already read under an old owner. Generic UDP carries
no socket ID or configuration epoch on the wire. An arbitrarily delayed network
packet that arrives only after the same local/remote tuple has been reused cannot
be distinguished from a current reply by this layer. Generation checks do not
claim that stronger guarantee. It would require application-level correlation or
an explicit, bounded packet-lifetime/quarantine contract; indefinitely retiring
ports would violate normal long-lived UDP reuse and eventually fail closed.

A completed UDP send remains successful even if its policy becomes stale during
completion. Staleness can suppress reply registration, but must not turn an
already transmitted packet into an error that triggers outbound fallback replay.

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

The TCP/UDP dual-family reload integration cases use the DNS feature and a local
UDP fixture that returns `127.0.0.1` for A and `::1` for AAAA. They must not assume
the host resolves `localhost` to both families: system resolvers and an active
TUN can return only IPv4 or IPv4-mapped candidates. Native literal-address and
binding validation tests remain independent of this fixture. The fixture is
compiled only into `proxy_dispatch`, preserving other test targets' cache inputs.
