# DNS and Fake-IP end-to-end verification

Zero DNS uses named backends and ordered `dispatch` rules. DNS dispatch reuses
the kernel rule-condition model; the first matching rule selects exactly one
backend and `default_server` handles unmatched names. There is no implicit
racing. An ordered fallback chain is used only when it is declared in
`policy.fallback_servers`.

All network backends use the same endpoint shape:

```json
{
  "runtime": {
    "dns": {
      "servers": {
        "plain": {
          "type": "udp",
          "host": "223.5.5.5",
          "port": 53
        },
        "https": {
          "type": "doh",
          "host": "cloudflare-dns.com",
          "port": 443,
          "path": "/dns-query",
          "bootstrap": ["1.1.1.1", "1.0.0.1"],
          "detour": "dns-proxy"
        },
        "tls": {
          "type": "dot",
          "host": "dns.google",
          "port": 853,
          "bootstrap": ["8.8.8.8", "8.8.4.4"]
        },
        "quic": {
          "type": "doq",
          "host": "dns.adguard-dns.com",
          "port": 853,
          "bootstrap": ["94.140.14.14", "94.140.15.15"]
        }
      },
      "default_server": "https",
      "dispatch": [],
      "policy": {
        "timeout_ms": 5000,
        "server_timeout_ms": { "https": 3000 },
        "fallback_servers": ["tls", "plain"],
        "node_server": "plain",
        "node_fallback_servers": ["tls"],
        "direct_server": "plain",
        "direct_fallback_servers": ["tls"],
        "reject_address_cidrs": ["198.18.0.0/15", "fd00::/96"],
        "address_family": "prefer_ipv6"
      },
      "cache": {
        "max_entries": 1024,
        "max_ttl_seconds": 3600
      },
      "reverse_mapping": {
        "max_entries": 4096,
        "max_domains_per_address": 8,
        "max_ttl_seconds": 300
      },
      "answer": {
        "type": "fake_ip",
        "cidr": "198.18.0.0/15",
        "ipv6_cidr": "fd00::/96",
        "ttl_seconds": 86400,
        "max_entries": 65536,
        "exclude_domains": []
      }
    }
  },
  "outbounds": [
    {
      "tag": "dns-proxy",
      "protocol": {
        "type": "socks5",
        "server": "192.0.2.10",
        "port": 1080
      }
    }
  ]
}
```

`bootstrap` is required whenever a network DNS backend `host` is a domain. DNS
backends never resolve their own transport endpoint through the system resolver;
the explicit addresses also let TUN install deterministic endpoint exclusions.
UDP, TCP fallback, DoH, DoT, and DoQ sockets use the current underlay selection.
Configuration fails before startup when an endpoint cannot be materialized
without recursive resolution.

Network DNS servers may set `detour` to an existing outbound or outbound-group
tag. Plain UDP DNS uses framed DNS-over-TCP when detoured, while DoH and DoT
carry their native TCP/TLS sessions through the selected target. Detoured DNS
endpoints are not installed as physical TUN route exclusions because the host
never opens a socket to those addresses. A configuration containing any
detoured DNS server must declare `policy.node_server`; that server and every
`node_fallback_servers` entry must be non-detoured, so resolving the proxy node
cannot recurse through the outbound it is trying to create. DoQ detours are
rejected until the runtime provides a proxy-aware UDP/QUIC carrier; Zero never
silently sends such traffic through the system route.

Each backend attempt has the `policy.timeout_ms` deadline;
`server_timeout_ms` overrides it by server tag. Transport errors, timeouts,
malformed responses, configured `reject_address_cidrs`, and retryable response
codes advance to the next explicit fallback; NOERROR and NXDOMAIN are terminal.
`node_server` isolates proxy-node and QUIC carrier lookup from client/default
DNS, while `direct_server` isolates direct targets. Each role has its own ordered
fallback list and cache namespace; omitting a role retains the historical
dispatch/default behavior. `address_family` accepts `ipv4_only`, `ipv6_only`,
`prefer_ipv4`, or `prefer_ipv6`. The two `prefer_*` modes query A and AAAA
concurrently and only change Zero's own candidate ordering. The same field is
also authoritative for intercepted DNS answers: `ipv4_only` returns
NOERROR/NODATA for AAAA and removes IPv6 HTTPS/SVCB hints and additional AAAA;
`ipv6_only` applies the symmetric rule to A and IPv4 hints. The `prefer_*`
modes continue to advertise both families rather than silently acting as an
`only` mode.

The DNS interceptor supports UDP and TCP port 53, A and AAAA independently,
EDNS client payload sizes, upstream truncation with TCP fallback, and raw
forwarding of CNAME, HTTPS/SVCB, SRV, TXT, PTR, RCODE, authority records, and
unknown record types. Fake-IP synthesizes A records and, when `ipv6_cidr` is
configured, AAAA records. Without an IPv6 pool, AAAA keeps the backward-
compatible NOERROR/NODATA behavior. Excluded domains use the selected real
backend and its declared fallback chain. For non-excluded Fake-IP names,
forwarded HTTPS/SVCB, SRV, and other non-address responses cannot advertise
real `ipv4hint`, `ipv6hint`, or additional A/AAAA glue; clients must resolve an
address record through the synthetic allocator. Excluded names retain real
service-binding data after the configured address-family filter is applied.
If a removed service-binding hint is declared mandatory, the record is made
unusable instead of forwarding a malformed or policy-violating record. Raw
cache entries contain the already-filtered response, so a cache hit cannot
restore a suppressed family.

Real address resolution starts A and AAAA lookups concurrently. TCP direct and
upstream dialing preserves the answer order within each family, interleaves the
two families, and starts later candidates after a bounded delay. Each candidate
performs its own TUN egress selection and interface binding; a failed first DNS
answer therefore does not prevent a reachable answer from being used.

Address candidates are accepted only when the A/AAAA owner is the queried name
or the terminal canonical name of a valid query-rooted CNAME chain. Compressed
and unordered CNAME answers are supported. Unrelated answer owners are retained
only in the raw forwarded DNS message; they never enter the address cache,
reverse mapping, or outbound candidate set. A malformed, conflicting, or cyclic
trusted CNAME chain is rejected as a malformed upstream response. Additional-
section glue is not promoted to a trusted address candidate.

When `reverse_mapping` is present, successful real A/AAAA answers also populate
a bounded IP-to-domain index owned by `zero-dns`. Transparent TUN sessions may
recover an unambiguous logical domain from that index while retaining the
client-selected IP as their authoritative direct socket target. For TCP, an
explicitly recovered domain also contributes current real-DNS candidates after
that original endpoint; the bounded candidate dialer can therefore recover from
a stale captured IPv4 address without guessing a name for IP-only traffic. DNS
refresh failure retains the original literal candidate. Connectionless direct
UDP remains pinned to the usable client-selected endpoint because it has no TCP-
style connect failure that safely authorizes retargeting. Explicit SOCKS/HTTP IP
targets are never rewritten. Shared CDN addresses with multiple live domain
candidates remain IP targets instead of guessing; TLS/HTTP/QUIC sniffing may
still recover a stronger application-layer name. The index is TTL-capped,
address-LRU bounded, preserved across compatible hot reloads, and intentionally
not persisted across process restarts.

Fake-IP names are IDNA-normalized, lower-cased, and trailing-dot insensitive.
Mappings expire in both directions, are bounded by `max_entries`, and use
deterministic LRU eviction. IPv4 and IPv6 addresses for one normalized domain
share one TTL, LRU identity, and capacity slot. A compatible hot reload
preserves live mappings; changing either pool, TTL, capacity, or exclusions
creates a new allocator. Expiry, LRU eviction, and administrative clearing move
each released address into a `RETIRED` quarantine for one complete configured
Fake-IP TTL. Retired addresses have no reverse mapping and cannot be assigned to
another domain; if every pool candidate is live or retired, allocation fails
closed and an intercepted DNS query receives SERVFAIL. This intentionally
prefers a visible lookup failure over cross-domain delivery through a stale
client DNS cache.
`diagnostics.fakeip_lookup` reports mapping counters, live capacity, and the
current `retired_addresses` count.
The admin command `fakeip.clear` manages the same allocator and persistent
journal. Empty params clear every mapping; `domain` or `ip` selects one mapping
in both directions:

```json
{ "method": "fakeip.clear", "params": {} }
{ "method": "fakeip.clear", "params": { "domain": "example.com" } }
{ "method": "fakeip.clear", "params": { "ip": "198.18.0.1" } }
```

At most one selector is accepted. Success reports `removed_mappings`,
`removed_addresses`, the remaining `live_mappings`, and `retired_addresses`.
Clearing mappings is a disruptive diagnostic action: applications may still
hold synthetic DNS answers whose reverse mappings no longer exist, so clients
should warn before a full clear and expect affected connections to resolve
again; new answers use a different non-retired address while capacity remains.
`diagnostics.dns_lookup` returns the query role plus the newest backend attempts,
including server tag, transport, concrete endpoint candidates, selected outbound,
success, and the failure reason that caused an ordered fallback.

When Zero is started from a configuration file, live Fake-IP mappings are also
written to a versioned journal and restored after a process restart. No JSON
field is required. The default state directory is `%LOCALAPPDATA%\Zero\state`
on Windows, `$XDG_STATE_HOME/zero` on Unix when set, or
`$HOME/.local/state/zero`; `ZERO_DNS_STATE_DIR` overrides the directory. The
journal filename contains a stable hash of the configuration directory so
independent client installations do not normally share state.

The journal is single-owner and atomically compacted. A second Zero process
using the same state fails before it can return Fake-IP answers. Expired
mappings are omitted on restore; a pool, TTL, capacity, or exclusion change
starts a clean journal. Invalid state is quarantined beside the active file
with a `.corrupt-<timestamp>` suffix and DNS starts with an empty allocator.
If a mapping cannot be appended, Zero returns SERVFAIL instead of exposing a
synthetic address that cannot be recovered after restart.
The current journal schema is `zero.dns.fake-ip.v3`; compatible IPv4-only v1 and
dual-stack v2 journals are migrated in place. Live mappings retain their
addresses, while v3 additionally persists unexpired retired-address deadlines so
a restart cannot bypass the reuse quarantine.

TUN without Fake-IP remains supported. For TLS on ports 443 and 8443 Zero can
recover a plaintext SNI; for HTTP/1.x on ports 80, 8000, 8080, and 8888 it can
recover a Host header or absolute-form authority. The consumed prefix is always
replayed byte-for-byte. When a TLS ClientHello carries ECH, Zero does not treat
the outer public name as the hidden application name: it falls back to an
unambiguous DNS reverse mapping and then to the original IP. ECH and
application-owned encrypted DNS are intentionally not decrypted. Port-53
hijacking therefore does not intercept an application's own DoH, DoT, or DoQ
connection. Use existing traffic route rules to route or block known
encrypted-DNS endpoints when deployment policy requires it.

Flow records expose `target.original_ip`, `target.host_source`, and
`target.fake_ip_reverse_status`. Together with `target.host`, `resolved_ip`, and
`sniffed_host`, these distinguish Fake-IP restoration, DNS reverse recovery
(`host_source=dns_reverse`), TLS/HTTP/QUIC sniffing (`tls_sni`, `http_host`,
`quic_sni`), a missing
reverse mapping, and the final direct endpoint without changing existing API
fields.

Run the unprivileged coverage with:

```bash
cargo test -p zero-dns --features udp,doh,dot,doq
cargo test -p zero-proxy --features dns
```

For restart recovery, resolve a new A record through the intercepted DNS,
record the returned Fake-IP, stop Zero, start it again with the same config,
and connect to the recorded address before issuing another DNS query. The flow
must restore the original domain and report `fake_ip_reverse_status=resolved`.

Then follow `tun-e2e.md` for privileged route/device tests on each platform.
