# DNS and Fake-IP end-to-end verification

Zero DNS uses named backends and ordered `dispatch` rules. DNS dispatch reuses
the kernel rule-condition model; the first matching rule selects exactly one
backend and `default_server` handles unmatched names. There is no implicit
racing or fallback to another backend.

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
          "bootstrap": ["1.1.1.1", "1.0.0.1"]
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
      "cache": {
        "max_entries": 1024,
        "max_ttl_seconds": 3600
      },
      "answer": {
        "type": "fake_ip",
        "cidr": "198.18.0.0/15",
        "ttl_seconds": 86400,
        "max_entries": 65536,
        "exclude_domains": []
      }
    }
  }
}
```

`bootstrap` is required when `host` is a domain and TUN strict routing is in
use. It prevents resolving the DNS server through itself and lets TUN install
deterministic endpoint exclusions. UDP, TCP fallback, DoT, and DoQ sockets use
the current underlay selection. DoH uses the bootstrap endpoint exclusions and
interface binding where the HTTP client supports it. Configuration fails before
TUN startup when a strict endpoint cannot be excluded safely.

The DNS interceptor supports UDP and TCP port 53, A and AAAA independently,
EDNS client payload sizes, upstream truncation with TCP fallback, and raw
forwarding of CNAME, HTTPS/SVCB, SRV, TXT, PTR, RCODE, authority records, and
unknown record types. Fake-IP synthesizes A records and returns NOERROR/NODATA
for AAAA; excluded domains use the selected real backend.

Fake-IP names are IDNA-normalized, lower-cased, and trailing-dot insensitive.
Mappings expire in both directions, are bounded by `max_entries`, and use
deterministic LRU eviction. A compatible hot reload preserves live mappings;
changing the pool, TTL, capacity, or exclusions creates a new allocator.
`diagnostics.fakeip_lookup` reports mapping counters and capacity.

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

TUN without Fake-IP remains supported. For TLS on ports 443 and 8443 Zero can
recover a plaintext SNI and route by domain; otherwise routing and dialing stay
on the original IP. ECH and application-owned encrypted DNS are intentionally
not decrypted. Port-53 hijacking therefore does not intercept an application's
own DoH, DoT, or DoQ connection. Use existing traffic route rules to route or
block known encrypted-DNS endpoints when deployment policy requires it.

Flow records expose `target.original_ip`, `target.host_source`, and
`target.fake_ip_reverse_status`. Together with `target.host`, `resolved_ip`, and
`sniffed_host`, these distinguish Fake-IP restoration, TLS sniffing, a missing
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
