# Listen address normalization

`listen.address` accepts IPv4 literals, bare IPv6 literals (`::1`, `::`) and
bracketed IPv6 literals (`[::1]`, `[::]`). Existing hostname support remains
available for TCP and datagram listeners. QUIC and the gRPC control companion
continue to require IP literals.

`zero-core::address` owns runtime-neutral literal parsing, socket endpoint
formatting, and listen-host overlap checks. IP listeners construct `SocketAddr`
values; text-only carrier APIs receive canonical bracketed IPv6 socket endpoints.
The protocol registry's default bind helpers retain hostname resolution without
reinterpreting valid IPv6 literals as hostnames. This also applies to Direct's
paired TCP/UDP listener, Shadowsocks datagrams, HY2 website listeners, VLESS
carriers, Mieru, WireGuard and the control API.

## Wildcards and conflicts

The implementation does not set `IPV6_V6ONLY` and has no `v6_only` config flag.
An IPv6 wildcard listener on `::` always targets the IPv6 wildcard; whether it
also receives IPv4 traffic follows the operating system's socket default.
`::` must not be treated as a portable promise of dual-stack acceptance.

For the same reason, a pair of `0.0.0.0` and `::` listeners on the same port is
not guaranteed to coexist. Config validation rejects potentially overlapping
wildcard claims conservatively, including IPv6 wildcard versus IPv4 literals.
Use separate ports when deployment behavior must be portable.

Equivalent compressed/expanded or bracketed IPv6 literals have the same endpoint
identity for conflict checks. IPv4-mapped IPv6 addresses are normalized to IPv4
for equality and wildcard overlap. Hostnames compare case-insensitively; config
validation does not resolve DNS aliases or hostname-to-literal equivalence.
Different concrete IPv4 and IPv6 addresses remain distinct when neither is an
IPv6 wildcard or IPv4-mapped equivalent.

## Regression coverage

- `zero-core/tests/listen_address.rs`: literal parsing, canonical socket strings,
  malformed bracket rejection, mapped equivalence and symmetric overlap policy.
- Config listener tests: normalized duplicate claims, wildcard conflicts,
  unaffected distinct listeners and control gRPC loopback classification.
- Proxy registry listener tests: real IPv6 TCP/UDP traffic through bare and
  bracketed loopback/wildcard listeners, Direct paired sockets, real HY2/VLESS QUIC handshakes, and IPv4/hostname
  regression cases. Wildcard tests print observed IPv4 acceptance without
  assuming a cross-platform default. IPv6-unavailable hosts print an explicit
  skip reason.

### Host socket probe (2026-10-09)

A standard-library Python probe on Linux 6.18.44 x86_64 observed default
`IPV6_V6ONLY=0` for both TCP and UDP sockets bound to `::`. Both received traffic
sent to `127.0.0.1`; a second `0.0.0.0` bind to the same port failed with
`EADDRINUSE` for both transports. Separate TCP loopback connections to `::1` and
`127.0.0.1` succeeded. This records the host's default behavior, not a portable
promise and not a substitute for running the Rust application regression tests.
