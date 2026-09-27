# Protocol audit scope clarification

Protocol alignment means the selected capabilities, observable semantics and
claimed wire compatibility. It does not require copying upstream internals,
configuration trees, scheduling algorithms or control planes.

- SOCKS5 BIND and UDP fragmentation are explicitly outside the selected scope.
  Keep rejecting them; do not count them as implementation debt.
- HTTP outbound and HTTP user authentication are absent from the current public
  configuration. Classify these as scope decisions until required, rather than
  automatically deriving requirements from RFC completeness. Mixed users apply
  to SOCKS5 only; the HTTP branch is unauthenticated and must be described so.
- Trojan uses Mux.Cool. This is an implemented multiplexing capability; a
  difference from Trojan-Go smux is not an instruction to replace it. Do not
  promise smux wire interoperability without implementing and verifying it.
- Mieru pool, heartbeat and recovery policy differences are not missing
  capabilities by themselves. Assess bounded state, isolation, recovery and
  throughput against the intended behavior. Apply the same rule to VLESS.
- Hysteria2 capabilities are assessed along its own config/profile/connection
  path. Shared transport or VLESS Hysteria-carrier support alone does not prove
  the independent Hysteria2 protocol exposes that support.

## VMess cipher naming

The existing private wire-security 0x06 mode is named `zero-plus`. Its wire
format is unchanged. Previous private `cipher: zero` configurations must be
explicitly migrated; no ambiguous alias is retained.

The name `zero` is reserved for Xray's NONE (0x05) mode with ChunkStream and
ChunkMasking disabled. That execution path is not implemented here yet, and
configuration validation rejects it with a migration explanation. Renaming the
private extension is not evidence of implementing Xray zero.

Reference: Xray-core v26.3.27, commit
`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`,
`proxy/vmess/outbound/outbound.go`, security normalization before header encoding.

## Hysteria2 source check

The public `Hysteria2TransportConfig` contains congestion, QUIC settings and
`ignore_client_bandwidth`. `Hysteria2OutboundOptionsRef` carries settings,
server name, password, insecure and fingerprint. `transport/pool.rs` builds
`Hysteria2QuicProfile` from these values; `transport/connection.rs` invokes the
shared `quic::client_config` and fixed-server/port endpoint connector. There is
no CA/pin, obfuscation or port-hopping projection through this path.

By contrast, `crates/transport/src/hysteria/settings.rs` assigns hopping to a
shared datagram profile for the VLESS Hysteria carrier. This is existing shared
implementation that could be reused, not evidence that HY2 has exposed it.
Package `2.6.1` and interop workflow `2.12.2` remain a separate baseline issue.

HTTP evidence: `crates/config/src/model/inbound.rs` defines fieldless
`HttpConnect`, while Mixed has only `socks5_users`.
`crates/proxy/src/adapters/mixed/inbound.rs` creates the HTTP handler with its
default constructor independently of the SOCKS5 authenticator.

## Local validation

- `cargo fmt --all -- --check` and `git diff --check`: passed.
- Cipher-name mapping: 1 passed. Config acceptance/rejection in both directions:
  2 passed. VMess stream tests: 10 passed, including all explicit cipher modes.
- `RUST_MIN_STACK=16777216 cargo test --workspace --all-features`: exit 101;
  completed results total 1895 passed, 2 failed, 125 ignored. Fail-fast stopped
  the remaining targets/doctests; this is not a clean full-workspace gate.
- Both failures are in unchanged shared Hysteria masquerade tests (`listing`
  and extension `sniff`). The ordinary environment reproduces 4 passed/2 failed
  in that six-test group. These fixtures construct `Appearance::File` directly
  from the non-canonical macOS temporary path. The production `Masquerade::file`
  constructor canonicalizes the root. Re-running the same binary with only
  `TMPDIR` resolved to its canonical path gives 6 passed/0 failed. No Hysteria
  source or fixture was changed, and this focused result does not replace the
  failed workspace result.
- VMess external-reference interoperability was not separately rerun. No commit,
  push, deployment or production acceptance is included.

Logs: `/tmp/zero-scope-20260918-workspace-final.log`,
`/tmp/zero-scope-20260918-cipher-config.log`,
`/tmp/zero-scope-20260918-masquerade-recheck.log`,
`/tmp/zero-scope-20260918-masquerade-canonical-temp.log`.
