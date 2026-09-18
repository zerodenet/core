# Protocol dev acceptance — 2026-09-18

This directory records the capability and external-interoperability acceptance
performed against repository state
`8fcc4ecb8f567e092475387033c1cd415aaaeb1f`. The worktree was already dirty and
remained dirty throughout the audit. No reset, cleanup, commit, push,
deployment, or production-node access was performed. The remediation aligned
the already-selected VMess and Hysteria2 package metadata with their fixed
reference versions; it did not select a newer baseline.

## Decision

**The remediated current tree passes the dev release gate and may be built for
production trial.** All five finite blockers from the initial audit are closed:

1. The VLESS REALITY+Vision case passes against pinned Xray v26.3.27. The old
   ten-second test deadline was a loaded-run false failure; the bounded
   deadline is now 30 seconds and the exact case also passed 10/10 repeated
   runs during diagnosis.
2. Trojan gRPC materializes `h2` ALPN only for the gRPC carrier. Raw TLS keeps
   an empty ALPN list. The fixed-Xray matrix now passes 9/9, including gRPC in
   both directions, and metadata truthfully lists `tcp`, `tls`, `ws`, `grpc`.
3. VMess package/lock/reference tooling are aligned to Xray v26.3.27 and a
   dedicated fixed-baseline CI workflow was added.
4. Hysteria2 package/lock/reference tooling and capability metadata are aligned
   to official app/v2.12.2; the official matrix passes 12/12.
5. Connector maintenance acknowledgements may now drain a full outbox while
   retaining a bounded 64 MiB emergency floor. The complete workspace suite is
   clean. A separate OCSP test-server EOF race exposed during full-suite reruns
   was also stabilized with an explicit response-stream shutdown.

The initial failing logs remain listed as historical evidence. Current
remediation evidence is recorded in [remediation.md](remediation.md).

## Protocol disposition

| Protocol | Agreed-scope disposition | External data-plane result | Release effect |
|---|---|---|---|
| SOCKS5 | Aligned: inbound/outbound CONNECT and UDP ASSOCIATE, auth and address types | Local end-to-end suites passed | Ready; BIND and UDP FRAG are confirmed exclusions |
| HTTP | Implemented inbound CONNECT and HTTP/1.x forward-proxy behavior | Local end-to-end suites passed | Ready for confirmed scope; outbound and HTTP auth remain scope decisions |
| Mixed | SOCKS5 and HTTP detection/dispatch on one listener works | Local end-to-end suites passed | Ready; HTTP branch remains unauthenticated by documented scope |
| Shadowsocks | Aligned with shadowsocks-rust v1.21.2 in selected cipher/plugin/state scope | Official TCP+UDP both directions and SIP023 passed | Ready |
| VMess | TCP/UDP, AEAD/none/standard zero, WS/gRPC and MUX paths implemented; zero-plus remains private | Pinned Xray 13/13; zero-plus rejection proved | Ready; package, tool and CI baseline aligned |
| VLESS | Broad TCP/UDP, carrier, XUDP/MUX, Vision and REALITY-extension coverage present | Pinned Xray 16/16, including REALITY+Vision outbound | Ready |
| Trojan | Raw TLS TCP/UDP, WS and gRPC work both ways; private Mux.Cool is not smux evidence | Xray 9/9 and Trojan-Go v0.10.6 2/2 | Ready; public descriptor aligned |
| Hysteria2 | Independent inbound/outbound path includes Salamander, hopping, CA/pin, congestion and QUIC controls | Official v2.12.2 12/12, policy 5/5, sing-box 6/6 | Ready; package, lock, CI and descriptor aligned |
| Mieru | Aligned for selected TCP/UDP underlays, TCP/UDP sessions, multiplexing, reliability and policy behavior | Official v3.33.0 8/8 passed | Ready; production long-run recovery remains a declared limitation |

## What was newly proved in this acceptance

- Standard VMess `zero` completed real TCP echo in both Xray directions.
  `zero-plus` was separately rejected by Xray and is not counted as official
  compatibility.
- Hysteria2 Salamander plus a trusted CA plus the correct certificate pin
  reached an official v2.12.2 server. Five front UDP ports were observed for
  more than one hopping interval, and the test asserted that at least two
  distinct remote ports carried packets. Wrong CA and wrong pin were both
  rejected.
- Trojan TLS+WS and TLS+gRPC completed real TCP echo in both Xray directions.
- The VMess non-ASCII UUID regression is covered by the current local suite and
  the workspace run; it no longer panics.

## Scope decisions, not hidden defects

- SOCKS5 BIND and UDP fragmentation are status 4 (confirmed excluded).
- HTTP outbound, HTTP inbound authentication, and authentication of Mixed's
  HTTP branch are status 5 (scope pending). They do not block this dev decision
  unless product scope explicitly adds them.
- Trojan's Mux.Cool is an internal/private extension. No Trojan-Go smux claim is
  made, and a Zero-to-Zero MUX success is not used as smux evidence.
- Different Mieru/VLESS scheduling, pooling or recovery algorithms are not
  defects by themselves when observable behavior and required wire semantics
  pass.
- WireGuard was not inspected or tested.

## Non-blocking production-trial work

Production trial should validate long-lived
connections, NAT rebinding and mobile network churn, real multi-region port
hopping, certificate/pin rotation, reload under traffic, resource ceilings,
loss/reordering behavior over hours, and packaged artifacts on production
platforms. These are intentionally not prerequisites for this dev gate.

See [capability-matrix.md](capability-matrix.md),
[interop-results.md](interop-results.md), and [commands.md](commands.md) for the
row-level evidence and reproduction instructions.
