# Capability matrix

Status values are deliberately evidence-sensitive:

1. **Implemented and verified** — configuration, runtime wiring and an
   applicable payload/lifecycle test all exist.
2. **Implemented, evidence incomplete** — implementation and entry point are
   present, but this run lacks the required external or behavioral proof.
3. **Missing, disconnected or failing** — the entry point is absent, cannot
   execute the advertised capability, or fails the selected reference.
4. **Confirmed excluded** — explicitly outside the agreed product scope.
5. **Scope pending** — upstream/RFC capability exists or is plausible, but the
   product requirement has not been confirmed.

The “official source” column identifies the comparison checklist, not a demand
to reproduce upstream application internals. Relative implementation links are
to the audited working tree.

## SOCKS5 and HTTP entry protocols

| Capability and official source | Project scope | Implementation and entry | Verification evidence | Status |
|---|---|---|---|---:|
| SOCKS5 CONNECT, IPv4/IPv6/domain and RFC 1929 auth — [RFC 1928](https://www.rfc-editor.org/rfc/rfc1928), [RFC 1929](https://www.rfc-editor.org/rfc/rfc1929) | Inbound and upstream outbound | [`protocols/socks5/src/inbound.rs`](../../../protocols/socks5/src/inbound.rs), [`outbound.rs`](../../../protocols/socks5/src/outbound.rs); config variants feed `Socks5Adapter` through the registry | SOCKS5 target: 19/19; TCP payload and auth/routing cases complete | 1 |
| SOCKS5 UDP ASSOCIATE, address framing and association lifecycle — RFC 1928 §7 | Inbound and upstream outbound | [`udp/`](../../../protocols/socks5/src/udp), proxy packet-path/association runtime | UDP targets: 26/26 plus idle 1/1 and reuse 1/1; payload echo, peer/lifecycle and reuse exercised | 1 |
| SOCKS5 unsupported command/feature rejection | Reject explicitly rather than falling through | Parser/dispatch rejects unknown commands; UDP decoder accepts only `FRAG=0` | Local negative tests in full suite | 1 |
| SOCKS5 BIND | Explicitly excluded | No supported runtime entry | Scope file and user instruction | 4 |
| SOCKS5 UDP fragmentation (`FRAG != 0`) | Explicitly excluded | [`protocols/socks5/src/shared.rs`](../../../protocols/socks5/src/shared.rs) rejects it | Negative parser coverage | 4 |
| HTTP CONNECT — [RFC 7231 §4.3.6](https://www.rfc-editor.org/rfc/rfc7231#section-4.3.6) | Inbound | [`protocols/http/src/inbound.rs`](../../../protocols/http/src/inbound.rs), HTTP adapter/listener | HTTP suite 12/12 includes real CONNECT relay | 1 |
| HTTP/1.0 and HTTP/1.1 absolute-form forward proxy, bodies, persistence and Upgrade | Inbound, part of current implemented behavior | [`body.rs`](../../../protocols/http/src/body.rs), [`parse.rs`](../../../protocols/http/src/parse.rs), [`wire.rs`](../../../protocols/http/src/wire.rs); per-request routing in proxy adapter | HTTP 12/12 covers fixed/chunked/close bodies, repeated requests, route changes, POST, Upgrade success/failure | 1 |
| HTTP outbound proxy | Requirement not confirmed | `HttpConnect` is inbound-only; capability descriptor reports outbound unsupported | Code inspection; no public outbound config | 5 |
| HTTP inbound user authentication | Requirement not confirmed | `HttpConnect` has no user field and constructs the default handler | Code inspection; scope clarification | 5 |
| Mixed protocol discrimination and combined listener | SOCKS5 + HTTP on one port | Mixed adapter sniffs then delegates to the same SOCKS5/HTTP handlers | Mixed suite 5/5 with both protocols and persistent HTTP behavior | 1 |
| Mixed HTTP-branch authentication | Requirement not confirmed; current `socks5_users` applies only to SOCKS5 | Mixed HTTP handler is constructed independently of SOCKS5 authenticator | Code inspection and scope clarification | 5 |

## Shadowsocks

Baseline: [shadowsocks-rust v1.21.2](https://github.com/shadowsocks/shadowsocks-rust/tree/a03006a753486e64717d6e3afa91e0c6d043c557),
commit `a03006a753486e64717d6e3afa91e0c6d043c557`, plus SIP022/SIP023 behavior
implemented by that release.

| Capability and official source | Project scope | Implementation and entry | Verification evidence | Status |
|---|---|---|---|---:|
| TCP inbound/outbound and IPv4/IPv6/domain targets | Both directions | [`inbound/`](../../../protocols/shadowsocks/src/inbound), [`outbound/`](../../../protocols/shadowsocks/src/outbound), transport leaf and proxy adapter | Official v1.21.2 both directions with TCP payload echo | 1 |
| UDP inbound/outbound, session isolation and packet addresses | Both directions | [`udp/`](../../../protocols/shadowsocks/src/udp), adapter-provided packet path and codec | Official v1.21.2 both directions with UDP payload echo; proxy isolation target passed | 1 |
| Legacy stream, AEAD 2017, AEAD 2022 cipher families exposed by the release fixture | Selected release methods, with invalid combinations rejected | [`shared/legacy.rs`](../../../protocols/shadowsocks/src/shared/legacy.rs), [`aead.rs`](../../../protocols/shadowsocks/src/shared/aead.rs), [`extra_aead.rs`](../../../protocols/shadowsocks/src/shared/extra_aead.rs), protocol-owned validation | `reference_interop` 2/2 runs TCP+UDP over every configured release method | 1 |
| SIP023/2022 identity headers, multi-user lookup and EIH | In scope | [`shared/eih.rs`](../../../protocols/shadowsocks/src/shared/eih.rs), [`inbound/identity.rs`](../../../protocols/shadowsocks/src/inbound/identity.rs), validation/users | External SIP023 suite 5/5, including both directions and sustained/large payloads | 1 |
| Replay, timestamp/window and bounded identity/session state | In scope as correctness/security behavior | [`shared/replay.rs`](../../../protocols/shadowsocks/src/shared/replay.rs), [`window.rs`](../../../protocols/shadowsocks/src/shared/window.rs), inbound store and transport state | Unit/integration targets passed plus live official payload suites | 1 |
| SIP003-style plugin process integration and failure isolation | In scope where configured | [`transport/plugin.rs`](../../../protocols/shadowsocks/src/transport/plugin.rs), config validation, proxy plugin tests | Local plugin/isolation targets passed; official release matrix traverses the normal transport leaf | 1 |
| Protocol-native MUX | Not a selected Shadowsocks wire capability | Metadata reports unsupported; other generic carriers are not counted as Shadowsocks MUX | Explicit capability descriptor | 4 |

## VMess

Actual comparison baseline: [Xray-core v26.3.27](https://github.com/XTLS/Xray-core/tree/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0),
commit `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`. Package, generated lockfile,
reference preparation and dedicated CI all use `26.3.27`.

| Capability and official source | Project scope | Implementation and entry | Verification evidence | Status |
|---|---|---|---|---:|
| Xray AEAD request/response header, timestamp/auth ID and IPv4/IPv6/domain targets | Inbound/outbound | [`auth.rs`](../../../protocols/vmess/src/auth.rs), [`crypto.rs`](../../../protocols/vmess/src/crypto.rs), [`inbound.rs`](../../../protocols/vmess/src/inbound.rs), [`outbound.rs`](../../../protocols/vmess/src/outbound.rs) | Fixed Xray payload tests in both directions | 1 |
| TCP plus UDP command/packet mode | Both directions | [`stream.rs`](../../../protocols/vmess/src/stream.rs), [`udp.rs`](../../../protocols/vmess/src/udp.rs), prepared TCP/managed UDP adapter operations | Xray TCP/UDP payload echo both directions | 1 |
| `aes-128-gcm`, `chacha20-poly1305`, `none` | In scope | Protocol-owned cipher parser/stream; config validation rejects bad values | Fixed Xray AEAD/none cases plus full local cipher suite | 1 |
| Standard Xray `zero` (`NONE`, no ChunkStream/ChunkMasking) | In scope; distinct from private mode | Dedicated raw-body path in [`shared.rs`](../../../protocols/vmess/src/shared.rs) and [`stream.rs`](../../../protocols/vmess/src/stream.rs); `cipher: zero` accepted by config | Zero→Xray and Xray→Zero real TCP echo passed | 1 |
| Private `zero-plus` security `0x06` | Zero-only extension; must not be called official | Separate enum/config name and chunked wire path | Zero→Zero local tests; pinned Xray rejection passed | 1 |
| TLS, TLS+WebSocket and TLS+gRPC | Both directions | [`transport/`](../../../protocols/vmess/src/transport), common transport stack and config carrier fields | Fixed Xray raw/WS/gRPC payload echo both directions | 1 |
| Mux.Cool TCP and UDP | In scope as exposed capability | [`mux.rs`](../../../protocols/vmess/src/mux.rs), pool/runtime bridge and managed UDP plan | Local MUX targets passed; official Xray-facing matrix covers declared external paths | 1 |
| UUID parsing must reject malformed/non-ASCII input without panic | In scope | [`validation.rs`](../../../protocols/vmess/src/validation.rs) and regression tests | The explicit non-ASCII regression test passed | 1 |
| Fixed package/lock/reference identity | Required release traceability | Manifest and generated lockfile say `26.3.27`; preparation script checks the exact Xray commit; dedicated VMess CI uses the same artifact | Baseline check plus fixed Xray 13/13 | 1 |

## VLESS

Baseline: [Xray-core v26.3.27](https://github.com/XTLS/Xray-core/tree/d2758a023cd7f4174a5a5fa4ff66e487d4342ba0),
commit `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`.

| Capability and official source | Project scope | Implementation and entry | Verification evidence | Status |
|---|---|---|---|---:|
| Base TCP/UDP, UUID/auth, IPv4/IPv6/domain | Both directions | [`inbound.rs`](../../../protocols/vless/src/inbound.rs), [`outbound.rs`](../../../protocols/vless/src/outbound.rs), [`udp/`](../../../protocols/vless/src/udp), config/adapter operations | Fixed Xray TCP/UDP and wrong-user cases pass | 1 |
| TLS, WS, gRPC, H2 and HTTP Upgrade carrier families | Supported configured carriers | Protocol transport profiles plus common carrier stack | Xray WS/gRPC passed here; remaining carrier integration targets completed in the non-connector run | 1 |
| XHTTP stream-one and relay-final-hop behavior | In scope | Common XHTTP transport plus VLESS transport leaf/runtime bridge | Pinned Xray TCP/UDP both directions and SOCKS relay cases pass | 1 |
| Mux.Cool TCP and XUDP, reset/reattach and concurrent associations | In scope; internal pooling policy need not match Xray | [`mux/`](../../../protocols/vless/src/mux), [`mux_pool.rs`](../../../protocols/vless/src/mux_pool.rs), UDP pump/packet path | Pinned Xray MUX/XUDP directions and reattach cases pass | 1 |
| Ordinary TLS+XTLS Vision | In scope | [`vision/`](../../../protocols/vless/src/vision), flow/addon handling and TLS handoff | Dedicated fixed-Xray suite 2/2 | 1 |
| REALITY base/client profiles, target fallback and ML-DSA extension behavior | In scope where exposed | [`reality/`](../../../protocols/vless/src/reality), [`reality_policy.rs`](../../../protocols/vless/src/reality_policy.rs), configured transport leaf | Dedicated extensions suite 3/3 with pinned Xray | 1 |
| REALITY + Vision Zero outbound → Xray inbound | Advertised supported combination | Same REALITY stream plus Vision composition | Fixed-Xray matrix 16/16 includes this case; exact test passed 10/10 repeated diagnostic runs | 1 |
| VLESS encryption, reverse, fallback and early-data options exposed by config | In scope as documented | [`encryption/`](../../../protocols/vless/src/encryption), [`reverse/`](../../../protocols/vless/src/reverse), [`fallback.rs`](../../../protocols/vless/src/fallback.rs), common carrier profiles | Dedicated local targets passed; not all rows were separately re-run against an external process in this acceptance | 1 |
| QUIC/mKCP/Hysteria carrier implementation | Supported/deprecated according to metadata; not confused with standalone HY2 | Config and transport carrier factories, adapter projection | Dedicated carrier targets passed; no new official Xray process evidence in this run | 2 |

## Trojan

Package baseline: [trojan-go v0.10.6](https://github.com/p4gefau1t/trojan-go/tree/2dc60f52e79ff8b910e78e444f1e80678e936450),
commit `2dc60f52e79ff8b910e78e444f1e80678e936450`. Xray v26.3.27 is
also used for the shared WS/gRPC carrier compatibility checks.

| Capability and official source | Project scope | Implementation and entry | Verification evidence | Status |
|---|---|---|---|---:|
| TLS password-authenticated TCP CONNECT and address types | Both directions | [`inbound.rs`](../../../protocols/trojan/src/inbound.rs), [`outbound.rs`](../../../protocols/trojan/src/outbound.rs), protocol-owned TLS profile/leaf | Trojan-Go v0.10.6 TCP both directions; Xray raw matrix passes | 1 |
| UDP-over-stream framing and session behavior | Both directions | [`udp.rs`](../../../protocols/trojan/src/udp.rs), managed UDP plan/runtime | Trojan-Go and Xray UDP payload echo both directions | 1 |
| TLS verification/SNI/fingerprint configuration | In scope | Trojan TLS profile materialization and common TLS client; config validation | Pinned Xray fingerprint case and raw TLS cases pass | 1 |
| TLS+WebSocket carrier | Inbound/outbound | [`transport/inbound.rs`](../../../protocols/trojan/src/transport/inbound.rs), [`transport/outbound.rs`](../../../protocols/trojan/src/transport/outbound.rs), WS config fields | Fixed Xray payload echo 2/2 | 1 |
| TLS+gRPC inbound (official client→Zero) | In scope | Same transport plan and common gRPC acceptor | Fixed Xray client→Zero server payload echo passes | 1 |
| TLS+gRPC outbound (Zero→official server) | In scope | Protocol-owned TLS profile adds `h2` only when the selected carrier is gRPC | Fixed Xray payload echo passes; raw TLS no-ALPN regression also passes | 1 |
| Mux.Cool TCP/UDP | Zero capability only; no Trojan-Go smux claim | [`mux.rs`](../../../protocols/trojan/src/mux.rs), pool and UDP resume plan | Local MUX targets passed | 1 |
| Trojan-Go smux interoperability | Explicitly not required | No claim; Mux.Cool evidence is not reused | Scope decision | 4 |
| Public capability metadata includes exposed WS/gRPC | Required truthful release claim | [`metadata.rs`](../../../protocols/trojan/src/metadata.rs) lists `tcp`, `tls`, `ws`, `grpc` | Code inspection plus registry/full-workspace tests | 1 |

## Hysteria2

Actual official baseline: [Hysteria app/v2.12.2](https://github.com/HyNetwork/hysteria/tree/619a6f856b69fb7ee6a7a379e810e68b84004605),
commit `619a6f856b69fb7ee6a7a379e810e68b84004605`.

| Capability and official source | Project scope | Implementation and entry | Verification evidence | Status |
|---|---|---|---|---:|
| Independent HY2 inbound/outbound authentication and HTTP/3/QUIC handshake | Both directions; must not rely on VLESS carrier presence | [`handshake.rs`](../../../protocols/hysteria2/src/handshake.rs), [`transport/`](../../../protocols/hysteria2/src/transport), HY2 adapter/listener/leaf | Official v2.12.2 payload suite 12/12 | 1 |
| TCP streams and fragmented UDP relay | Both directions | [`shared/stream.rs`](../../../protocols/hysteria2/src/shared/stream.rs), [`udp/`](../../../protocols/hysteria2/src/udp), managed UDP bridge | Official TCP and fragmented UDP both directions | 1 |
| Brutal/BBR selection, bandwidth, flow-control windows and connection reuse | Exposed HY2 behavior | [`settings/`](../../../protocols/hysteria2/src/settings), congestion/pool/connection modules and config projection | Official 11/11 includes Brutal, BBR, loss/bandwidth/windows/shared authentication | 1 |
| Salamander obfuscation on the standalone HY2 entry | In scope | HY2 config fields projected through [`transport/projection.rs`](../../../protocols/hysteria2/src/transport/projection.rs) into the datagram profile | New Zero→official payload test passes with Salamander | 1 |
| Outbound remote port hopping must actually use multiple ports | In scope | Node endpoint/range config and hopping datagram profile | Five forwarding ports counted packets for >20s; assertion requires at least two distinct ports | 1 |
| Per-node CA trust and certificate SHA-256 pin | In scope | Node security fields, config validation and client TLS/QUIC projection | Correct CA+pin reaches official server; wrong CA and wrong pin both rejected | 1 |
| Password rejection, credential reload/revocation | In scope | Auth store/listener reload path | Supplementary fixed sing-box suite 6/6 includes wrong password and auth reload | 1 |
| Website/masquerade behavior | In scope where configured | [`transport/website.rs`](../../../protocols/hysteria2/src/transport/website.rs), inbound HTTP/3 handling | Official suite covers website/masquerade; dedicated targets pass with canonical TMPDIR | 1 |
| Invalid option combinations and feature gating | Reject early | Protocol/config validation, registry capability claims and compiled features | Config/registry targets passed plus initial invalid keepalive rejection | 1 |
| Package/lock/reference and public capability status | Required release traceability | Manifest/generated lockfile, docs, CI and reference all use `2.12.2`; metadata reports supported and removes the stale incomplete-interop limitation | Manifest/lock/workflow inspection plus official 12/12 | 1 |

## Mieru

Baseline: [official v3.33.0](https://github.com/enfein/mieru/tree/48ddb69d5d343d76c9004c5054ee36609579ba13),
commit `48ddb69d5d343d76c9004c5054ee36609579ba13`.

| Capability and official source | Project scope | Implementation and entry | Verification evidence | Status |
|---|---|---|---|---:|
| TCP and native UDP underlays | Both directions | [`client/`](../../../protocols/mieru/src/client), [`inbound/`](../../../protocols/mieru/src/inbound), adapter transport selection | Official v3.33.0 both underlays pass | 1 |
| Multiplexed TCP CONNECT and UDP ASSOCIATE business sessions | Both directions | [`inbound/multiplex.rs`](../../../protocols/mieru/src/inbound/multiplex.rs), [`session.rs`](../../../protocols/mieru/src/session.rs), [`tunnel.rs`](../../../protocols/mieru/src/tunnel.rs) | Official 8/8 includes TCP+UDP over each underlay | 1 |
| Protocol crypto, metadata, replay and framing | In scope | [`crypto.rs`](../../../protocols/mieru/src/crypto.rs), [`metadata.rs`](../../../protocols/mieru/src/metadata.rs), [`segment.rs`](../../../protocols/mieru/src/segment.rs), packet codecs | Real official payload paths plus full unit suite | 1 |
| UDP reliability, ACK/window/reorder/retransmit/congestion | Observable reliability required; upstream algorithm identity not required | [`packet/reliability.rs`](../../../protocols/mieru/src/packet/reliability.rs), [`congestion.rs`](../../../protocols/mieru/src/packet/congestion.rs), driver/session | Official UDP-underlay cases, isolation/reuse and local loss/reorder tests pass | 1 |
| Stalled-session isolation and carrier reuse | Required behavior; pool policy may differ | Inbound multiplex queues and [`client/pool.rs`](../../../protocols/mieru/src/client/pool.rs) | Official suite explicitly holds a stalled session while other sessions complete | 1 |
| MTU and traffic-pattern configuration changes wire behavior | In scope | [`traffic_pattern/`](../../../protocols/mieru/src/traffic_pattern), segment budgets and config-owned Mieru ADTs | Four official cases cover inbound/outbound TCP/UDP policy combinations | 1 |
| Multiple official profile endpoints, exact scheduler/heartbeat/retry algorithms | Not required one-to-one; not used to shrink observable capability | Zero has its documented bounded pool and recovery policies | Scope decision plus passing observable tests | 4 |
| Production-length recovery/throughput | Deferred to production trial | Descriptor intentionally retains `long_running_recovery_is_not_verified` | Not performed in this acceptance | 2 |

## Cross-cutting feature and runtime wiring

| Capability | Evidence | Status |
|---|---|---:|
| Uncompiled protocol is rejected rather than silently accepted | Registry/config feature targets passed | 1 |
| Protocol-private parsing stays in owning crate | Source inspection of UUID/cipher/password/identity constructors and thin adapter projection | 1 |
| Capability exists at config, registry, prepared operation and execution layers | Source tracing plus end-to-end tests for every protocol family | 1 |
| External test is mandatory evidence rather than ignored-by-default success | All external suites were explicitly invoked with fixed environment and `--ignored --nocapture` or an exact ignored filter | 1 |
| CI enforces every fixed external baseline | SS, VLESS/Xray, VMess, Trojan, HY2 and Mieru have dedicated fixed-version jobs | 1 |
| Required full-workspace gate | `TMPDIR=/private/tmp RUST_MIN_STACK=16777216 cargo test --workspace --all-features` completed successfully after remediation | 1 |
