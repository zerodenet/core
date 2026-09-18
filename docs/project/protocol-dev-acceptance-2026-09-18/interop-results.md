# Interoperability and gate results

## Source state and execution rules

- Repository commit: `8fcc4ecb8f567e092475387033c1cd415aaaeb1f`
- Execution date/time zone: 2026-09-18, Asia/Shanghai
- External artifacts: isolated under
  `/private/tmp/zero-dev-acceptance-tools-run1`
- Rust temporary directory: `TMPDIR=/private/tmp`
- Full-suite stack: `RUST_MIN_STACK=16777216`
- Every passing interop case below checks application payload round trips. A
  process start, listener readiness, handshake, ignored test, or compile-only
  result was not counted as passing interoperability.

## Fixed references

| Protocol | Selected reference | Immutable identity | Package / lock | Test tool / CI | Result |
|---|---|---|---|---|---|
| SOCKS5 | RFC 1928 + RFC 1929 | RFC baseline; no upstream application release | `0.0.1` / `0.0.1` | Local binary-level tests | Aligned |
| HTTP | RFC 7231-era CONNECT + implemented HTTP/1.x proxy semantics | RFC baseline; no upstream application release | `0.0.1` / `0.0.1` | Local binary-level tests | Aligned for confirmed inbound scope |
| Shadowsocks | shadowsocks-rust v1.21.2 | `a03006a753486e64717d6e3afa91e0c6d043c557` | `1.21.2` / `1.21.2` | `prepare-ss-interop.py` and dedicated CI use v1.21.2 | Aligned |
| VMess | Xray-core v26.3.27 | `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0` | `26.3.27` / `26.3.27` | Baseline checker and dedicated VMess CI use the exact verified Xray artifact | Aligned |
| VLESS | Xray-core v26.3.27 | `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0` | `26.3.27` / `26.3.27` | Baseline checker and VLESS/XHTTP CI run the complete Xray-named matrix | Aligned |
| Trojan | trojan-go v0.10.6 | annotated tag peels to `2dc60f52e79ff8b910e78e444f1e80678e936450` | `0.10.6` / `0.10.6` | Dedicated CI verifies pinned Xray and builds the exact Trojan-Go commit with official `full` tags | Aligned |
| Hysteria2 | Hysteria app/v2.12.2 | `619a6f856b69fb7ee6a7a379e810e68b84004605` | `2.12.2` / `2.12.2` | Hysteria2 CI verifies manifest/lock, exact commit and binary checksum; sing-box remains supplementary | Aligned |
| Mieru | official v3.33.0 | annotated tag peels to `48ddb69d5d343d76c9004c5054ee36609579ba13` | `3.33.0` / `3.33.0` | preparation script and dedicated CI enforce v3.33.0 | Aligned |

`git ls-remote` returned no `refs/tags/v25.3.1` for Xray and returned the
expected `v26.3.27` commit. For annotated tags, both tag-object and peeled
commit identities were recorded.

Local artifact hashes used in this run:

- Xray v26.3.27: `afd0eaebb77994a18f29b00c5f50a4f7fbb77da06e24352d43035f3cad3c3786`
- Shadowsocks v1.21.2 release archive:
  `d47509328f0f267889154dd207e658f92f8b43c34c31800069a223847ff113cc`
- Mieru v3.33.0 reference probe:
  `293ec89279b3dac5eae570dc424421c72861f7bef94c121fc2d5e416dbdd5385`
- Hysteria v2.12.2 source build:
  `d87ba3de39c1e804012619038bfc790426918536a5541793b54c40c0c9cf16dd`
- sing-box v1.13.14 `with_quic` supplementary build:
  `b0a9fa64c62723a26470a940f6cd6b02cffcb911ef5f4eb08b4776de8b39db7a`
- Trojan-Go v0.10.6 `full` build:
  `0a742158dff1608cfda9b7efbcb77c45d53aa7abee73598d0a8dab84da384a65`

## External and binary-level results

| Suite | Result | Coverage actually exercised |
|---|---:|---|
| SOCKS5/HTTP/Mixed binary-level targets | 64/64 | SOCKS5 TCP and UDP lifecycle/reuse; HTTP CONNECT, forward proxy, persistence, body framing, Upgrade and routing; combined listener dispatch |
| shadowsocks `reference_interop` | 2/2 | Official client→Zero and Zero→official, TCP+UDP, every release method selected by the fixture |
| shadowsocks `external_sip023` | 5/5 | SIP023/2022 TCP+UDP, both directions, large/sustained payload behavior |
| VMess fixed-Xray matrix | 13/13 | Both directions: AEAD TCP/UDP, WS, gRPC, none and standard zero; explicit zero-plus rejection |
| VLESS fixed-Xray matrix | 16/16 | TCP/UDP, WS, gRPC, XHTTP stream-one, XUDP, MUX, relay, rejection and REALITY+Vision outbound |
| VLESS REALITY extensions | 3/3 | Official bidirectional ML-DSA path, versioned ClientHello profiles and target fallback |
| VLESS ordinary TLS+Vision | 2/2 | Pinned Xray both directions |
| Trojan raw Xray matrix | 5/5 | TLS TCP/UDP both directions and client fingerprint |
| Trojan WS Xray matrix | 2/2 | Real TCP echo both directions |
| Trojan fixed-Xray matrix | 9/9 | Raw TLS TCP/UDP, fingerprint, WS and gRPC payload echo in both directions |
| Trojan-Go v0.10.6 | 2/2 | Each direction completed TCP and UDP payload echo |
| Hysteria official v2.12.2 | 12/12 | TCP, fragmented UDP, both directions, Brutal/BBR profiles, loss/bandwidth/windows/shared-auth/website plus Salamander/CA/pin/hopping |
| Hysteria new-node security/hopping | 1/1 | Salamander + CA + pin + observed multi-port hopping to official server |
| Hysteria TLS policy | 5/5 | trusted/insecure paths plus correct node security; wrong CA and wrong pin rejected |
| Hysteria sing-box supplement | 6/6 | TCP/UDP both directions, wrong password and auth reload |
| Mieru official v3.33.0 | 8/8 | TCP/UDP underlays, multiplexed TCP/UDP, stalled-session isolation/reuse, traffic pattern and MTU |

## Remediated findings

### VLESS REALITY+Vision outbound

The initial audit failed this path in a loaded run with:

```text
REALITY: processed invalid connection from 127.0.0.1:...: failed to read client hello
```

Trace runs showed the real path naturally taking about five to six seconds and
passing repeatedly. Under the original ten-second bound it could be starved by
a loaded workspace run. The test deadline is now 30 seconds, the exact case
passed 10/10 diagnostic repetitions, the serial fixed-Xray matrix passed 16/16,
and the complete Xray-named matrix is now required by CI. No VLESS production
wire change was necessary.

### Trojan gRPC outbound

The root cause was the protocol-owned outbound TLS profile advertising no ALPN
for the gRPC carrier. Carrier materialization now adds `h2` only for gRPC; raw
Trojan TLS and WS retain their prior ALPN behavior. The serial fixed-Xray
matrix passes 9/9, Trojan-Go passes 2/2, local regression coverage proves raw
TLS does not negotiate `h2`, and dedicated fixed-baseline CI was added.

## Setup-only failures excluded from product results

These were corrected and rerun; they are retained so the evidence is not
silently cleaned up:

- Initial sing-box build lacked `with_quic`; all six Hysteria tests then failed
  with `QUIC is not included in this build`. Rebuilt with the official tag and
  `-tags with_quic`; 6/6 passed.
- Initial Trojan-Go build lacked the official `full` tags and did not start the
  required transports. Rebuilt at the same commit with `-tags full`; 2/2
  passed.
- The first new Hysteria hopping fixture used invalid keepalive `1`; config
  validation correctly rejected it. Changed the test fixture to the supported
  value `2`; the intended interop test passed.
- The first VMess filter also selected two supplementary sing-box cases without
  the binary environment. With the fixed supplementary tool path, the selected
  9/9 matrix passed.

No ignored/skipped case or the setup-only failing run is counted as product
interoperability evidence.

## Workspace gate

The Connector failure was storage-policy dependent rather than a timing bug:
with a nearly full volume, the percentage reserve exceeded the available
space, so even a tiny durable ACK could not be written after the remote 204.
Maintenance records now reserve `min(configured reserve, 64 MiB)` while normal
PUT records still honor the full configured reserve. Connector's regular
event-dispatcher and webhook tests pass (12/12 and 17/17).

The complete canonical command subsequently finished with exit 0:

```bash
TMPDIR=/private/tmp RUST_MIN_STACK=16777216 \
  cargo test --workspace --all-features
```

An automatic-OCSP test-server race exposed by a prior full run was fixed by
explicitly shutting down the response stream after writing the close-delimited
body. The exact test passed 20/20 diagnostic repetitions and also passed in the
final workspace run.
