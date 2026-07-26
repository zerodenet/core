# Shadowsocks

This crate owns the Shadowsocks protocol semantics used by Zero. The proxy
runtime owns orchestration, sockets, routing, sessions, stats, events, and
response bridging.

## Capability

| Area | Current fact |
|------|--------------|
| TCP inbound | Accepts AEAD stream requests (legacy + 2022 SIP022), selects AES 2022 users through SIP023 EIH, and returns `ShadowsocksAccept` |
| TCP outbound | Writes the initial target request (legacy + 2022 SIP022), emits SIP023 EIH for AES 2022 password chains, and returns `ShadowsocksOutboundSession` |
| TCP stream | `ShadowsocksAeadStream` owns chunk encryption, decryption, response salt, and download key derivation |
| UDP datagram | `UdpDatagramFraming` encodes and decodes Shadowsocks UDP packets; AES 2022 supports SIP023 EIH user selection and response encryption with the selected uPSK |
| UDP composition | `ShadowsocksDatagramCodec` is used by generic packet-path orchestration; Shadowsocks final-hop UDP chains support SOCKS5 and Shadowsocks packet-path carriers |
| MUX | Not applicable |

## Validation

In-tree validation covers these Shadowsocks paths:

- TCP outbound through a SOCKS5 inbound to a Shadowsocks inbound for every
  supported cipher listed below, including a large payload that crosses AEAD
  chunk boundaries.
- TCP authentication failure when the outbound password does not match the
  upstream Shadowsocks inbound password; the flow is closed before reaching the
  target service.
- UDP outbound through SOCKS5 UDP ASSOCIATE to a Shadowsocks inbound.
- UDP end-to-end relay for every supported cipher listed below.
- Shadowsocks UDP packet-path relay chains where the carrier is SOCKS5 UDP
  ASSOCIATE or Shadowsocks UDP.
- Local external UDP interoperability against `shadowsocks-rust ssserver -U`
  for every supported cipher listed below.
- SIP023 TCP and UDP EIH wire probes for both AES 2022 methods, password-chain
  outbound to EIH inbound round trips, selected-uPSK responses, and atomic
  managed-user replacement while retaining the static server iPSK.
- Bidirectional TCP and UDP interoperability with `shadowsocks-rust` 1.24.0 for
  both AES 2022 EIH methods (`sslocal` to Zero and Zero to `ssserver`). Run with
  `cargo test -p shadowsocks --all-features --test external_sip023 -- --ignored`.

Supported cipher names:

- `aes-128-gcm`
- `aes-256-gcm`
- `chacha20-ietf-poly1305`
- `2022-blake3-aes-128-gcm`
- `2022-blake3-aes-256-gcm`
- `2022-blake3-chacha20-poly1305`

For AEAD 2022 cipher names, `password` is standard base64 key material. The
decoded length must match the method key length: 16 bytes for
`2022-blake3-aes-128-gcm`, and 32 bytes for
`2022-blake3-aes-256-gcm` and `2022-blake3-chacha20-poly1305`. AES 2022 outbound
passwords may contain `iPSK[:iPSK...]:uPSK`; Zero validates every segment,
emits SIP023 TCP/UDP EIH, and uses the final uPSK for payload and responses.
Managed AES 2022 inbound profiles use a separate static `identity_password` and
an atomically replaceable uPSK user set. SIP023 EIH does not apply to the 2022
chacha20 method.

## Boundaries

```text
src/lib.rs       - crate root and re-exports
src/inbound.rs   - inbound request parsing and accept state
src/outbound.rs  - outbound TCP session and UDP datagram framing
src/shared.rs    - cipher enum, key derivation, address and target-data helpers
src/stream.rs    - AEAD stream wrapper
src/metadata.rs  - protocol capability descriptor
```

## Known Limits

- AEAD 2022 UDP **server-side responses** are implemented and validated (SIP022
  3.2.3 echo of client session id, DNS round-trip probe).
- **SIP022 3.2.4** per-session sliding-window replay filtering and per-client
  session-id flow isolation are implemented and tested.
- **SIP023** EIH is implemented for AES 2022 TCP and UDP, including O(1) user
  identity lookup. Both directions have been validated against independent
  `shadowsocks-rust` 1.24.0 TCP and UDP implementations.
- The remaining limitation is `shadowsocks_2022_hardening_not_externally_validated`:
  the detection-prevention drain and sliding-window replay filter have not been
  validated against real active probes/replay attacks.
