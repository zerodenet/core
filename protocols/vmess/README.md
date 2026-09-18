# VMess

VMess is the V2Ray/Xray-family proxy protocol implementation used by Zero.
This crate owns VMess protocol behavior: AEAD request/response headers, body
framing, TCP stream state, UDP packet framing, and Mux.Cool frame encoding.

## Current State

| Capability | State |
|------|------|
| TCP handshake | experimental |
| TCP body stream | experimental |
| TCP inbound | experimental |
| TCP outbound | experimental |
| UDP-over-stream | experimental |
| Mux.Cool TCP sub-connection | experimental |
| Mux.Cool UDP sub-connection | experimental |
| Body AEAD authenticated length | implemented |
| Body AEAD chunk masking (SHAKE128) | implemented |
| Body AEAD global padding | implemented |
| Body AEAD periodic rekey (2^14 chunks) | implemented |
| `cipher: auto` | normalized to the current AEAD baseline |
| `cipher: none` | implemented; Xray TCP interoperability is covered |
| `cipher: zero` | implemented with Xray's unchunked NONE body semantics |
| `cipher: zero-plus` | implemented for Zero-to-Zero paths; not claimed as mainstream external compatibility |

Supported explicit cipher names:

- `aes-128-gcm`
- `chacha20-poly1305`
- `none`
- `zero`
- `zero-plus`

`auto` is accepted as a config/import alias and maps to the current AEAD
baseline.

## Boundary

Protocol handshake, key derivation, response validation, body chunk
encryption/decryption, VMess UDP packet encoding, and MUX frame encoding stay in
this crate. The proxy runtime owns transport connection setup, routing,
session lifecycle, stats, events, and wrapping authenticated streams in
`VmessAeadStream` or `VmessMuxStream`.

In-tree validation covers bidirectional body relay for explicit ciphers,
shutdown termination chunks, UDP packet framing, raw TLS, WSS, gRPC, TCP MUX,
UDP-over-stream, MUX UDP, and same-protocol VMess UDP relay-chain paths.

External interoperability currently covers Xray TCP and UDP in both directions,
Xray WS/gRPC TCP in both directions, Zero outbound to sing-box inbound TCP/UDP,
and Mihomo outbound to Zero inbound TCP/UDP. `cipher: zero-plus` remains a
Zero-to-Zero capability, not a mainstream external compatibility claim.

## File Layout

```text
src/lib.rs       - crate root and re-exports
src/crypto.rs    - key derivation, AEAD header seal/open, body AEAD state (BodyAead), rekey, padding, chunk masking
src/inbound.rs   - inbound accept with single-user and multi-user auth, response header
src/outbound.rs  - outbound TCP session establishment, request encoding
src/stream.rs    - VmessAeadStream (AsyncRead + AsyncWrite) for bidirectional body relay
src/shared.rs    - cipher enum, address helpers, constants, read_exact, shared outbound session helpers
src/metadata.rs  - protocol capability descriptor
src/mux.rs       - Mux.Cool frame encode/decode, VmessMuxStream
src/udp.rs       - UDP packet encode/decode, UDP packet-session and UDP-over-stream establishment
```

`zero` now means Xray's unchunked NONE body semantics. Existing Zero-private
`zero` configurations must explicitly migrate to `zero-plus`; the private wire
format remains unchanged. See [cipher semantics](../../docs/protocols/vmess/shared.md).
