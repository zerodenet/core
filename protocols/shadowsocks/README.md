# Shadowsocks

The implementation baseline is **shadowsocks-rust 1.21.2**, commit
`a03006a753486e64717d6e3afa91e0c6d043c557`, with its locked
`shadowsocks-crypto 0.5.5`. The package version identifies this baseline; it is
not, by itself, evidence of completeness.

## Protocol scope

TCP and UDP support the complete reference method catalog, including optional
`aead-extra`, `stream-cipher` and `aead-cipher-2022-extra` methods:

- Plain: `none`, `plain`.
- Stream: `table`, `rc4`, `rc4-md5`, `chacha20-ietf`; AES and Camellia with
  128/192/256-bit keys in CTR, CFB1, CFB8, CFB128 (`cfb`/`cfb128`) and OFB modes.
- AEAD: AES-128/256-GCM, ChaCha20-IETF-Poly1305, XChaCha20-IETF-Poly1305,
  AES-128/256-CCM, AES-128/256-GCM-SIV, SM4-GCM and SM4-CCM.
- AEAD 2022: `2022-blake3-aes-128-gcm`, `2022-blake3-aes-256-gcm`,
  `2022-blake3-chacha20-poly1305`, `2022-blake3-chacha8-poly1305`.

Method parsing is case-insensitive; the empty method alias maps to `table`.
2022 passwords are standard base64, decoding to 16 bytes for AES-128 and 32
bytes for the other methods. AES 2022 outbound passwords support
`iPSK[:iPSK...]:uPSK`; inbound `identity_password` is a static iPSK and `users`
contains the atomically replaceable uPSKs. SIP023 EIH applies only to AES 2022.

The implementation includes stateful TCP framing and partial I/O, legacy target
continuation across chunks, SIP022 response binding, replay protection, bounded
failed-handshake drain, per-user UDP association isolation, endpoint migration,
stable sender sessions, packet counters, empty datagrams and state reclamation.
SIP003/SIP003u plugins support TCP-only, UDP-only and combined carrier modes.

## Ownership

- `validation/`: cipher/key/policy/plugin parsing without runtime dependencies.
- `inbound/`, `outbound/`, `stream/`: TCP handshake and encrypted stream state.
- `udp/`: datagram framing, identity, replay, response and association state.
- `shared/`: KDF, AEAD primitives, address encoding and wire helpers.
- `transport/`: protocol-owned carrier/leaf plans and plugin process leases.
- Zero proxy runtime: neutral accept loops, sockets, route dispatch, cancellation,
  accounting and native management. It does not parse SS keys or frames.

## Validation and operations

See [the implementation matrix](../../docs/protocols/shadowsocks/parity.md) and
[native configuration](../../docs/protocols/shadowsocks/configuration.md).
`reference_ciphers` compares all v1 methods with the exact pinned crypto
backend; `reference_2022` covers optional ChaCha8. `reference_interop` exercises
every method/alias included in the official release binary in both TCP and UDP
directions; `external_sip023` covers EIH and sustained UDP sessions.

```sh
python3 scripts/prepare-ss-interop.py --output /tmp/ss-reference
SS_RUST_BIN_DIR=/tmp/ss-reference cargo test -p shadowsocks --all-features --test reference_interop --test external_sip023 -- --ignored
cargo test -p zero-proxy --all-features --test shadowsocks_plugins --test shadowsocks_isolation
```

Production qualification is a separate final operational step, not an
unimplemented protocol capability. Zero native APIs provide management; no
`ssmanager` or subscription-tool control dialect is added to the kernel.
