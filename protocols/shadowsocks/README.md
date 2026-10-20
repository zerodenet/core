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

See [protocol configuration](https://docs.zerodenet.org/projects/core/protocols/configuration)
in the standalone documentation site. The in-tree tests verify the pinned baseline:
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

## Native library Stream factory

With `runtime` + `blake3`, `ShadowsocksTransportState::prepare_tcp(TcpPreparation)`
validates framing while retaining that adapter's actual nonce history. It never
constructs a second pool and replaces it afterward. The host canonicalizes the
returned `replay_options()` with `ResourcePreparation::share` and the existing
roleless `ReplayFactory`, then passes its real ComponentId to `PreparedTcp::bind`.
`StreamFactory` declares that dependency and checks its actual generation and
registered driver ownership before admission; a claim or a pending legacy join
is not readiness. Cleanup preserves live salts and capacity rejection.

Connections consume Destination and OpenContext directly. Request writes and
flush return Deferred, with request bytes separated from physical Outer traffic.
Only a real SIP022 response that passes AEAD, type, timestamp, request-salt echo
and the shared replay check records PeerConfirmed. Legacy and plain streams keep
Deferred; payload arrival alone does not become CONNECT confirmation. Original
I/O errors remain in the source chain through the protocol's static error mapping.

This is a library-only path. Production registration remains unchanged until host
resource planning and confirmed replacement/I/O closure are complete. Configured
SIP003 plugins are explicitly rejected by this factory; their real PluginFactory
and child ownership remain intact, and a future Stream integration must consume
an already-ready lease without starting a child from connect. No UDP or inbound
process ownership is changed here.

## Ordinary inbound TCP handoff

The production TCP listener authenticates/decrypts the request and completes its
existing replay admission before returning Destination, full SessionAuth and the
original encrypted stream to the host's standard accepted Stream path. The host
mints the one business identity and keeps the existing policy, routing,
accounting, cancellation and half-close pipeline. Plain/stream methods, legacy
AEAD, SIP022 and SIP023 EIH share this handoff; framing, salts, nonce positions,
first-chunk remaining plaintext and response binding remain protocol-owned.

Rust callers may opt into `ShadowsocksInboundTcpAcceptor::accept_stream_facts`;
the Session-returning `accept_stream`, public `ShadowsocksAccept` fields and
deterministic response-salt APIs remain compatible. Acceptor clones retain the
same replay authorities, retention and capacity rules. Admission emits no new
wire acknowledgement or response-salt flush. SIP003 activation/Child ownership
and UDP execution retain their existing paths. No configuration, V1 API/event
shape or FFI ABI changes are required.

Regressions use the production proxy listener in
`crates/proxy/tests/shadowsocks_inbound.rs` and real native TCP in
`tests/inbound/facts.rs`, including pipelined plaintext, reverse half-close,
cross-clone replay rejection and retention after capacity failure. These tests
do not replace native resource-closure or full RuntimeHost qualification.
