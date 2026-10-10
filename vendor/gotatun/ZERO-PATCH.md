# Zero's GotaTun 0.9.2 patch

This directory is based on the pinned crates.io `gotatun` 0.9.2 package,
SHA-256 `4578da6dc5281756ef0277a8ab92fd390b472b87b939c89252130be9ca28812b`.
`Cargo.toml.orig` is the package's unmodified manifest. `LICENSE` and
`LICENSE-CLOUDFLARE` are from the matching upstream `v0.9.2` tag, since the
crates.io archive omits those files. Source headers and the upstream README are
retained.

Zero enables `zeroized-rustcrypto` without GotaTun's default feature. This
selects a small adapter over RustCrypto `ChaCha20Poly1305` in place of
`ring::aead::LessSafeKey` for Noise transport sessions and handshake AEAD.
The RustCrypto type implements `ZeroizeOnDrop`; the pinned 0.10.1 implementation
erases its stored key when dropped. The adapter preserves the existing nonce,
AAD, ciphertext, and detached-tag semantics. Both upstream backends remain
available when this feature is not selected. The same feature enables
`poly1305/zeroize` so the per-message MAC state also erases its internal key
material on drop.

The patch also enables `x25519-dalek`'s `zeroize` feature, wraps session-key
inputs and in-flight chaining-key state with `Zeroizing`, erases rate-limiter
secrets and configured preshared keys on drop/replacement, avoids temporary
preshared-key copies in the Noise KDF, and redacts secret fields from handshake
Debug output. Tests in `src/crypto/tests.rs` and
`src/noise/handshake/tests_zero.rs` cover the selected backend and redaction;
Zero's protocol and pinned external peer tests cover wire compatibility.

Zero additionally exposes `Packet::try_into_unpooled_buffer`, a safe ownership
operation on the existing packet storage. It moves an unpooled BytesMut with its
original slice offset and returns pooled packets intact. The protocol adapter
can therefore avoid a payload copy and an owner Box for ordinary engine output,
while keeping a pool return guard alive until its consumer finishes. This helper
does not parse packets, modify cryptography, alter pooling policy, or change the
pinned upstream implementation version. Zero's buffer lifetime tests cover the
direct-storage and retained-pool-guard paths.

This patch gives stored session keys an explicit drop-time erasure path. It does
not prove that every transient stack copy, crypto dependency internal state,
allocator page, or crash dump is erased. It is not a complete security audit
of GotaTun or its dependencies, and it does not by itself enable WireGuard in
Zero's default feature set. [GotaTun's published audit](https://github.com/mullvad/gotatun/blob/v0.9.2/audits/2026-02-17-Assured.md)
reports lower throughput for the RustCrypto backend than for `ring` in its
benchmark; Zero must measure
the impact in its own Packet/Flow workloads before treating this as a default
backend choice.

Zero also exposes `Tunn::next_timer_delay` in `noise/timers/deadline.rs`.
This read-only projection uses the same clock, sampled jitter, pending flags,
occupied session slots and predicates as `update_timers`. It schedules cookie
expiry, session-key destruction, connection expiration, handshake retries,
rekey, reactive keepalive and persistent keepalive without resampling or changing
wire behavior. Expired tunnels return `None` until I/O revives their state.
The strict session-expiry boundary adds one nanosecond to prevent a zero-delay
loop at equality; an executor may still round sleeps to its timer resolution.
Rate-limit counters are reset on packet receive, so they do not require an
independent idle wake. The adapter owns no duplicate WireGuard timer constants.

`tests/timer_deadline.rs` is an owner-level mock-clock regression suite; run:
`cargo test --manifest-path vendor/gotatun/Cargo.toml --target-dir target --no-default-features --features zeroized-rustcrypto,mock_instant --lib`.
The WireGuard interoperability workflow runs it separately so the thread-local
mock clock is never enabled in Zero's production or network-interoperability
feature graph. Updating the upstream timer state machine requires updating this
projection and its boundary tests together. The pinned engine version and the
existing cryptographic patches remain unchanged.

`noise/timers/expiration.rs` shares expiration checks between timer execution
and public crypto I/O boundaries. It stamps `TimeCurrent` at actual I/O time;
`timer_tick` then records packet-driven transitions against that timestamp.
Upstream's periodic device loop refreshes this time frequently; after a long
exact-deadline sleep, reusing the previous tick would backdate session keys,
cookies and keepalive resets. Encapsulation, decapsulation, handshake generation
and inbound handshake handling also expire old keys/attempts before use, so
executor/protocol clock differences after suspend cannot admit an expired data
packet or late handshake response. This helper only expires state; it never
emits and loses a pending retry/rekey packet. Timer execution uses the same
helper, so expiration rules are not independently implemented twice. The
mock-clock suite covers delayed I/O, rejected stale keys/responses and revival.
