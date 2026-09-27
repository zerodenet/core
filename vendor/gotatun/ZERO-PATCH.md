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

This patch gives stored session keys an explicit drop-time erasure path. It does
not prove that every transient stack copy, crypto dependency internal state,
allocator page, or crash dump is erased. It is not a complete security audit
of GotaTun or its dependencies, and it does not by itself enable WireGuard in
Zero's default feature set. [GotaTun's published audit](https://github.com/mullvad/gotatun/blob/v0.9.2/audits/2026-02-17-Assured.md)
reports lower throughput for the RustCrypto backend than for `ring` in its
benchmark; Zero must measure
the impact in its own Packet/Flow workloads before treating this as a default
backend choice.
