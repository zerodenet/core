# BLAKE3 binary derivation context

Pinned crates.io blake3 1.8.5, checksum
`0aa83c34e62843d924f905e0f5c866eb1dd6545fc4d719e803d9ba6030371fce`.
Original licenses are retained.

The only implementation change adds `Hasher::new_derive_key_bytes(&[u8])`;
`new_derive_key(&str)` delegates to it. Flags, compression, tree hashing, and
key-material derivation are unchanged. This provides byte-exact domain separation
for protocols that exchange binary contexts, without fabricating invalid UTF-8.
The patch contains no protocol fields or framing. Regression vectors live in the
owning protocol's encryption tests and are checked against the pinned Go reference.

The published crate omits the upstream test-only `reference_impl` dependency.
It is restored from upstream commit `93a431c78a52d7ccf0f366f106467f5070e6075e`
(the crate's recorded source commit) so the unchanged upstream library tests run.
