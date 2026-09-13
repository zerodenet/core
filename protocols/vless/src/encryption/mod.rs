// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
//! VLESS Encryption, pinned to Xray-core v26.3.27.
//! Wire compatibility includes binary-context key derivation and authenticated
//! TLS-shaped records. Configuration parsing remains available without runtime.

pub mod config;
#[cfg(all(feature = "runtime", feature = "tokio"))]
mod crypto;
#[cfg(all(feature = "runtime", feature = "tokio"))]
mod keys;
#[cfg(all(feature = "runtime", feature = "tokio"))]
mod padding;
#[cfg(all(feature = "runtime", feature = "tokio"))]
mod stream;

#[cfg(all(feature = "runtime", feature = "tokio"))]
mod state;

#[cfg(all(feature = "runtime", feature = "tokio"))]
mod client;
#[cfg(all(feature = "runtime", feature = "tokio"))]
mod server;
#[cfg(all(feature = "runtime", feature = "tokio"))]
pub use {client::EncryptionClient, server::EncryptionServer, stream::EncryptionStream};

#[cfg(all(feature = "runtime", feature = "tokio"))]
mod masking;

#[cfg(all(test, feature = "runtime", feature = "tokio"))]
#[path = "tests/crypto.rs"]
mod tests;
