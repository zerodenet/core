//! Generic TLS 1.3 client implementation with custom ClientHello support.
//!
//! Extracted from the REALITY protocol's TLS 1.3 stack for use with
//! standard (non-REALITY) TLS outbound connections.
//!
//! Provides byte-level control over the ClientHello for uTLS-level
//! browser fingerprint matching.

#[cfg(feature = "runtime")]
pub mod aead;
#[cfg(feature = "runtime")]
pub mod buf_reader;
#[cfg(feature = "runtime")]
pub mod certificate;
#[cfg(feature = "runtime")]
pub mod cipher;
#[cfg(feature = "runtime")]
pub mod common;
pub mod fingerprint;
#[cfg(feature = "runtime")]
pub mod handshake;
#[cfg(feature = "runtime")]
pub mod hello;
#[cfg(feature = "runtime")]
pub mod keys;
#[cfg(feature = "runtime")]
pub mod messages;
#[cfg(feature = "runtime")]
pub mod post_handshake;
#[cfg(feature = "runtime")]
pub mod reader_writer;
#[cfg(feature = "runtime")]
pub mod reality_io_state;
#[cfg(feature = "runtime")]
pub mod record;
#[cfg(feature = "runtime")]
pub mod slide_buffer;
#[cfg(feature = "runtime")]
pub mod stream;
#[cfg(feature = "runtime")]
pub mod util;

#[cfg(feature = "validation")]
pub mod settings;
