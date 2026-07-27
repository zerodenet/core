//! Generic TLS 1.3 client implementation with custom ClientHello support.
//!
//! Extracted from the REALITY protocol's TLS 1.3 stack for use with
//! standard (non-REALITY) TLS outbound connections.
//!
//! Provides byte-level control over the ClientHello for uTLS-level
//! browser fingerprint matching.

pub mod aead;
pub mod buf_reader;
pub mod cipher;
pub mod common;
pub mod fingerprint;
pub mod handshake;
pub mod keys;
pub mod messages;
pub mod reader_writer;
pub mod reality_io_state;
pub mod record;
pub mod slide_buffer;
pub mod stream;
pub mod util;
