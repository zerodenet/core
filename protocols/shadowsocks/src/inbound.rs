// Shadowsocks inbound protocol.

#[cfg(feature = "crypto")]
use alloc::string::String;
#[cfg(feature = "crypto")]
use alloc::sync::Arc;
#[cfg(feature = "crypto")]
use alloc::vec::Vec;
#[cfg(feature = "crypto")]
use std::{
    collections::HashMap,
    sync::{Mutex, RwLock},
};
use zero_core::ProtocolType;
#[cfg(feature = "crypto")]
use zero_core::{Error, Network, Session, SessionAuth};

#[cfg(feature = "crypto")]
use crate::udp::{
    ShadowsocksInboundUdpCodec, ShadowsocksInboundUdpRelay, ShadowsocksInboundUdpResponder,
    ShadowsocksInboundUdpSession,
};

/// Shadowsocks inbound handler.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShadowsocksInbound;

/// Result of accepting a Shadowsocks TCP connection.
#[cfg(feature = "crypto")]
pub struct ShadowsocksAccept {
    pub legacy: Option<crate::shared::legacy::LegacyCipherState>,
    pub session: Session,
    /// Remaining plaintext payload after the target address in the first chunk.
    pub remaining_payload: Vec<u8>,
    /// Derived session key for subsequent AEAD operations.
    pub session_key: Vec<u8>,
    /// Cipher kind for subsequent chunks.
    pub cipher: super::shared::CipherKind,
    /// Next nonce for decrypting client-to-server chunks after the first request chunk.
    pub next_upload_nonce: u128,
    /// For 2022 edition: the client request salt, echoed back in the server
    /// response fixed header. Empty for legacy AEAD.
    pub request_salt: Vec<u8>,
}

mod accept;
mod identity;
mod legacy;
mod model;
mod profile;
mod request;
mod store;
mod tcp;
pub(crate) use model::ShadowsocksAuthorizedUsers;
pub use model::{ShadowsocksInboundProfile, ShadowsocksInboundUserRef, ShadowsocksUser};
pub use store::{inbound_profile_from_config_cipher_password, ShadowsocksInboundProfileStore};
pub use tcp::{ShadowsocksInboundTcpAcceptor, ShadowsocksInboundTcpState};

#[cfg(all(feature = "crypto", feature = "blake3"))]
/// SIP022 3.1.3 detection-prevention drain cap (bytes). Bounds the drain so a
/// malicious peer cannot hold the connection open indefinitely; typical active
/// probes send far fewer bytes than this.
const SS_2022_DRAIN_CAP: usize = 1 << 20; // 1 MiB
#[cfg(all(feature = "crypto", feature = "blake3"))]
/// Hard wall on how long a failed-handshake drain may block. A peer that sends
/// a short probe and then holds the connection open would otherwise pin a task
/// until the byte cap is reached; this keeps the anti-probe drain bounded.
const SS_2022_DRAIN_TIMEOUT: core::time::Duration = core::time::Duration::from_secs(2);

/// Drain up to `cap` bytes (or `timeout`, or EOF) from `stream`, discarding
/// them. Used after a failed 2022 handshake so closing the connection sends FIN
/// (empty receive buffer) instead of RST, hiding how many bytes the server
/// consumed. Bounded by both a byte cap and a wall-clock timeout so a peer
/// cannot pin the task by keeping the connection open after a short probe.
#[cfg(all(feature = "crypto", feature = "blake3"))]
async fn drain_stream<S: zero_traits::AsyncSocket>(stream: &mut S, cap: usize) {
    let mut buf = [0u8; 4096];
    let mut total = 0usize;
    let deadline = tokio::time::Instant::now() + SS_2022_DRAIN_TIMEOUT;
    while total < cap {
        match tokio::time::timeout_at(deadline, stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => total += n,
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }
}
