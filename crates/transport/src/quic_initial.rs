//! Bounded, passive QUIC Initial decryption for traffic classification.
//!
//! Initial keys are public by design. This module only reconstructs the client
//! Initial CRYPTO stream; it does not terminate QUIC or retain application data.

mod crypto;
mod frame;
mod packet;

use std::collections::HashMap;

use frame::CryptoReassembler;
use packet::decrypt_client_initial;

const MAX_CONNECTION_IDS: usize = 8;

/// Result of observing one UDP datagram for a possible QUIC ClientHello.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuicInitialOutcome {
    /// The datagram is not a supported QUIC client Initial.
    NotInitial,
    /// More Initial CRYPTO data is required.
    Pending,
    /// A complete TLS ClientHello handshake message, including its 4-byte
    /// handshake header, has been reconstructed.
    ClientHello(Vec<u8>),
    /// The datagram looked like an Initial but was malformed or could not be
    /// authenticated. Callers must fall back without guessing a hostname.
    Rejected,
}

/// Per-UDP-flow QUIC Initial state.
#[derive(Default)]
pub struct QuicInitialSniffer {
    crypto: CryptoReassembler,
    largest_packet_numbers: HashMap<Vec<u8>, u64>,
}

impl QuicInitialSniffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe all coalesced packets in one UDP datagram.
    pub fn ingest(&mut self, datagram: &[u8]) -> QuicInitialOutcome {
        if !looks_like_client_initial(datagram) {
            return QuicInitialOutcome::NotInitial;
        }

        let decrypted = match decrypt_client_initial(datagram, &mut self.largest_packet_numbers) {
            Ok(Some(decrypted)) => decrypted,
            Ok(None) => return QuicInitialOutcome::Pending,
            Err(()) => return QuicInitialOutcome::Rejected,
        };
        if self.largest_packet_numbers.len() > MAX_CONNECTION_IDS {
            return QuicInitialOutcome::Rejected;
        }
        for plaintext in decrypted {
            if frame::collect_crypto_frames(&plaintext, &mut self.crypto).is_err() {
                return QuicInitialOutcome::Rejected;
            }
        }
        match self.crypto.client_hello() {
            Ok(Some(client_hello)) => QuicInitialOutcome::ClientHello(client_hello),
            Ok(None) => QuicInitialOutcome::Pending,
            Err(()) => QuicInitialOutcome::Rejected,
        }
    }
}

/// Cheap classification used before allocating per-flow state.
pub fn looks_like_client_initial(datagram: &[u8]) -> bool {
    let Some((&first, remainder)) = datagram.split_first() else {
        return false;
    };
    if first & 0xc0 != 0xc0 || remainder.len() < 4 {
        return false;
    }
    let version = u32::from_be_bytes([remainder[0], remainder[1], remainder[2], remainder[3]]);
    match version {
        packet::QUIC_V1 => (first >> 4) & 0x03 == 0,
        packet::QUIC_V2 => (first >> 4) & 0x03 == 1,
        _ => false,
    }
}

#[cfg(test)]
mod tests;
