// Shadowsocks protocol constants and helpers.
//
// SIP003 AEAD ciphers: aes-128-gcm, aes-256-gcm, chacha20-ietf-poly1305.

use alloc::string::String;
#[cfg(feature = "blake3")]
use alloc::vec;
use alloc::vec::Vec;

use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

#[cfg(feature = "blake3")]
use crate::validation::decode_blake3_master_key;
pub use crate::CipherKind;

#[path = "shared/extra_aead.rs"]
mod extra_aead;
mod identity;
pub mod legacy;
mod target;
pub(crate) use identity::cache_identity;
pub(crate) use target::complete_tcp_target;

pub const ADDR_TYPE_IPV4: u8 = 0x01;
pub const ADDR_TYPE_DOMAIN: u8 = 0x03;
pub const ADDR_TYPE_IPV6: u8 = 0x04;
pub const TCP_CHUNK_SIZE_LEN: usize = 2;
pub const MAX_TCP_PAYLOAD_SIZE: usize = 0x3fff;
#[cfg(feature = "blake3")]
const SS_2022_IDENTITY_SUBKEY_CONTEXT: &str = "shadowsocks 2022 identity subkey";

#[path = "udp/wire.rs"]
mod udp_wire;
pub(crate) use udp_wire::{decode_udp_datagram_2022, encode_udp_datagram_2022};
#[cfg(feature = "blake3")]
pub(crate) use udp_wire::{
    decode_udp_datagram_2022_session, decode_udp_wire_2022, encode_udp_request_with_session,
    encode_udp_response_2022,
};

#[cfg(feature = "blake3")]
#[path = "shared/replay.rs"]
mod replay;
#[cfg(all(feature = "crypto", feature = "blake3"))]
pub use replay::ReplaySaltPool;

/// Per-session UDP replay protection for Shadowsocks 2022 (SIP022 3.2.4).
///
#[cfg(feature = "blake3")]
#[path = "shared/window.rs"]
mod window;
#[cfg(feature = "blake3")]
pub use window::ReplayWindow;

#[path = "shared/chacha8.rs"]
mod chacha8;

#[path = "shared/legacy_replay.rs"]
pub(crate) mod legacy_replay;

mod address;
mod aead;
#[cfg(feature = "blake3")]
mod block;
#[cfg(feature = "blake3")]
mod eih;
mod headers;
mod keys;
mod random;
mod tcp;
pub use address::*;
pub use aead::{aead_decrypt, aead_encrypt};
pub(crate) use aead::{aead_decrypt_udp, aead_encrypt_udp};
#[cfg(feature = "blake3")]
use block::{decrypt_aes_2022_header, encrypt_aes_2022_header};
#[cfg(feature = "blake3")]
use eih::encode_udp_2022_identity_headers;
#[cfg(feature = "blake3")]
pub(crate) use eih::{
    decode_udp_datagram_2022_eih_session, decrypt_tcp_2022_identity_header,
    encode_tcp_2022_identity_headers, identify_udp_2022_user, identity_hash_2022,
    parse_2022_key_chain,
};
pub use headers::*;
#[cfg(feature = "blake3")]
pub use keys::derive_key_blake3;
pub(crate) use keys::derive_udp_packet_key;
use keys::evp_bytes_to_key;
pub use keys::{derive_download_key, derive_key, derive_session_key};
pub(crate) use random::fill_random;
pub use random::now_unix_seconds;
#[cfg(feature = "blake3")]
pub(crate) use random::random_u64;
pub use tcp::*;
