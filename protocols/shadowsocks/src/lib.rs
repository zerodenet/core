#![cfg_attr(not(feature = "crypto"), no_std)]
#![allow(async_fn_in_trait)]

extern crate alloc;

#[cfg(feature = "runtime")]
mod inbound;
mod metadata;
#[cfg(feature = "runtime")]
mod outbound;
#[cfg(feature = "runtime")]
pub mod shared;
#[cfg(feature = "runtime")]
mod stream;
#[cfg(feature = "runtime")]
pub mod transport;
#[cfg(feature = "runtime")]
pub mod udp;
#[cfg(feature = "validation")]
pub mod validation;

#[cfg(feature = "runtime")]
pub use inbound::ShadowsocksInbound;
#[cfg(feature = "runtime")]
pub use inbound::{
    inbound_profile_from_config_cipher_password, ShadowsocksAccept, ShadowsocksInboundProfile,
    ShadowsocksInboundProfileStore, ShadowsocksInboundTcpAcceptor, ShadowsocksInboundTcpState,
    ShadowsocksInboundUserRef, ShadowsocksUser,
};
pub use metadata::ShadowsocksProtocol;
#[cfg(feature = "runtime")]
pub use outbound::ShadowsocksOutbound;
#[cfg(feature = "runtime")]
pub use outbound::{
    tcp_connect_config_from_config, ShadowsocksOutboundSession, ShadowsocksTcpConnectConfig,
};
#[cfg(all(feature = "runtime", feature = "blake3"))]
pub use shared::derive_key_blake3;
#[cfg(feature = "runtime")]
pub use shared::{
    aead_decrypt, aead_encrypt, decrypt_tcp_chunk_length, decrypt_tcp_chunk_payload,
    derive_download_key, derive_key, derive_session_key, encrypt_tcp_chunk, read_tcp_chunk,
    write_tcp_chunk, TCP_CHUNK_SIZE_LEN,
};
#[cfg(feature = "runtime")]
pub use shared::{
    build_2022_request_fixed_header, build_2022_request_var_header,
    build_2022_response_fixed_header, parse_2022_request_fixed_header,
    parse_2022_request_var_header, parse_2022_response_fixed_header,
    ss_2022_response_header_plain_len, SS_2022_HEADER_TYPE_CLIENT_STREAM,
    SS_2022_HEADER_TYPE_SERVER_STREAM, SS_2022_MAX_PADDING_LENGTH,
    SS_2022_REQUEST_FIXED_HEADER_LEN, SS_2022_TIMESTAMP_WINDOW_SECS,
};
#[cfg(feature = "runtime")]
pub use shared::{
    build_target_data, decode_address, encode_address, parse_target_data, read_exact,
    ADDR_TYPE_DOMAIN, ADDR_TYPE_IPV4, ADDR_TYPE_IPV6,
};
#[cfg(feature = "runtime")]
pub use shared::{
    decrypt_tcp_2022_single_chunk, encrypt_tcp_2022_single_chunk, max_tcp_payload_len,
};
#[cfg(all(feature = "runtime", feature = "blake3"))]
pub use shared::{
    now_unix_seconds, random_2022_padding, validate_2022_timestamp, ReplaySaltPool, ReplayWindow,
};
#[cfg(feature = "runtime")]
pub use stream::ShadowsocksAeadStream;
#[cfg(feature = "validation")]
pub use validation::CipherKind;
