// TLS 1.3 Message Construction
//
// Construct TLS 1.3 handshake messages for REALITY protocol

use crate::common::{
    HANDSHAKE_TYPE_CERTIFICATE, HANDSHAKE_TYPE_CERTIFICATE_VERIFY,
    HANDSHAKE_TYPE_ENCRYPTED_EXTENSIONS, HANDSHAKE_TYPE_FINISHED, HANDSHAKE_TYPE_SERVER_HELLO,
    VERSION_TLS_1_2_MAJOR, VERSION_TLS_1_2_MINOR,
};
use crate::fingerprint::ClientHelloProfile;
use std::io::Result;

/// Construct Finished message
///
/// # Arguments
/// * `verify_data` - HMAC of handshake transcript (32 bytes for SHA256)
pub fn construct_finished(verify_data: &[u8]) -> Result<Vec<u8>> {
    let mut finished = Vec::new();

    // Finished structure:
    // - handshake_type (1 byte) = 20
    // - length (3 bytes)
    // - verify_data (variable, 32 bytes for SHA256)

    finished.push(HANDSHAKE_TYPE_FINISHED);

    // Payload length (3 bytes)
    finished.extend_from_slice(&[
        ((verify_data.len() >> 16) & 0xff) as u8,
        ((verify_data.len() >> 8) & 0xff) as u8,
        (verify_data.len() & 0xff) as u8,
    ]);

    finished.extend_from_slice(verify_data);

    Ok(finished)
}

pub fn construct_server_hello(
    server_random: &[u8; 32],
    session_id: &[u8],
    cipher_suite: u16,
    server_public_key: &[u8; 32],
) -> Result<Vec<u8>> {
    let mut hello = Vec::with_capacity(128);
    hello.push(HANDSHAKE_TYPE_SERVER_HELLO);
    let length_offset = hello.len();
    hello.extend_from_slice(&[0u8; 3]);
    hello.extend_from_slice(&[VERSION_TLS_1_2_MAJOR, VERSION_TLS_1_2_MINOR]);
    hello.extend_from_slice(server_random);
    hello.push(session_id.len() as u8);
    hello.extend_from_slice(session_id);
    hello.extend_from_slice(&cipher_suite.to_be_bytes());
    hello.push(0x00);

    let extensions_offset = hello.len();
    hello.extend_from_slice(&[0u8; 2]);
    let mut extensions = Vec::new();

    extensions.extend_from_slice(&[0x00, 0x2b]);
    extensions.extend_from_slice(&[0x00, 0x02]);
    extensions.extend_from_slice(&[0x03, 0x04]);

    extensions.extend_from_slice(&[0x00, 0x33]);
    extensions.extend_from_slice(&(4 + server_public_key.len() as u16).to_be_bytes());
    extensions.extend_from_slice(&[0x00, 0x1d]);
    extensions.extend_from_slice(&(server_public_key.len() as u16).to_be_bytes());
    extensions.extend_from_slice(server_public_key);

    hello[extensions_offset..extensions_offset + 2]
        .copy_from_slice(&(extensions.len() as u16).to_be_bytes());
    hello.extend_from_slice(&extensions);

    let message_length = hello.len() - 4;
    hello[length_offset..length_offset + 3]
        .copy_from_slice(&(message_length as u32).to_be_bytes()[1..]);
    Ok(hello)
}

pub fn construct_encrypted_extensions(alpn: Option<&str>) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let mut extensions = Vec::new();
    if let Some(protocol) = alpn {
        extensions.extend_from_slice(&[0x00, 0x10]);
        let protocol = protocol.as_bytes();
        let protocols_list_len = 1 + protocol.len();
        extensions.extend_from_slice(&((2 + protocols_list_len) as u16).to_be_bytes());
        extensions.extend_from_slice(&(protocols_list_len as u16).to_be_bytes());
        extensions.push(protocol.len() as u8);
        extensions.extend_from_slice(protocol);
    }
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend_from_slice(&extensions);
    Ok(handshake_message(
        HANDSHAKE_TYPE_ENCRYPTED_EXTENSIONS,
        &body,
    ))
}

pub fn construct_certificate(cert_der: &[u8]) -> Result<Vec<u8>> {
    let mut body = Vec::with_capacity(cert_der.len() + 16);
    body.push(0x00);
    let list_len = 3 + cert_der.len() + 2;
    body.extend_from_slice(&(list_len as u32).to_be_bytes()[1..]);
    body.extend_from_slice(&(cert_der.len() as u32).to_be_bytes()[1..]);
    body.extend_from_slice(cert_der);
    body.extend_from_slice(&[0x00, 0x00]);
    Ok(handshake_message(HANDSHAKE_TYPE_CERTIFICATE, &body))
}

pub fn construct_certificate_verify(signature: &[u8]) -> Result<Vec<u8>> {
    let mut body = Vec::with_capacity(4 + signature.len());
    body.extend_from_slice(&[0x08, 0x07]);
    body.extend_from_slice(&(signature.len() as u16).to_be_bytes());
    body.extend_from_slice(signature);
    Ok(handshake_message(HANDSHAKE_TYPE_CERTIFICATE_VERIFY, &body))
}

fn handshake_message(message_type: u8, body: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(4 + body.len());
    message.push(message_type);
    message.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    message.extend_from_slice(body);
    message
}

/// Default ALPN protocols for REALITY client (matches browser fingerprints)
pub const DEFAULT_ALPN_PROTOCOLS: &[&str] = &["h2", "http/1.1"];

/// Construct TLS 1.3 ClientHello message
///
/// Returns handshake message bytes (without record header)
///
/// # Arguments
/// * `client_random` - 32 bytes client random
/// * `session_id` - 32 bytes session ID
/// * `client_public_key` - X25519 public key bytes
/// * `server_name` - SNI hostname
/// * `cipher_suites` - Cipher suite IDs to offer (e.g., &[0x1301, 0x1302, 0x1303])
/// * `alpn_protocols` - ALPN protocols to offer (e.g., &["h2", "http/1.1"])
pub fn construct_client_hello(
    client_random: &[u8; 32],
    session_id: &[u8; 32],
    client_public_key: &[u8],
    server_name: &str,
    cipher_suites: &[u16],
    alpn_protocols: &[&str],
) -> Result<Vec<u8>> {
    construct_client_hello_with_profile(
        client_random,
        session_id,
        client_public_key,
        server_name,
        cipher_suites,
        alpn_protocols,
        ClientHelloProfile::DEFAULT,
    )
}

/// Serialize a standalone ClientHello for inspection. Auxiliary key state is
/// discarded; live connections use `fingerprint::wire::build` and retain their
/// `fingerprint::key_share::KeyShares` until ServerHello has been processed.
pub fn construct_client_hello_with_profile(
    client_random: &[u8; 32],
    session_id: &[u8; 32],
    client_public_key: &[u8],
    server_name: &str,
    cipher_suites: &[u16],
    alpn_protocols: &[&str],
    profile: ClientHelloProfile,
) -> Result<Vec<u8>> {
    let mut keys = crate::fingerprint::key_share::KeyShares::for_profile(profile);
    crate::fingerprint::wire::build(
        client_random,
        session_id,
        server_name,
        cipher_suites,
        alpn_protocols,
        profile,
        |group| keys.offer(group, client_public_key),
    )
}

/// Write TLS record header
///
/// # Arguments
/// * `record_type` - TLS record type (0x16 for Handshake, 0x17 for ApplicationData)
/// * `length` - Length of record payload
pub fn write_record_header(record_type: u8, length: u16) -> Vec<u8> {
    let mut header = Vec::new();
    header.push(record_type);
    header.extend_from_slice(&[VERSION_TLS_1_2_MAJOR, VERSION_TLS_1_2_MINOR]); // Version: TLS 1.2
    header.extend_from_slice(&length.to_be_bytes());
    header
}
