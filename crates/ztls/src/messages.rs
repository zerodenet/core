// TLS 1.3 Message Construction
//
// Construct TLS 1.3 handshake messages for REALITY protocol

use crate::common::{
    calculate_client_hello_padding, HANDSHAKE_TYPE_CERTIFICATE, HANDSHAKE_TYPE_CERTIFICATE_VERIFY,
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

pub fn construct_client_hello_with_profile(
    client_random: &[u8; 32],
    session_id: &[u8; 32],
    client_public_key: &[u8],
    server_name: &str,
    cipher_suites: &[u16],
    alpn_protocols: &[&str],
    profile: ClientHelloProfile,
) -> Result<Vec<u8>> {
    let mut hello = Vec::with_capacity(512);

    // Handshake message type: ClientHello (0x01)
    hello.push(0x01);

    // Placeholder for handshake message length (3 bytes)
    let length_offset = hello.len();
    hello.extend_from_slice(&[0u8; 3]);

    // TLS version: 3.3 (TLS 1.2 for compatibility)
    hello.extend_from_slice(&[VERSION_TLS_1_2_MAJOR, VERSION_TLS_1_2_MINOR]);

    // Client random (32 bytes)
    hello.extend_from_slice(client_random);

    // Session ID length (1 byte) + Session ID (32 bytes)
    hello.push(32);
    hello.extend_from_slice(session_id);

    // Cipher suites
    let cipher_suites_len = (cipher_suites.len() * 2) as u16;
    hello.extend_from_slice(&cipher_suites_len.to_be_bytes());
    for &suite in cipher_suites {
        hello.extend_from_slice(&suite.to_be_bytes());
    }

    // Compression methods (1 method: null)
    hello.extend_from_slice(&[0x01, 0x00]);

    // Extensions
    let extensions_offset = hello.len();
    hello.extend_from_slice(&[0u8; 2]); // Placeholder for extensions length

    let mut extensions = Vec::new();

    // server_name (0)
    {
        let server_name_bytes = server_name.as_bytes();
        let server_name_len = server_name_bytes.len();
        extensions.extend_from_slice(&[0x00, 0x00]);
        let ext_len = 5 + server_name_len;
        extensions.extend_from_slice(&(ext_len as u16).to_be_bytes());
        extensions.extend_from_slice(&((server_name_len + 3) as u16).to_be_bytes());
        extensions.push(0x00);
        extensions.extend_from_slice(&(server_name_len as u16).to_be_bytes());
        extensions.extend_from_slice(server_name_bytes);
    }

    // supported_versions (43)
    {
        extensions.extend_from_slice(&[0x00, 0x2b]);
        extensions.extend_from_slice(&[0x00, 0x03]);
        extensions.push(0x02);
        extensions.extend_from_slice(&[0x03, 0x04]);
    }

    // extended_master_secret (23)
    {
        extensions.extend_from_slice(&[0x00, 0x17]);
        extensions.extend_from_slice(&[0x00, 0x00]);
    }

    // ec_point_formats (11)
    {
        extensions.extend_from_slice(&[0x00, 0x0b]);
        extensions.extend_from_slice(&[0x00, 0x02]);
        extensions.push(0x01);
        extensions.push(0x00);
    }

    // supported_groups (10), ordered by the selected ClientHello profile.
    {
        let groups = profile.supported_groups();
        let groups_len = groups.len() * 2;
        extensions.extend_from_slice(&[0x00, 0x0a]);
        extensions.extend_from_slice(&((groups_len + 2) as u16).to_be_bytes());
        extensions.extend_from_slice(&(groups_len as u16).to_be_bytes());
        for group in groups {
            extensions.extend_from_slice(&group.to_be_bytes());
        }
    }

    // key_share (51)
    {
        extensions.extend_from_slice(&[0x00, 0x33]);
        let key_share_len = 2 + 4 + client_public_key.len();
        extensions.extend_from_slice(&(key_share_len as u16).to_be_bytes());
        let key_share_list_len = 4 + client_public_key.len();
        extensions.extend_from_slice(&(key_share_list_len as u16).to_be_bytes());
        extensions.extend_from_slice(&profile.key_share_group().to_be_bytes());
        extensions.extend_from_slice(&(client_public_key.len() as u16).to_be_bytes());
        extensions.extend_from_slice(client_public_key);
    }

    // signature_algorithms (13)
    {
        extensions.extend_from_slice(&[0x00, 0x0d]);
        const ALGS: &[u16] = &[
            0x0403, 0x0804, 0x0807, 0x0401, 0x0503, 0x0805, 0x0501, 0x0806, 0x0601,
        ];
        let alen = (ALGS.len() * 2) as u16;
        extensions.extend_from_slice(&(2 + alen).to_be_bytes());
        extensions.extend_from_slice(&alen.to_be_bytes());
        for &a in ALGS {
            extensions.extend_from_slice(&a.to_be_bytes());
        }
    }

    // supported_signature_algorithms_cert (50)
    {
        extensions.extend_from_slice(&[0x00, 0x32]);
        const CERT_ALGS: &[u16] = &[0x0403, 0x0804, 0x0807, 0x0401, 0x0503, 0x0805];
        let alen = (CERT_ALGS.len() * 2) as u16;
        extensions.extend_from_slice(&(2 + alen).to_be_bytes());
        extensions.extend_from_slice(&alen.to_be_bytes());
        for &a in CERT_ALGS {
            extensions.extend_from_slice(&a.to_be_bytes());
        }
    }

    // ALPN (16)
    if !alpn_protocols.is_empty() {
        extensions.extend_from_slice(&[0x00, 0x10]);
        let protocols_list_len: usize = alpn_protocols.iter().map(|p| 1 + p.len()).sum();
        let ext_len = 2 + protocols_list_len;
        extensions.extend_from_slice(&(ext_len as u16).to_be_bytes());
        extensions.extend_from_slice(&(protocols_list_len as u16).to_be_bytes());
        for protocol in alpn_protocols {
            extensions.push(protocol.len() as u8);
            extensions.extend_from_slice(protocol.as_bytes());
        }
    }

    // compress_certificate (27) — RFC 8879
    //
    // extension_data = CertificateCompressionAlgorithms, which is a
    // vector `<2..2^8-2>`: a 1-byte **byte-length** prefix (not a count)
    // followed by the 2-byte algorithm ids.
    {
        extensions.extend_from_slice(&[0x00, 0x1b]);
        extensions.extend_from_slice(&[0x00, 0x05]); // 1 (prefix) + 2 algos * 2 bytes
        extensions.push(0x04); // byte-length of the algorithm list: 2 * 2
        extensions.extend_from_slice(&[0x00, 0x02]); // brotli
        extensions.extend_from_slice(&[0x00, 0x03]); // zstd
    }

    // encrypt_then_mac (22)
    {
        extensions.extend_from_slice(&[0x00, 0x16]);
        extensions.extend_from_slice(&[0x00, 0x00]);
    }

    // psk_key_exchange_modes (45)
    {
        extensions.extend_from_slice(&[0x00, 0x2d]);
        extensions.extend_from_slice(&[0x00, 0x02]);
        extensions.push(0x01);
        extensions.push(0x01);
    }

    extensions = order_extensions(extensions, profile)?;

    // Temporary: write extensions length without padding, compute size,
    // then add RFC 7685 padding extension to round to 512-byte boundary.
    let extensions_len_before_padding = extensions.len();
    hello[extensions_offset..extensions_offset + 2]
        .copy_from_slice(&(extensions_len_before_padding as u16).to_be_bytes());

    // Total message size without padding = hello header + extensions
    let current_total = hello.len() + extensions_len_before_padding;
    let padding_data_len = calculate_client_hello_padding(current_total);

    if padding_data_len > 0 {
        // padding extension: type (2) + length (2) + data (padding_data_len)
        extensions.extend_from_slice(&[0x00, 0x15]);
        extensions.extend_from_slice(&(padding_data_len as u16).to_be_bytes());
        extensions.extend_from_slice(&vec![0u8; padding_data_len]);

        // Update extensions length in hello
        hello[extensions_offset..extensions_offset + 2]
            .copy_from_slice(&(extensions.len() as u16).to_be_bytes());
    }

    // Append extensions
    hello.extend_from_slice(&extensions);

    // Write handshake message length
    let message_length = hello.len() - 4; // Exclude type (1) and length (3)
    hello[length_offset..length_offset + 3]
        .copy_from_slice(&(message_length as u32).to_be_bytes()[1..]);

    Ok(hello)
}

fn order_extensions(encoded: Vec<u8>, profile: ClientHelloProfile) -> Result<Vec<u8>> {
    let mut extensions = Vec::new();
    let mut offset = 0;
    while offset < encoded.len() {
        if encoded.len() - offset < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "truncated ClientHello extension header",
            ));
        }
        let extension_type = u16::from_be_bytes([encoded[offset], encoded[offset + 1]]);
        let body_len = u16::from_be_bytes([encoded[offset + 2], encoded[offset + 3]]) as usize;
        let end = offset + 4 + body_len;
        if end > encoded.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "truncated ClientHello extension body",
            ));
        }
        extensions.push((extension_type, encoded[offset..end].to_vec()));
        offset = end;
    }

    let mut ordered = Vec::with_capacity(encoded.len());
    for expected in profile.extension_order() {
        if let Some(index) = extensions
            .iter()
            .position(|(extension_type, _)| extension_type == expected)
        {
            ordered.extend_from_slice(&extensions.remove(index).1);
        }
    }
    for (_, extension) in extensions {
        ordered.extend_from_slice(&extension);
    }
    Ok(ordered)
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
