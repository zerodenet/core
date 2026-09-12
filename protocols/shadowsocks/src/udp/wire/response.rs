use super::*;
/// Encode a Shadowsocks 2022 UDP **server-to-client** response datagram
/// (SIP022 3.2.3, socket type 1). The caller supplies its stable server session
/// id and checked packet counter for the separate header (or ChaCha20 body) and echoes `client_session_id` in the body so the client
/// can map the response to its session.
#[cfg(feature = "crypto")]
pub(crate) fn encode_udp_response_2022(
    cipher: CipherKind,
    password: &[u8],
    client_session_id: u64,
    target: &Address,
    port: u16,
    payload: &[u8],
    session: (u64, u64),
) -> Result<Vec<u8>, Error> {
    #[cfg(feature = "blake3")]
    {
        let key_chain = parse_2022_key_chain(cipher, password)?;
        let master_key = key_chain.user_key;

        let target_data = build_target_data(target, port, payload)?;
        let padding = packet_padding(payload)?;
        let mut packet = Vec::with_capacity(64 + target_data.len() + cipher.tag_len());
        let (server_session_id, server_packet_id) = session;

        if cipher.is_2022_chacha() {
            let mut nonce = [0u8; 24];
            fill_random(&mut nonce)?;
            packet.extend_from_slice(&nonce);
        }

        packet.extend_from_slice(&server_session_id.to_be_bytes());
        packet.extend_from_slice(&server_packet_id.to_be_bytes());
        packet.push(SS_2022_HEADER_TYPE_SERVER_PACKET);
        packet.extend_from_slice(&now_unix_seconds().to_be_bytes());
        packet.extend_from_slice(&client_session_id.to_be_bytes());
        packet.extend_from_slice(&(padding.len() as u16).to_be_bytes());
        packet.extend_from_slice(&padding);
        packet.extend_from_slice(&target_data);

        match cipher {
            CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm => {
                let mut header = [0u8; 16];
                header.copy_from_slice(&packet[..16]);
                let session_key = derive_key_blake3(&master_key, &header[..8], cipher.key_len())?;
                let encrypted =
                    aead_encrypt_udp(cipher, &session_key, &header[4..16], &packet[16..])?;
                encrypt_aes_2022_header(cipher, &master_key, &mut header)?;

                let mut out = Vec::with_capacity(header.len() + encrypted.len());
                out.extend_from_slice(&header);
                out.extend_from_slice(&encrypted);
                Ok(out)
            }
            CipherKind::Blake3Chacha20Poly1305 | CipherKind::Blake3Chacha8Poly1305 => {
                let encrypted =
                    aead_encrypt_udp(cipher, &master_key, &packet[..24], &packet[24..])?;
                let mut out = Vec::with_capacity(24 + encrypted.len());
                out.extend_from_slice(&packet[..24]);
                out.extend_from_slice(&encrypted);
                Ok(out)
            }
            _ => Err(Error::Protocol("ss: cipher is not a 2022 method")),
        }
    }

    #[cfg(not(feature = "blake3"))]
    {
        let _ = (cipher, password, client_session_id, target, port, payload);
        Err(Error::Protocol(
            "ss: 2022 udp response requires `blake3` feature",
        ))
    }
}
