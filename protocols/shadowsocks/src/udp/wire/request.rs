use super::*;
#[cfg(feature = "crypto")]
pub(crate) fn encode_udp_datagram_2022(
    cipher: CipherKind,
    password: &[u8],
    target: &Address,
    port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    #[cfg(feature = "blake3")]
    {
        encode_udp_request_with_session(cipher, password, target, port, payload, (random_u64()?, 0))
    }
    #[cfg(not(feature = "blake3"))]
    {
        let _ = (cipher, password, target, port, payload);
        Err(Error::Protocol("ss: 2022 requires blake3"))
    }
}

#[cfg(feature = "blake3")]
pub(crate) fn encode_udp_request_with_session(
    cipher: CipherKind,
    password: &[u8],
    target: &Address,
    port: u16,
    payload: &[u8],
    session: (u64, u64),
) -> Result<Vec<u8>, Error> {
    #[cfg(feature = "blake3")]
    {
        let key_chain = parse_2022_key_chain(cipher, password)?;
        let target_data = build_target_data(target, port, payload)?;
        let padding = packet_padding(payload)?;
        let (session_id, packet_id) = session;

        match cipher {
            CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm => {
                let mut header = [0_u8; 16];
                header[..8].copy_from_slice(&session_id.to_be_bytes());
                header[8..].copy_from_slice(&packet_id.to_be_bytes());
                let identity_headers = encode_udp_2022_identity_headers(
                    cipher,
                    &key_chain.identity_keys,
                    &key_chain.user_key,
                    &header,
                )?;
                let mut body = Vec::with_capacity(11 + target_data.len());
                body.push(0);
                body.extend_from_slice(&now_unix_seconds().to_be_bytes());
                body.extend_from_slice(&(padding.len() as u16).to_be_bytes());
                body.extend_from_slice(&padding);
                body.extend_from_slice(&target_data);
                let session_key =
                    derive_key_blake3(&key_chain.user_key, &header[..8], cipher.key_len())?;
                let encrypted = aead_encrypt_udp(cipher, &session_key, &header[4..16], &body)?;
                let header_key = key_chain
                    .identity_keys
                    .first()
                    .map_or(key_chain.user_key.as_slice(), Vec::as_slice);
                encrypt_aes_2022_header(cipher, header_key, &mut header)?;

                let mut out =
                    Vec::with_capacity(header.len() + identity_headers.len() + encrypted.len());
                out.extend_from_slice(&header);
                out.extend_from_slice(&identity_headers);
                out.extend_from_slice(&encrypted);
                Ok(out)
            }
            CipherKind::Blake3Chacha20Poly1305 | CipherKind::Blake3Chacha8Poly1305 => {
                let mut nonce = [0_u8; 24];
                fill_random(&mut nonce)?;
                let mut body = Vec::with_capacity(27 + target_data.len());
                body.extend_from_slice(&session_id.to_be_bytes());
                body.extend_from_slice(&packet_id.to_be_bytes());
                body.push(0);
                body.extend_from_slice(&now_unix_seconds().to_be_bytes());
                body.extend_from_slice(&(padding.len() as u16).to_be_bytes());
                body.extend_from_slice(&padding);
                body.extend_from_slice(&target_data);
                let encrypted = aead_encrypt_udp(cipher, &key_chain.user_key, &nonce, &body)?;
                let mut out = Vec::with_capacity(24 + encrypted.len());
                out.extend_from_slice(&nonce);
                out.extend_from_slice(&encrypted);
                Ok(out)
            }
            _ => Err(Error::Protocol("ss: cipher is not a 2022 method")),
        }
    }

    #[cfg(not(feature = "blake3"))]
    {
        let _ = (cipher, password, target, port, payload);
        Err(Error::Protocol(
            "ss: 2022 udp datagram requires `blake3` feature",
        ))
    }
}
