use super::*;
#[cfg(feature = "blake3")]
fn packet_padding(payload: &[u8]) -> Result<Vec<u8>, Error> {
    if !payload.is_empty() {
        return Ok(Vec::new());
    }
    let mut seed = [0; 8];
    fill_random(&mut seed)?;
    let mut padding = vec![0; (u64::from_ne_bytes(seed) % 900) as usize];
    fill_random(&mut padding)?;
    Ok(padding)
}

#[path = "wire/request.rs"]
mod request;
#[cfg(feature = "blake3")]
#[path = "wire/response.rs"]
mod response;
pub(crate) use request::encode_udp_datagram_2022;
#[cfg(feature = "blake3")]
pub(crate) use request::encode_udp_request_with_session;
#[cfg(feature = "blake3")]
pub(crate) use response::encode_udp_response_2022;
#[cfg(feature = "crypto")]
pub(crate) fn decode_udp_datagram_2022(
    cipher: CipherKind,
    password: &[u8],
    datagram: &[u8],
) -> Result<(Address, u16, Vec<u8>), Error> {
    #[cfg(feature = "blake3")]
    {
        let p = decode_udp_wire_2022(cipher, password, datagram)?;
        Ok((p.target, p.port, p.payload))
    }
    #[cfg(not(feature = "blake3"))]
    {
        let _ = (cipher, password, datagram);
        Err(Error::Protocol("ss: 2022 requires blake3"))
    }
}

#[cfg(feature = "blake3")]
pub(crate) fn decode_udp_wire_2022(
    cipher: CipherKind,
    password: &[u8],
    datagram: &[u8],
) -> Result<Packet, Error> {
    #[cfg(feature = "blake3")]
    {
        let master_key = parse_2022_key_chain(cipher, password)?.user_key;

        let plain = match cipher {
            CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm => {
                if datagram.len() < 16 + cipher.tag_len() {
                    return Err(Error::Protocol("ss: udp datagram too short"));
                }
                let mut header = [0u8; 16];
                header.copy_from_slice(&datagram[..16]);
                decrypt_aes_2022_header(cipher, &master_key, &mut header)?;
                let session_key = derive_key_blake3(&master_key, &header[..8], cipher.key_len())?;
                let message =
                    aead_decrypt_udp(cipher, &session_key, &header[4..16], &datagram[16..])?;
                let mut plain = Vec::with_capacity(header.len() + message.len());
                plain.extend_from_slice(&header);
                plain.extend_from_slice(&message);
                plain
            }
            CipherKind::Blake3Chacha20Poly1305 | CipherKind::Blake3Chacha8Poly1305 => {
                if datagram.len() < 24 + cipher.tag_len() {
                    return Err(Error::Protocol("ss: udp datagram too short"));
                }
                aead_decrypt_udp(cipher, &master_key, &datagram[..24], &datagram[24..])?
            }
            _ => return Err(Error::Protocol("ss: cipher is not a 2022 method")),
        };

        parse_udp_2022_plain(&plain)
    }

    #[cfg(not(feature = "blake3"))]
    {
        let _ = (cipher, password, datagram);
        Err(Error::Protocol(
            "ss: 2022 udp datagram requires `blake3` feature",
        ))
    }
}

/// Decode a 2022 UDP datagram, also returning the separate-header session id.
///
/// A server uses this to recover the client session id from an incoming
/// client packet (type 0) so it can echo it in server-to-client responses.
#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn decode_udp_datagram_2022_session(
    cipher: CipherKind,
    password: &[u8],
    datagram: &[u8],
) -> Result<(Address, u16, Vec<u8>, u64, u64), Error> {
    #[cfg(feature = "blake3")]
    {
        decode_udp_wire_2022(cipher, password, datagram)?.into_request()
    }
    #[cfg(not(feature = "blake3"))]
    {
        let _ = (cipher, password, datagram);
        Err(Error::Protocol("ss: 2022 requires blake3"))
    }
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(super) fn parse_udp_2022_plain(plain: &[u8]) -> Result<Packet, Error> {
    if plain.len() < 8 + 8 + 1 + 8 + 2 {
        return Err(Error::Protocol("ss: udp 2022 packet too short"));
    }

    // The separate-header session id / packet id occupy the first 16 bytes (for
    // AES they are the separate-header fields; for ChaCha20 they are in body).
    let session_id = u64::from_be_bytes([
        plain[0], plain[1], plain[2], plain[3], plain[4], plain[5], plain[6], plain[7],
    ]);
    let packet_id = u64::from_be_bytes([
        plain[8], plain[9], plain[10], plain[11], plain[12], plain[13], plain[14], plain[15],
    ]);

    let socket_type = plain[16];
    let timestamp = u64::from_be_bytes([
        plain[17], plain[18], plain[19], plain[20], plain[21], plain[22], plain[23], plain[24],
    ]);
    validate_2022_timestamp(timestamp)?;
    let mut cursor = match socket_type {
        0 => 17 + 8,
        1 => 17 + 8 + 8,
        _ => return Err(Error::Protocol("ss: invalid udp 2022 socket type")),
    };

    if plain.len() < cursor + 2 {
        return Err(Error::Protocol("ss: udp 2022 packet too short"));
    }
    let padding_len = u16::from_be_bytes([plain[cursor], plain[cursor + 1]]) as usize;
    cursor += 2;
    if plain.len() < cursor + padding_len {
        return Err(Error::Protocol("ss: invalid udp 2022 padding length"));
    }
    cursor += padding_len;

    let (target, port, payload_offset) = parse_target_data(&plain[cursor..])?;
    Ok(Packet {
        target,
        port,
        payload: plain[cursor + payload_offset..].to_vec(),
        session_id,
        packet_id,
        client_session_id: if socket_type == 1 {
            Some(u64::from_be_bytes(plain[25..33].try_into().unwrap()))
        } else {
            None
        },
    })
}

#[cfg(feature = "blake3")]
pub(crate) struct Packet {
    pub target: Address,
    pub port: u16,
    pub payload: Vec<u8>,
    pub session_id: u64,
    pub packet_id: u64,
    pub client_session_id: Option<u64>,
}
#[cfg(feature = "blake3")]
impl Packet {
    pub(super) fn into_request(self) -> Result<(Address, u16, Vec<u8>, u64, u64), Error> {
        if self.client_session_id.is_some() {
            return Err(Error::Protocol("ss: response on inbound UDP"));
        }
        Ok((
            self.target,
            self.port,
            self.payload,
            self.session_id,
            self.packet_id,
        ))
    }
}
