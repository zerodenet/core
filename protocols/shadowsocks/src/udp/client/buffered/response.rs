use super::*;
pub(super) struct Decoded {
    pub target: Address,
    pub port: u16,
    pub payload: core::ops::Range<usize>,
    pub session: u64,
    pub packet: u64,
    pub client: Option<u64>,
}
pub(super) fn decode(
    codec: &ShadowsocksDatagramCodec,
    wire: &[u8],
    plain: &mut [u8],
    domain: &mut PacketBuffer,
) -> Result<Decoded> {
    let cipher = codec.cipher;
    let key = shared::parse_2022_key_chain(cipher, &codec.password)
        .map_err(protocol)?
        .user_key;
    let invalid = || protocol(Error::Protocol("ss: udp 2022 packet too short"));
    let length = if cipher.is_2022_chacha() {
        let nonce = wire.get(..24).ok_or_else(invalid)?;
        let body = &wire[24..];
        let length = body
            .len()
            .checked_sub(cipher.tag_len())
            .ok_or_else(invalid)?;
        plain[..body.len()].copy_from_slice(body);
        let (body, tag) = plain[..body.len()].split_at_mut(length);
        shared::aead_udp_buffered(cipher, &key, nonce, body, tag, false).map_err(protocol)?;
        length
    } else {
        let mut header = [0; 16];
        header.copy_from_slice(wire.get(..16).ok_or_else(invalid)?);
        shared::decrypt_aes_2022_header(cipher, &key, &mut header).map_err(protocol)?;
        let session_key =
            shared::derive_key_blake3(&key, &header[..8], cipher.key_len()).map_err(protocol)?;
        let length = wire
            .len()
            .checked_sub(cipher.tag_len())
            .filter(|n| *n >= 16)
            .ok_or_else(invalid)?;
        plain[..16].copy_from_slice(&header);
        plain[16..wire.len()].copy_from_slice(&wire[16..]);
        let (body, tag) = plain[16..wire.len()].split_at_mut(length - 16);
        shared::aead_udp_buffered(cipher, &session_key, &header[4..16], body, tag, false)
            .map_err(protocol)?;
        length
    };
    let data = &plain[..length];
    if data.len() < 27 {
        return Err(invalid());
    }
    let session = u64::from_be_bytes(data[..8].try_into().map_err(|_| invalid())?);
    let packet = u64::from_be_bytes(data[8..16].try_into().map_err(|_| invalid())?);
    let timestamp = u64::from_be_bytes(data[17..25].try_into().map_err(|_| invalid())?);
    shared::validate_2022_timestamp(timestamp).map_err(protocol)?;
    let (cursor, client) = match data[16] {
        0 => (25, None),
        1 => (
            33,
            Some(u64::from_be_bytes(
                data.get(25..33)
                    .ok_or_else(invalid)?
                    .try_into()
                    .map_err(|_| invalid())?,
            )),
        ),
        _ => {
            return Err(protocol(Error::Protocol(
                "ss: invalid udp 2022 socket type",
            )))
        }
    };
    let padding = data.get(cursor..cursor + 2).ok_or_else(invalid)?;
    let start = cursor + 2 + u16::from_be_bytes([padding[0], padding[1]]) as usize;
    let target = data.get(start..).ok_or_else(invalid)?;
    let (target, port, payload) = address::decode(target, domain)?;
    Ok(Decoded {
        target,
        port,
        payload: start + payload..length,
        session,
        packet,
        client,
    })
}
