use super::*;
pub(super) fn encode(
    codec: &ShadowsocksDatagramCodec,
    target: &Address,
    port: u16,
    payload: &[u8],
    plain: &mut [u8],
    wire: &mut [u8],
) -> core::result::Result<usize, Error> {
    let length = address::encode(target, port, payload, plain)?;
    let cipher = codec.cipher;
    let salt = cipher.udp_salt_len();
    shared::fill_random(&mut wire[..salt])?;
    wire[salt..salt + length].copy_from_slice(&plain[..length]);
    if cipher.is_stream() {
        let mut state =
            shared::legacy::LegacyCipherState::new(cipher, &codec.password, &wire[..salt])?;
        state.encrypt(&mut wire[salt..salt + length]);
        return Ok(salt + length);
    }
    let key = shared::derive_udp_packet_key(cipher, &codec.password, &wire[..salt])?;
    let (_, encrypted) = wire.split_at_mut(salt);
    let (data, tag) = encrypted[..length + cipher.tag_len()].split_at_mut(length);
    shared::aead_udp_buffered(cipher, &key, &[0; 12], data, tag, true)?;
    Ok(salt + length + cipher.tag_len())
}
pub(super) fn decrypt(
    codec: &ShadowsocksDatagramCodec,
    wire: &[u8],
    plain: &mut [u8],
) -> core::result::Result<(usize, usize), Error> {
    let cipher = codec.cipher;
    let salt = cipher.udp_salt_len();
    let iv = wire
        .get(..salt)
        .ok_or(Error::Protocol("ss: udp datagram too short"))?;
    let data = &wire[salt..];
    plain[..data.len()].copy_from_slice(data);
    if cipher.is_stream() {
        shared::legacy::LegacyCipherState::new(cipher, &codec.password, iv)?
            .decrypt(&mut plain[..data.len()]);
        return Ok((data.len(), 0));
    }
    let length = data
        .len()
        .checked_sub(cipher.tag_len())
        .ok_or(Error::Protocol("ss: udp datagram too short"))?;
    let key = shared::derive_udp_packet_key(cipher, &codec.password, iv)?;
    let (data, tag) = plain[..data.len()].split_at_mut(length);
    shared::aead_udp_buffered(cipher, &key, &[0; 12], data, tag, false)?;
    Ok((length, 0))
}
