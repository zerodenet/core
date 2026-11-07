use super::*;
pub(super) fn encode(
    codec: &ShadowsocksDatagramCodec,
    target: &Address,
    port: u16,
    payload: &[u8],
    session: (u64, u64),
    plain: &mut [u8],
    wire: &mut [u8],
) -> core::result::Result<usize, Error> {
    let cipher = codec.cipher;
    let keys = shared::parse_2022_key_chain(cipher, &codec.password)?;
    let padding = if payload.is_empty() {
        let mut seed = [0; 8];
        shared::fill_random(&mut seed)?;
        (u64::from_ne_bytes(seed) % 900) as usize
    } else {
        0
    };
    let mut header = [0; 16];
    header[..8].copy_from_slice(&session.0.to_be_bytes());
    header[8..].copy_from_slice(&session.1.to_be_bytes());
    let body_header = if cipher.is_2022_chacha() {
        plain[..16].copy_from_slice(&header);
        16
    } else {
        0
    };
    plain[body_header] = 0;
    plain[body_header + 1..body_header + 9]
        .copy_from_slice(&shared::now_unix_seconds().to_be_bytes());
    plain[body_header + 9..body_header + 11].copy_from_slice(&(padding as u16).to_be_bytes());
    let offset = body_header + 11;
    shared::fill_random(&mut plain[offset..offset + padding])?;
    let length =
        offset + padding + address::encode(target, port, payload, &mut plain[offset + padding..])?;
    let prefix;
    let key;
    let mut nonce = [0; 24];
    if cipher.is_2022_chacha() {
        prefix = 24;
        shared::fill_random(&mut nonce)?;
        wire[..24].copy_from_slice(&nonce);
        key = keys.user_key;
    } else {
        prefix = 16 + keys.identity_keys.len() * 16;
        for (index, identity) in keys.identity_keys.iter().enumerate() {
            let next = keys
                .identity_keys
                .get(index + 1)
                .map_or(keys.user_key.as_slice(), Vec::as_slice);
            let mut identity_header = [0; 16];
            identity_header.copy_from_slice(&blake3::hash(next).as_bytes()[..16]);
            for (byte, separate) in identity_header.iter_mut().zip(header) {
                *byte ^= separate;
            }
            shared::encrypt_aes_2022_header(cipher, identity, &mut identity_header)?;
            wire[16 + 16 * index..32 + 16 * index].copy_from_slice(&identity_header);
        }
        key = shared::derive_key_blake3(&keys.user_key, &header[..8], cipher.key_len())?;
        nonce[..12].copy_from_slice(&header[4..16]);
        let first = keys
            .identity_keys
            .first()
            .map_or(keys.user_key.as_slice(), Vec::as_slice);
        shared::encrypt_aes_2022_header(cipher, first, &mut header)?;
        wire[..16].copy_from_slice(&header);
    }
    wire[prefix..prefix + length].copy_from_slice(&plain[..length]);
    let (body, tag) = wire[prefix..prefix + length + cipher.tag_len()].split_at_mut(length);
    let nonce = if cipher.is_2022_chacha() {
        &nonce[..24]
    } else {
        &nonce[..12]
    };
    shared::aead_udp_buffered(cipher, &key, nonce, body, tag, true)?;
    Ok(prefix + length + cipher.tag_len())
}
