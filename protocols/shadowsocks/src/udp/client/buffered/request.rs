use super::*;
pub(super) fn encode(
    keys: &keys::Chain<'_>,
    target: &Address,
    port: u16,
    payload: &[u8],
    session: (u64, u64),
    plain: &mut [u8],
    wire: &mut [u8],
) -> core::result::Result<usize, Error> {
    let cipher = keys.cipher();
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
        key = keys.user;
    } else {
        prefix = 16 + keys.identity_count() * 16;
        let mut identities = keys.identities();
        let mut current = identities.next().transpose()?;
        let first = current.unwrap_or(keys.user);
        let mut index = 0;
        while let Some(identity) = current {
            let next = identities.next().transpose()?;
            let next_key = next.as_ref().unwrap_or(&keys.user);
            let mut identity_header = [0; 16];
            identity_header.copy_from_slice(&blake3::hash(next_key.as_ref()).as_bytes()[..16]);
            for (byte, separate) in identity_header.iter_mut().zip(header) {
                *byte ^= separate;
            }
            shared::encrypt_aes_2022_header(cipher, identity.as_ref(), &mut identity_header)?;
            wire[16 + 16 * index..32 + 16 * index].copy_from_slice(&identity_header);
            current = next;
            index += 1;
        }
        key = keys::session_key(&keys.user, &header[..8]);
        nonce[..12].copy_from_slice(&header[4..16]);
        shared::encrypt_aes_2022_header(cipher, first.as_ref(), &mut header)?;
        wire[..16].copy_from_slice(&header);
    }
    wire[prefix..prefix + length].copy_from_slice(&plain[..length]);
    let (body, tag) = wire[prefix..prefix + length + cipher.tag_len()].split_at_mut(length);
    let nonce = if cipher.is_2022_chacha() {
        &nonce[..24]
    } else {
        &nonce[..12]
    };
    shared::aead_udp_buffered(cipher, key.as_ref(), nonce, body, tag, true)?;
    Ok(prefix + length + cipher.tag_len())
}
