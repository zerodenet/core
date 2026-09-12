use super::*;
#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn identity_hash_2022(cipher: CipherKind, password: &[u8]) -> Result<[u8; 16], Error> {
    let master_key = decode_blake3_master_key(cipher, password)?;
    let digest = blake3::hash(&master_key);
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(identity)
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) struct Shadowsocks2022KeyChain {
    pub(crate) identity_keys: Vec<Vec<u8>>,
    pub(crate) user_password: Vec<u8>,
    pub(crate) user_key: Vec<u8>,
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn parse_2022_key_chain(
    cipher: CipherKind,
    password: &[u8],
) -> Result<Shadowsocks2022KeyChain, Error> {
    let password = core::str::from_utf8(password)
        .map_err(|_| Error::Protocol("ss: 2022 password chain must be utf-8"))?;
    let segments = password.split(':').collect::<Vec<_>>();
    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(Error::Protocol(
            "ss: 2022 password chain contains an empty PSK",
        ));
    }
    if segments.len() > 1
        && !matches!(
            cipher,
            CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm
        )
    {
        return Err(Error::Protocol("ss: SIP023 EIH requires a 2022 AES method"));
    }
    let (user_password, identities) = segments
        .split_last()
        .ok_or(Error::Protocol("ss: missing 2022 user PSK"))?;
    let user_key = decode_blake3_master_key(cipher, user_password.as_bytes())?;
    let identity_keys = identities
        .iter()
        .map(|identity| decode_blake3_master_key(cipher, identity.as_bytes()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Shadowsocks2022KeyChain {
        identity_keys,
        user_password: user_password.as_bytes().to_vec(),
        user_key,
    })
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn encode_tcp_2022_identity_headers(
    cipher: CipherKind,
    identity_keys: &[Vec<u8>],
    user_key: &[u8],
    salt: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut headers = Vec::with_capacity(identity_keys.len() * 16);
    for (index, identity_key) in identity_keys.iter().enumerate() {
        let next_key = identity_keys.get(index + 1).map_or(user_key, Vec::as_slice);
        let mut material = Vec::with_capacity(identity_key.len() + salt.len());
        material.extend_from_slice(identity_key);
        material.extend_from_slice(salt);
        let identity_subkey = blake3::derive_key(SS_2022_IDENTITY_SUBKEY_CONTEXT, &material);
        let mut header = [0_u8; 16];
        header.copy_from_slice(&blake3::hash(next_key).as_bytes()[..16]);
        encrypt_aes_2022_header(cipher, &identity_subkey[..cipher.key_len()], &mut header)?;
        headers.extend_from_slice(&header);
    }
    Ok(headers)
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(super) fn encode_udp_2022_identity_headers(
    cipher: CipherKind,
    identity_keys: &[Vec<u8>],
    user_key: &[u8],
    separate_header: &[u8; 16],
) -> Result<Vec<u8>, Error> {
    let mut headers = Vec::with_capacity(identity_keys.len() * 16);
    for (index, identity_key) in identity_keys.iter().enumerate() {
        let next_key = identity_keys.get(index + 1).map_or(user_key, Vec::as_slice);
        let mut header = [0_u8; 16];
        header.copy_from_slice(&blake3::hash(next_key).as_bytes()[..16]);
        for (byte, separate_byte) in header.iter_mut().zip(separate_header) {
            *byte ^= separate_byte;
        }
        encrypt_aes_2022_header(cipher, identity_key, &mut header)?;
        headers.extend_from_slice(&header);
    }
    Ok(headers)
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn decrypt_tcp_2022_identity_header(
    cipher: CipherKind,
    identity_password: &[u8],
    salt: &[u8],
    encrypted_identity: &[u8],
) -> Result<[u8; 16], Error> {
    if encrypted_identity.len() != 16 {
        return Err(Error::Protocol(
            "ss: invalid SIP023 tcp identity header length",
        ));
    }
    let identity_key = decode_blake3_master_key(cipher, identity_password)?;
    let mut material = Vec::with_capacity(identity_key.len() + salt.len());
    material.extend_from_slice(&identity_key);
    material.extend_from_slice(salt);
    let identity_subkey = blake3::derive_key(SS_2022_IDENTITY_SUBKEY_CONTEXT, &material);
    let mut identity = [0_u8; 16];
    identity.copy_from_slice(encrypted_identity);
    decrypt_aes_2022_header(cipher, &identity_subkey[..cipher.key_len()], &mut identity)?;
    Ok(identity)
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
fn identify_udp_2022_eih(
    cipher: CipherKind,
    identity_password: &[u8],
    datagram: &[u8],
) -> Result<([u8; 16], [u8; 16]), Error> {
    if !matches!(
        cipher,
        CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm
    ) {
        return Err(Error::Protocol("ss: SIP023 EIH requires a 2022 AES method"));
    }
    if datagram.len() < 32 + cipher.tag_len() {
        return Err(Error::Protocol("ss: SIP023 udp datagram too short"));
    }
    let identity_key = decode_blake3_master_key(cipher, identity_password)?;
    let mut separate_header = [0_u8; 16];
    separate_header.copy_from_slice(&datagram[..16]);
    decrypt_aes_2022_header(cipher, &identity_key, &mut separate_header)?;

    let mut user_hash = [0_u8; 16];
    user_hash.copy_from_slice(&datagram[16..32]);
    decrypt_aes_2022_header(cipher, &identity_key, &mut user_hash)?;
    for (byte, header_byte) in user_hash.iter_mut().zip(separate_header) {
        *byte ^= header_byte;
    }
    Ok((user_hash, separate_header))
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn identify_udp_2022_user(
    cipher: CipherKind,
    identity_password: &[u8],
    datagram: &[u8],
) -> Result<[u8; 16], Error> {
    identify_udp_2022_eih(cipher, identity_password, datagram).map(|(identity, _)| identity)
}

#[cfg(all(feature = "crypto", feature = "blake3"))]
pub(crate) fn decode_udp_datagram_2022_eih_session(
    cipher: CipherKind,
    identity_password: &[u8],
    user_password: &[u8],
    datagram: &[u8],
) -> Result<(Address, u16, Vec<u8>, u64, u64), Error> {
    let (identity, separate_header) = identify_udp_2022_eih(cipher, identity_password, datagram)?;
    if identity != identity_hash_2022(cipher, user_password)? {
        return Err(Error::Protocol("ss: SIP023 udp user identity mismatch"));
    }

    let user_key = decode_blake3_master_key(cipher, user_password)?;
    let session_key = derive_key_blake3(&user_key, &separate_header[..8], cipher.key_len())?;
    let message = aead_decrypt_udp(
        cipher,
        &session_key,
        &separate_header[4..16],
        &datagram[32..],
    )?;
    let mut plain = Vec::with_capacity(separate_header.len() + message.len());
    plain.extend_from_slice(&separate_header);
    plain.extend_from_slice(&message);
    udp_wire::parse_udp_2022_plain(&plain)?.into_request()
}
