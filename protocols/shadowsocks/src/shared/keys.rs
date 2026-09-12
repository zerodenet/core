use super::*;
// Key derivation.

#[cfg(feature = "crypto")]
pub fn derive_key(password: &[u8], salt: &[u8], key_len: usize) -> Result<Vec<u8>, Error> {
    use ring::hkdf::Salt;
    let master_key = evp_bytes_to_key(password, key_len);
    let salt = Salt::new(ring::hkdf::HKDF_SHA1_FOR_LEGACY_USE_ONLY, salt);
    let prk = salt.extract(&master_key);
    let mut key = vec![0u8; key_len];
    prk.expand(&[b"ss-subkey"], ShadowsocksKeyLen(key_len))
        .and_then(|okm| okm.fill(&mut key))
        .map_err(|_| Error::Protocol("ss: key derivation failed"))?;
    Ok(key)
}

#[cfg(feature = "crypto")]
pub(super) fn evp_bytes_to_key(password: &[u8], key_len: usize) -> Vec<u8> {
    use md5::{Digest, Md5};

    let mut key = Vec::with_capacity(key_len);
    let mut previous = Vec::new();
    while key.len() < key_len {
        let mut hasher = Md5::new();
        if !previous.is_empty() {
            hasher.update(&previous);
        }
        hasher.update(password);
        previous = hasher.finalize().to_vec();
        key.extend_from_slice(&previous);
    }
    key.truncate(key_len);
    key
}

// 2022 Blake3 KDF.

#[cfg(feature = "blake3")]
pub fn derive_key_blake3(
    master_key: &[u8],
    material: &[u8],
    key_len: usize,
) -> Result<Vec<u8>, Error> {
    let mut key = vec![0u8; key_len];
    let mut hasher = blake3::Hasher::new_derive_key("shadowsocks 2022 session subkey");
    hasher.update(master_key);
    if !material.is_empty() {
        hasher.update(material);
    }
    hasher.finalize_xof().fill(&mut key);
    Ok(key)
}

#[cfg(feature = "crypto")]
pub fn derive_session_key(
    cipher: CipherKind,
    password: &[u8],
    salt: &[u8],
) -> Result<Vec<u8>, Error> {
    if cipher.is_blake3() {
        #[cfg(feature = "blake3")]
        {
            let master_key = decode_blake3_master_key(cipher, password)?;
            return derive_key_blake3(&master_key, salt, cipher.key_len());
        }
        #[cfg(not(feature = "blake3"))]
        return Err(Error::Protocol(
            "ss: blake3 key derivation requires `blake3` feature",
        ));
    }
    derive_key(password, salt, cipher.key_len())
}

#[cfg(feature = "crypto")]
pub(crate) fn derive_udp_packet_key(
    cipher: CipherKind,
    password: &[u8],
    salt: &[u8],
) -> Result<Vec<u8>, Error> {
    if !cipher.is_blake3() {
        return derive_key(password, salt, cipher.key_len());
    }

    #[cfg(feature = "blake3")]
    {
        let key_chain = parse_2022_key_chain(cipher, password)?;
        let master_key = key_chain.user_key;
        match cipher {
            CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm => {
                if salt.len() != 12 {
                    return Err(Error::Protocol("ss: invalid 2022 aes udp nonce length"));
                }
                let mut session_id = [0u8; 8];
                session_id.copy_from_slice(&salt[..8]);
                derive_key_blake3(&master_key, &session_id, cipher.key_len())
            }
            CipherKind::Blake3Chacha20Poly1305 | CipherKind::Blake3Chacha8Poly1305 => {
                if salt.len() != 24 {
                    return Err(Error::Protocol("ss: invalid 2022 chacha udp nonce length"));
                }
                Ok(master_key)
            }
            _ => Err(Error::Protocol("ss: cipher is not a 2022 method")),
        }
    }
    #[cfg(not(feature = "blake3"))]
    {
        let _ = (cipher, password, salt);
        Err(Error::Protocol(
            "ss: blake3 key derivation requires `blake3` feature",
        ))
    }
}

#[cfg(feature = "crypto")]
struct ShadowsocksKeyLen(usize);

#[cfg(feature = "crypto")]
impl ring::hkdf::KeyType for ShadowsocksKeyLen {
    fn len(&self) -> usize {
        self.0
    }
}

// TCP stream helpers.

/// Derive the download key from password and salt.
///
/// Handles both standard (HKDF) and blake3 key derivation based on cipher type.
/// For outbound connections, the download key is derived from the server's
/// response salt. For inbound connections, from the client's request salt.
#[cfg(feature = "crypto")]
pub fn derive_download_key(
    cipher: CipherKind,
    password: &[u8],
    salt: &[u8],
) -> Result<Vec<u8>, Error> {
    derive_session_key(cipher, password, salt)
}
