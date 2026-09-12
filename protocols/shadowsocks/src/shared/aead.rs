use super::*;
impl CipherKind {
    fn ring_algorithm(self) -> Result<&'static ring::aead::Algorithm, Error> {
        use ring::aead;
        match self {
            Self::Aes128Gcm | Self::Blake3Aes128Gcm => Ok(&aead::AES_128_GCM),
            Self::Aes256Gcm | Self::Blake3Aes256Gcm => Ok(&aead::AES_256_GCM),
            Self::Chacha20Poly1305 | Self::Blake3Chacha20Poly1305 => Ok(&aead::CHACHA20_POLY1305),
            _ => Err(Error::Protocol("ss: cipher requires reduced-round backend")),
        }
    }
}

// AEAD encrypt / decrypt.

#[cfg(feature = "crypto")]
pub fn aead_encrypt(
    cipher: CipherKind,
    key: &[u8],
    nonce: &[u8; 12],
    plaintext: &[u8],
) -> Result<Vec<u8>, Error> {
    if cipher == CipherKind::Blake3Chacha8Poly1305 {
        return chacha8::encrypt(key, nonce, plaintext);
    }
    if cipher.is_extra_aead() {
        return extra_aead::encrypt(cipher, key, nonce, plaintext);
    }
    use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey};
    let unbound = UnboundKey::new(cipher.ring_algorithm()?, key)
        .map_err(|_| Error::Protocol("ss: invalid key"))?;
    let key = LessSafeKey::new(unbound);
    let nonce = Nonce::assume_unique_for_key(*nonce);
    let mut buf = Vec::with_capacity(plaintext.len() + cipher.tag_len());
    buf.extend_from_slice(plaintext);
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut buf)
        .map_err(|_| Error::Protocol("ss: encryption failed"))?;
    Ok(buf)
}

#[cfg(feature = "crypto")]
pub fn aead_decrypt(
    cipher: CipherKind,
    key: &[u8],
    nonce: &[u8; 12],
    ciphertext: &[u8],
) -> Result<Vec<u8>, Error> {
    if cipher == CipherKind::Blake3Chacha8Poly1305 {
        return chacha8::decrypt(key, nonce, ciphertext);
    }
    if cipher.is_extra_aead() {
        return extra_aead::decrypt(cipher, key, nonce, ciphertext);
    }
    use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey};
    if ciphertext.len() < cipher.tag_len() {
        return Err(Error::Protocol("ss: ciphertext too short"));
    }
    let unbound = UnboundKey::new(cipher.ring_algorithm()?, key)
        .map_err(|_| Error::Protocol("ss: invalid key"))?;
    let key = LessSafeKey::new(unbound);
    let nonce = Nonce::assume_unique_for_key(*nonce);
    let mut buf = ciphertext.to_vec();
    let decrypted = key
        .open_in_place(nonce, Aad::empty(), &mut buf)
        .map_err(|_| Error::Protocol("ss: decryption failed"))?;
    Ok(decrypted.to_vec())
}

// UDP AEAD uses per-packet salt and a fixed zero nonce.

#[cfg(feature = "crypto")]
pub(crate) fn aead_encrypt_udp(
    cipher: CipherKind,
    key: &[u8],
    nonce: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, Error> {
    if cipher == CipherKind::Blake3Chacha8Poly1305 {
        return chacha8::encrypt(key, nonce, plaintext);
    }
    if cipher.is_extra_aead() {
        return extra_aead::encrypt(cipher, key, nonce, plaintext);
    }
    if cipher.is_2022_chacha() {
        use chacha20poly1305::{
            aead::{AeadInPlace, KeyInit},
            XChaCha20Poly1305, XNonce,
        };
        if nonce.len() != 24 {
            return Err(Error::Protocol("ss: invalid xchacha nonce length"));
        }
        let cipher = XChaCha20Poly1305::new_from_slice(key)
            .map_err(|_| Error::Protocol("ss: invalid key"))?;
        let mut buf =
            Vec::with_capacity(plaintext.len() + CipherKind::Blake3Chacha20Poly1305.tag_len());
        buf.extend_from_slice(plaintext);
        cipher
            .encrypt_in_place(XNonce::from_slice(nonce), b"", &mut buf)
            .map_err(|_| Error::Protocol("ss: encryption failed"))?;
        return Ok(buf);
    }

    use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey};
    let nonce: &[u8; 12] = nonce
        .try_into()
        .map_err(|_| Error::Protocol("ss: invalid nonce length"))?;
    let unbound = UnboundKey::new(cipher.ring_algorithm()?, key)
        .map_err(|_| Error::Protocol("ss: invalid key"))?;
    let key = LessSafeKey::new(unbound);
    let nonce = Nonce::assume_unique_for_key(*nonce);
    let mut buf = Vec::with_capacity(plaintext.len() + cipher.tag_len());
    buf.extend_from_slice(plaintext);
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut buf)
        .map_err(|_| Error::Protocol("ss: encryption failed"))?;
    Ok(buf)
}

#[cfg(feature = "crypto")]
pub(crate) fn aead_decrypt_udp(
    cipher: CipherKind,
    key: &[u8],
    nonce: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, Error> {
    if cipher == CipherKind::Blake3Chacha8Poly1305 {
        return chacha8::decrypt(key, nonce, ciphertext);
    }
    if cipher.is_extra_aead() {
        return extra_aead::decrypt(cipher, key, nonce, ciphertext);
    }
    if cipher.is_2022_chacha() {
        use chacha20poly1305::{
            aead::{AeadInPlace, KeyInit},
            XChaCha20Poly1305, XNonce,
        };
        if nonce.len() != 24 {
            return Err(Error::Protocol("ss: invalid xchacha nonce length"));
        }
        if ciphertext.len() < cipher.tag_len() {
            return Err(Error::Protocol("ss: ciphertext too short"));
        }
        let cipher = XChaCha20Poly1305::new_from_slice(key)
            .map_err(|_| Error::Protocol("ss: invalid key"))?;
        let mut buf = ciphertext.to_vec();
        cipher
            .decrypt_in_place(XNonce::from_slice(nonce), b"", &mut buf)
            .map_err(|_| Error::Protocol("ss: decryption failed"))?;
        return Ok(buf);
    }

    use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey};
    if ciphertext.len() < cipher.tag_len() {
        return Err(Error::Protocol("ss: ciphertext too short"));
    }
    let nonce: &[u8; 12] = nonce
        .try_into()
        .map_err(|_| Error::Protocol("ss: invalid nonce length"))?;
    let unbound = UnboundKey::new(cipher.ring_algorithm()?, key)
        .map_err(|_| Error::Protocol("ss: invalid key"))?;
    let key = LessSafeKey::new(unbound);
    let nonce = Nonce::assume_unique_for_key(*nonce);
    let mut buf = ciphertext.to_vec();
    let decrypted = key
        .open_in_place(nonce, Aad::empty(), &mut buf)
        .map_err(|_| Error::Protocol("ss: decryption failed"))?;
    Ok(decrypted.to_vec())
}
