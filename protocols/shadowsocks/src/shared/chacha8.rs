//! SIP022 optional ChaCha8 backend: 96-bit TCP and XChaCha8 UDP nonces.
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha8Poly1305, XChaCha8Poly1305,
};
use zero_core::Error;

pub(super) fn encrypt(key: &[u8], nonce: &[u8], data: &[u8]) -> Result<Vec<u8>, Error> {
    match nonce.len() {
        12 => ChaCha8Poly1305::new_from_slice(key)
            .map_err(|_| Error::Protocol("ss: invalid key"))?
            .encrypt(nonce.into(), data),
        24 => XChaCha8Poly1305::new_from_slice(key)
            .map_err(|_| Error::Protocol("ss: invalid key"))?
            .encrypt(nonce.into(), data),
        _ => return Err(Error::Protocol("ss: invalid chacha8 nonce length")),
    }
    .map_err(|_| Error::Protocol("ss: encryption failed"))
}
pub(super) fn decrypt(key: &[u8], nonce: &[u8], data: &[u8]) -> Result<Vec<u8>, Error> {
    match nonce.len() {
        12 => ChaCha8Poly1305::new_from_slice(key)
            .map_err(|_| Error::Protocol("ss: invalid key"))?
            .decrypt(nonce.into(), data),
        24 => XChaCha8Poly1305::new_from_slice(key)
            .map_err(|_| Error::Protocol("ss: invalid key"))?
            .decrypt(nonce.into(), data),
        _ => return Err(Error::Protocol("ss: invalid chacha8 nonce length")),
    }
    .map_err(|_| Error::Protocol("ss: decryption failed"))
}
