// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use aes::cipher::{KeyIvInit, StreamCipher};
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use chacha20poly1305::ChaCha20Poly1305;
use std::io;

pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
pub(super) fn derive(context: &[u8], key: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key_bytes(context);
    hasher.update(key);
    *hasher.finalize().as_bytes()
}
pub(super) type Ctr = ctr::Ctr128BE<aes::Aes256>;
pub(super) fn ctr(key: &[u8], iv: &[u8; 16]) -> Ctr {
    Ctr::new(&derive(b"VLESS", key).into(), iv.into())
}
pub(super) fn mask(ctr: &mut Option<Ctr>, header: &mut [u8]) {
    if let Some(ctr) = ctr {
        ctr.apply_keystream(header);
    }
}
enum Cipher {
    Aes(Aes256Gcm),
    ChaCha(ChaCha20Poly1305),
}
pub(super) struct AeadState {
    cipher: Cipher,
    nonce: [u8; 12],
    pub aes: bool,
}
impl AeadState {
    pub fn new(context: &[u8], key: &[u8], aes: bool) -> Self {
        let key = derive(context, key);
        let cipher = if aes {
            Cipher::Aes(Aes256Gcm::new(&key.into()))
        } else {
            Cipher::ChaCha(ChaCha20Poly1305::new(&key.into()))
        };
        Self {
            cipher,
            nonce: [0; 12],
            aes,
        }
    }
    fn next(&mut self) -> [u8; 12] {
        for byte in self.nonce.iter_mut().rev() {
            *byte = byte.wrapping_add(1);
            if *byte != 0 {
                break;
            }
        }
        self.nonce
    }
    pub fn exhausted(&self) -> bool {
        self.nonce == [255; 12]
    }
    pub fn seal_at(&self, nonce: &[u8; 12], plain: &[u8], aad: &[u8]) -> io::Result<Vec<u8>> {
        let payload = Payload { msg: plain, aad };
        match &self.cipher {
            Cipher::Aes(c) => c.encrypt(Nonce::from_slice(nonce), payload),
            Cipher::ChaCha(c) => c.encrypt(Nonce::from_slice(nonce), payload),
        }
        .map_err(|_| invalid("encryption failed"))
    }
    pub fn open_at(&self, nonce: &[u8; 12], data: &[u8], aad: &[u8]) -> io::Result<Vec<u8>> {
        let payload = Payload { msg: data, aad };
        match &self.cipher {
            Cipher::Aes(c) => c.decrypt(Nonce::from_slice(nonce), payload),
            Cipher::ChaCha(c) => c.decrypt(Nonce::from_slice(nonce), payload),
        }
        .map_err(|_| invalid("encryption authentication failed"))
    }
    pub fn seal(&mut self, plain: &[u8], aad: &[u8]) -> io::Result<Vec<u8>> {
        let nonce = self.next();
        self.seal_at(&nonce, plain, aad)
    }
    pub fn open(&mut self, data: &[u8], aad: &[u8]) -> io::Result<Vec<u8>> {
        let nonce = self.next();
        self.open_at(&nonce, data, aad)
    }
}
pub(super) fn header(length: usize) -> [u8; 5] {
    [23, 3, 3, (length >> 8) as u8, length as u8]
}
pub(super) fn record_length(header: &[u8; 5]) -> io::Result<usize> {
    let len = usize::from(u16::from_be_bytes([header[3], header[4]]));
    if header[..3] != [23, 3, 3] || !(17..=16640).contains(&len) {
        return Err(invalid("invalid encrypted record header"));
    }
    Ok(len)
}
