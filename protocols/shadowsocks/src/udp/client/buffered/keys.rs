//! Borrowed SIP022/023 keys for the caller-reserved UDP path only.
use crate::shared::CipherKind;
use base64::{
    alphabet,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
    Engine,
};
use zero_core::Error;

const ENGINE: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[derive(Clone, Copy)]
pub(super) struct Key {
    bytes: [u8; 32],
    len: usize,
}
impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

fn decode(cipher: CipherKind, password: &str) -> Result<Key, Error> {
    let invalid = || Error::Protocol("ss: invalid 2022 base64 password");
    let mut key = Key {
        bytes: [0; 32],
        len: cipher.key_len(),
    };
    // Validate even oversized malformed input with the original engine, without
    // allocating an attacker-sized decoded key. Nonfinal chunks cannot pad.
    let mut input = password.as_bytes();
    let mut decoded = 0;
    let mut scratch = [0; 36];
    while input.len() > 44 {
        let (chunk, remaining) = input.split_at(40);
        if chunk.contains(&b'=') {
            return Err(invalid());
        }
        let count = ENGINE
            .decode_slice(chunk, &mut scratch)
            .map_err(|_| invalid())?;
        let retained = count.min(key.bytes.len().saturating_sub(decoded));
        key.bytes[decoded.min(32)..decoded.min(32) + retained]
            .copy_from_slice(&scratch[..retained]);
        decoded += count; // Decoded bytes never exceed the original input length.
        input = remaining;
    }
    let count = ENGINE
        .decode_slice(input, &mut scratch)
        .map_err(|_| invalid())?;
    let retained = count.min(key.bytes.len().saturating_sub(decoded));
    key.bytes[decoded.min(32)..decoded.min(32) + retained].copy_from_slice(&scratch[..retained]);
    if decoded + count != key.len {
        return Err(Error::Protocol("ss: invalid 2022 password key length"));
    }
    Ok(key)
}

pub(super) struct Chain<'a> {
    cipher: CipherKind,
    identities: Option<&'a str>,
    pub(super) user: Key,
}
impl<'a> Chain<'a> {
    pub(super) fn new(cipher: CipherKind, password: &'a [u8]) -> Result<Self, Error> {
        let password = core::str::from_utf8(password)
            .map_err(|_| Error::Protocol("ss: 2022 password chain must be utf-8"))?;
        if password.split(':').any(str::is_empty) {
            return Err(Error::Protocol(
                "ss: 2022 password chain contains an empty PSK",
            ));
        }
        let (identities, user) = match password.rsplit_once(':') {
            Some((identities, user)) => (Some(identities), user),
            None => (None, password),
        };
        if identities.is_some()
            && !matches!(
                cipher,
                CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm
            )
        {
            return Err(Error::Protocol("ss: SIP023 EIH requires a 2022 AES method"));
        }
        // Match the old parser's validation order: user first, then identities.
        let chain = Self {
            cipher,
            identities,
            user: decode(cipher, user)?,
        };
        for identity in chain.identities() {
            identity?;
        }
        Ok(chain)
    }
    pub(super) fn cipher(&self) -> CipherKind {
        self.cipher
    }
    pub(super) fn identity_count(&self) -> usize {
        self.identities
            .map_or(0, |identities| identities.split(':').count())
    }
    pub(super) fn identities(&self) -> impl Iterator<Item = Result<Key, Error>> + '_ {
        self.identities
            .into_iter()
            .flat_map(|identities| identities.split(':'))
            .map(|password| decode(self.cipher, password))
    }
}

pub(super) fn session_key(master: &Key, material: &[u8]) -> Key {
    let mut key = *master;
    let mut hasher = blake3::Hasher::new_derive_key("shadowsocks 2022 session subkey");
    hasher.update(master.as_ref());
    if !material.is_empty() {
        hasher.update(material);
    }
    hasher.finalize_xof().fill(&mut key.bytes[..key.len]);
    key
}

#[cfg(test)]
mod tests;
