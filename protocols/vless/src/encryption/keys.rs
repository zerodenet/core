// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use super::crypto::{ctr, invalid, Ctr};
use aes::cipher::StreamCipher;
use ml_kem::EncodedSizeUser;
use std::io;
use x25519_dalek::{PublicKey, StaticSecret};

pub(super) use crate::mlkem::{
    decapsulate, encapsulate, kem_public, kem_secret, random, KemSecret,
};
pub(super) fn exchange(secret: &StaticSecret, bytes: &[u8]) -> io::Result<[u8; 32]> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| invalid("invalid X25519 public key"))?;
    let shared = secret.diffie_hellman(&PublicKey::from(bytes));
    if !shared.was_contributory() {
        return Err(invalid("non-contributory X25519 public key"));
    }
    Ok(shared.to_bytes())
}
pub(super) enum Secret {
    X25519(StaticSecret),
    Kem(Box<KemSecret>),
}
pub(super) struct ServerKey {
    secret: Secret,
    public: Vec<u8>,
}
impl ServerKey {
    pub fn new(bytes: &[u8]) -> io::Result<Self> {
        let secret = if bytes.len() == 32 {
            Secret::X25519(StaticSecret::from(<[u8; 32]>::try_from(bytes).unwrap()))
        } else {
            Secret::Kem(Box::new(kem_secret(bytes)?))
        };
        let public = match &secret {
            Secret::X25519(s) => PublicKey::from(s).as_bytes().to_vec(),
            Secret::Kem(s) => s.encapsulation_key().as_bytes().to_vec(),
        };
        Ok(Self { secret, public })
    }
    pub fn public(&self) -> &[u8] {
        &self.public
    }
    pub fn wire_len(&self) -> usize {
        if matches!(self.secret, Secret::X25519(_)) {
            32
        } else {
            1088
        }
    }
    fn exchange(&self, bytes: &[u8]) -> io::Result<[u8; 32]> {
        match &self.secret {
            Secret::X25519(s) => {
                if bytes[31] > 127 {
                    return Err(invalid("non-canonical X25519 relay key"));
                }
                exchange(s, bytes)
            }
            Secret::Kem(s) => decapsulate(s, bytes),
        }
    }
}
pub(super) fn validate_public(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() == 32 {
        Ok(())
    } else {
        kem_public(bytes).map(|_| ())
    }
}
pub(super) fn client_relays(
    keys: &[Vec<u8>],
    iv: &[u8; 16],
    xor: bool,
) -> io::Result<(Vec<u8>, [u8; 32])> {
    let mut wire = Vec::new();
    let mut previous: Option<Ctr> = None;
    let mut final_key = [0; 32];
    for (index, public) in keys.iter().enumerate() {
        let (mut bytes, key) = if public.len() == 32 {
            let secret = StaticSecret::from(random::<32>());
            (
                PublicKey::from(&secret).as_bytes().to_vec(),
                exchange(&secret, public)?,
            )
        } else {
            encapsulate(public)?
        };
        if xor {
            ctr(public, iv).apply_keystream(&mut bytes);
        }
        if let Some(previous) = &mut previous {
            previous.apply_keystream(&mut bytes[..32]);
        }
        wire.extend(bytes);
        final_key = key;
        if let Some(next) = keys.get(index + 1) {
            let mut cipher = ctr(&key, iv);
            let mut hash = *blake3::hash(next).as_bytes();
            cipher.apply_keystream(&mut hash);
            wire.extend(hash);
            previous = Some(cipher);
        }
    }
    Ok((wire, final_key))
}
pub(super) fn server_relays(
    keys: &[ServerKey],
    iv: &[u8; 16],
    bytes: &mut [u8],
    xor: bool,
) -> io::Result<[u8; 32]> {
    let mut previous: Option<Ctr> = None;
    let mut offset = 0;
    let mut key = [0; 32];
    for (index, secret) in keys.iter().enumerate() {
        let len = secret.wire_len();
        let relay = bytes
            .get_mut(offset..offset + len)
            .ok_or_else(|| invalid("truncated key chain"))?;
        if let Some(previous) = &mut previous {
            previous.apply_keystream(&mut relay[..32]);
        }
        if xor {
            ctr(secret.public(), iv).apply_keystream(relay);
        }
        key = secret.exchange(relay)?;
        offset += len;
        if let Some(next) = keys.get(index + 1) {
            let hash = bytes
                .get_mut(offset..offset + 32)
                .ok_or_else(|| invalid("truncated key chain hash"))?;
            let mut cipher = ctr(&key, iv);
            cipher.apply_keystream(hash);
            if hash != blake3::hash(next.public()).as_bytes() {
                return Err(invalid("key chain identity mismatch"));
            }
            previous = Some(cipher);
            offset += 32;
        }
    }
    Ok(key)
}
