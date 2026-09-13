use super::*;
use blake2::{digest::consts::U32, Blake2b, Digest};
use ring::{aead, digest};
pub(super) enum Crypto {
    Original,
    Aes(aead::LessSafeKey),
    Salamander(Vec<u8>),
}
impl Crypto {
    pub(super) fn new(mask: &Mask) -> io::Result<Self> {
        match mask {
            Mask::MkcpOriginal => Ok(Self::Original),
            Mask::MkcpAes128Gcm { password } => {
                let hash = digest::digest(&digest::SHA256, password.as_bytes());
                let key = aead::UnboundKey::new(&aead::AES_128_GCM, &hash.as_ref()[..16])
                    .map_err(|_| invalid("invalid AES key"))?;
                Ok(Self::Aes(aead::LessSafeKey::new(key)))
            }
            Mask::Salamander { password } if password.len() >= 4 => {
                Ok(Self::Salamander(password.as_bytes().to_vec()))
            }
            _ => Err(invalid("Salamander requires at least four password bytes")),
        }
    }
    pub(super) fn encode(&self, packet: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Self::Original => {
                let length = u16::try_from(packet.len())
                    .map_err(|_| invalid("mKCP mask payload too long"))?;
                let mut out = vec![0; 4];
                out.extend(length.to_be_bytes());
                out.extend(packet);
                let hash = fnv(&out[4..]);
                out[..4].copy_from_slice(&hash.to_be_bytes());
                for i in 4..out.len() {
                    out[i] ^= out[i - 4];
                }
                Ok(out)
            }
            Self::Aes(key) => {
                let nonce: [u8; 12] = rand::random();
                let mut out = packet.to_vec();
                key.seal_in_place_append_tag(
                    aead::Nonce::assume_unique_for_key(nonce),
                    aead::Aad::empty(),
                    &mut out,
                )
                .map_err(|_| invalid("mKCP AES encryption failed"))?;
                let mut wire = nonce.to_vec();
                wire.extend(out);
                Ok(wire)
            }
            Self::Salamander(password) => {
                let salt: [u8; 8] = rand::random();
                let key = salamander_key(password, &salt);
                let mut out = salt.to_vec();
                out.extend(packet.iter().enumerate().map(|(i, b)| b ^ key[i % 32]));
                Ok(out)
            }
        }
    }
    pub(super) fn decode(&self, packet: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Self::Original => {
                if packet.len() < 6 {
                    return Err(invalid("short mKCP original mask"));
                }
                let mut out = packet.to_vec();
                for i in (4..out.len()).rev() {
                    out[i] ^= out[i - 4];
                }
                let checksum = u32::from_be_bytes(out[..4].try_into().unwrap());
                let length = u16::from_be_bytes(out[4..6].try_into().unwrap());
                if checksum != fnv(&out[4..]) || usize::from(length) != out.len() - 6 {
                    return Err(invalid("invalid mKCP original authentication"));
                }
                Ok(out.split_off(6))
            }
            Self::Aes(key) => {
                if packet.len() < 28 {
                    return Err(invalid("short mKCP AES mask"));
                }
                let nonce = packet[..12].try_into().unwrap();
                let mut out = packet[12..].to_vec();
                let plain = key
                    .open_in_place(
                        aead::Nonce::assume_unique_for_key(nonce),
                        aead::Aad::empty(),
                        &mut out,
                    )
                    .map_err(|_| invalid("invalid mKCP AES authentication"))?;
                Ok(plain.to_vec())
            }
            Self::Salamander(password) => {
                if packet.len() <= 8 {
                    return Err(invalid("short Salamander mask"));
                }
                let key = salamander_key(password, &packet[..8]);
                Ok(packet[8..]
                    .iter()
                    .enumerate()
                    .map(|(i, b)| b ^ key[i % 32])
                    .collect())
            }
        }
    }
}
fn fnv(bytes: &[u8]) -> u32 {
    bytes.iter().fold(2166136261u32, |hash, byte| {
        (hash ^ u32::from(*byte)).wrapping_mul(16777619)
    })
}
fn salamander_key(password: &[u8], salt: &[u8]) -> [u8; 32] {
    let mut hash = Blake2b::<U32>::new();
    hash.update(password);
    hash.update(salt);
    hash.finalize().into()
}
