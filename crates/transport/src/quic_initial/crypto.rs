use aes::cipher::{BlockEncrypt, KeyInit};
use aes::Aes128;
use ring::{aead, hkdf};

const INITIAL_SALT_V1: [u8; 20] = [
    0x38, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17, 0x9a, 0xe6, 0xa4, 0xc8,
    0x0c, 0xad, 0xcc, 0xbb, 0x7f, 0x0a,
];
const INITIAL_SALT_V2: [u8; 20] = [
    0x0d, 0xed, 0xe3, 0xde, 0xf7, 0x00, 0xa6, 0xdb, 0x81, 0x93, 0x81, 0xbe, 0x6e, 0x26,
    0x9d, 0xcb, 0xf9, 0xbd, 0x2e, 0xd9,
];

pub(super) struct InitialKeys {
    key: [u8; 16],
    iv: [u8; 12],
    hp: [u8; 16],
}

impl InitialKeys {
    pub(super) fn derive(version: u32, destination_connection_id: &[u8]) -> Result<Self, ()> {
        let (salt, key_label, iv_label, hp_label) = match version {
            super::packet::QUIC_V1 => (
                INITIAL_SALT_V1.as_slice(),
                "quic key",
                "quic iv",
                "quic hp",
            ),
            super::packet::QUIC_V2 => (
                INITIAL_SALT_V2.as_slice(),
                "quicv2 key",
                "quicv2 iv",
                "quicv2 hp",
            ),
            _ => return Err(()),
        };
        let initial = hkdf::Salt::new(hkdf::HKDF_SHA256, salt).extract(destination_connection_id);
        let client_secret = expand::<32>(&initial, "client in")?;
        let client = hkdf::Prk::new_less_safe(hkdf::HKDF_SHA256, &client_secret);
        Ok(Self {
            key: expand::<16>(&client, key_label)?,
            iv: expand::<12>(&client, iv_label)?,
            hp: expand::<16>(&client, hp_label)?,
        })
    }

    pub(super) fn header_mask(&self, sample: &[u8]) -> Result<[u8; 5], ()> {
        let mut block = aes::Block::<Aes128>::clone_from_slice(sample.get(..16).ok_or(())?);
        Aes128::new_from_slice(&self.hp)
            .map_err(|_| ())?
            .encrypt_block(&mut block);
        let mut mask = [0_u8; 5];
        mask.copy_from_slice(&block[..5]);
        Ok(mask)
    }

    pub(super) fn decrypt(
        &self,
        packet_number: u64,
        header: &[u8],
        ciphertext: &mut [u8],
    ) -> Result<Vec<u8>, ()> {
        let mut nonce = self.iv;
        for (left, right) in nonce[4..].iter_mut().zip(packet_number.to_be_bytes()) {
            *left ^= right;
        }
        let key = aead::UnboundKey::new(&aead::AES_128_GCM, &self.key).map_err(|_| ())?;
        let key = aead::LessSafeKey::new(key);
        let plaintext = key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(header),
                ciphertext,
            )
            .map_err(|_| ())?;
        Ok(plaintext.to_vec())
    }
}

struct OutputLength(usize);

impl hkdf::KeyType for OutputLength {
    fn len(&self) -> usize {
        self.0
    }
}

fn expand<const N: usize>(secret: &hkdf::Prk, label: &str) -> Result<[u8; N], ()> {
    let label = format!("tls13 {label}");
    let mut info = Vec::with_capacity(4 + label.len());
    info.extend_from_slice(&(N as u16).to_be_bytes());
    info.push(label.len().try_into().map_err(|_| ())?);
    info.extend_from_slice(label.as_bytes());
    info.push(0);
    let mut output = [0_u8; N];
    let info_parts = [info.as_slice()];
    let output_key = secret.expand(&info_parts, OutputLength(N)).map_err(|_| ())?;
    output_key.fill(&mut output).map_err(|_| ())?;
    Ok(output)
}

#[cfg(test)]
pub(super) fn vector_keys(destination_connection_id: &[u8]) -> Result<InitialKeys, ()> {
    InitialKeys::derive(super::packet::QUIC_V1, destination_connection_id)
}

#[cfg(test)]
impl InitialKeys {
    pub(super) fn parts(&self) -> (&[u8], &[u8], &[u8]) {
        (&self.key, &self.iv, &self.hp)
    }
}
