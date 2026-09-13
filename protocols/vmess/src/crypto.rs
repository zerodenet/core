use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use std::sync::Mutex;

use ring::aead::{Aad, BoundKey, Nonce, NonceSequence, OpeningKey, SealingKey, UnboundKey};
use ring::hkdf::{KeyType, Salt, HKDF_SHA256};
use sha3::{
    digest::{ExtendableOutput, Update, XofReader},
    Shake128,
};

use zero_core::Error;

use crate::VmessCipher;

impl VmessCipher {
    pub(crate) fn aead_algorithm(self) -> &'static ring::aead::Algorithm {
        match self {
            VmessCipher::Aes128Gcm | VmessCipher::None | VmessCipher::Zero => {
                &ring::aead::AES_128_GCM
            }
            VmessCipher::Chacha20Poly1305 => &ring::aead::CHACHA20_POLY1305,
        }
    }
}

pub(crate) const GCM_TAG_LEN: usize = 16;
pub(crate) const NONCE_LEN: usize = 12;
pub(crate) const MAX_BODY_PAYLOAD_SIZE: usize = 16 * 1024;

const REKEY_INTERVAL: u64 = 1 << 14;
const VMESS_ID_SALT: &[u8] = b"c48619fe-8f02-49e0-b9e9-edf763e17e21";
const VMESS_AEAD_KDF_SALT: &[u8] = b"VMess AEAD KDF";
const AUTH_ID_ENCRYPTION_KEY: &[u8] = b"AES Auth ID Encryption";
const HEADER_LENGTH_AEAD_KEY: &[u8] = b"VMess Header AEAD Key_Length";
const HEADER_LENGTH_AEAD_IV: &[u8] = b"VMess Header AEAD Nonce_Length";
const HEADER_PAYLOAD_AEAD_KEY: &[u8] = b"VMess Header AEAD Key";
const HEADER_PAYLOAD_AEAD_IV: &[u8] = b"VMess Header AEAD Nonce";
const RESPONSE_HEADER_LENGTH_AEAD_KEY: &[u8] = b"AEAD Resp Header Len Key";
const RESPONSE_HEADER_LENGTH_AEAD_IV: &[u8] = b"AEAD Resp Header Len IV";
const RESPONSE_HEADER_PAYLOAD_AEAD_KEY: &[u8] = b"AEAD Resp Header Key";
const RESPONSE_HEADER_PAYLOAD_AEAD_IV: &[u8] = b"AEAD Resp Header IV";

struct HkdfLen(usize);

impl KeyType for HkdfLen {
    fn len(&self) -> usize {
        self.0
    }
}

pub(crate) fn derive_xray_cmd_key(uuid: &[u8; 16]) -> [u8; 16] {
    let mut ctx = md5::Context::new();
    ctx.consume(uuid);
    ctx.consume(VMESS_ID_SALT);
    let digest = ctx.compute();
    digest.0
}

pub(crate) fn create_xray_auth_id(cmd_key: &[u8; 16], timestamp: u64) -> Result<[u8; 16], Error> {
    let random = rand::random::<[u8; 4]>();
    let mut plain = [0_u8; 16];
    plain[..8].copy_from_slice(&timestamp.to_be_bytes());
    plain[8..12].copy_from_slice(&random);
    let crc = crc32fast::hash(&plain[..12]);
    plain[12..16].copy_from_slice(&crc.to_be_bytes());

    let key = xray_kdf16(cmd_key, &[AUTH_ID_ENCRYPTION_KEY]);
    let cipher = aes::Aes128::new_from_slice(&key)
        .map_err(|_| Error::Protocol("vmess invalid auth id key"))?;
    let mut out = [0_u8; 16];
    let mut block = aes::cipher::Block::<aes::Aes128>::clone_from_slice(&plain);
    cipher.encrypt_block(&mut block);
    out.copy_from_slice(&block);
    Ok(out)
}

pub(crate) fn decode_xray_auth_id(cmd_key: &[u8; 16], auth_id: &[u8; 16]) -> Result<u64, Error> {
    let key = xray_kdf16(cmd_key, &[AUTH_ID_ENCRYPTION_KEY]);
    let cipher = aes::Aes128::new_from_slice(&key)
        .map_err(|_| Error::Protocol("vmess invalid auth id key"))?;
    let mut block = aes::cipher::Block::<aes::Aes128>::clone_from_slice(auth_id);
    cipher.decrypt_block(&mut block);
    let checksum = u32::from_be_bytes(block[12..16].try_into().unwrap());
    if crc32fast::hash(&block[..12]) != checksum {
        return Err(Error::Protocol("vmess invalid auth id checksum"));
    }
    Ok(u64::from_be_bytes(block[..8].try_into().unwrap()))
}

pub(crate) fn seal_xray_aead_header(
    cmd_key: &[u8; 16],
    auth_id: &[u8; 16],
    header: &[u8],
) -> Result<Vec<u8>, Error> {
    let nonce = rand::random::<[u8; 8]>();
    let len_key = xray_kdf16(
        cmd_key,
        &[
            HEADER_LENGTH_AEAD_KEY,
            bytes_to_path(auth_id),
            bytes_to_path(&nonce),
        ],
    );
    let len_nonce = xray_kdf(
        cmd_key,
        &[
            HEADER_LENGTH_AEAD_IV,
            bytes_to_path(auth_id),
            bytes_to_path(&nonce),
        ],
    );
    let encrypted_len = aes_128_gcm_seal(
        &len_key,
        &len_nonce[..NONCE_LEN],
        &(header.len() as u16).to_be_bytes(),
        auth_id,
    )?;

    let payload_key = xray_kdf16(
        cmd_key,
        &[
            HEADER_PAYLOAD_AEAD_KEY,
            bytes_to_path(auth_id),
            bytes_to_path(&nonce),
        ],
    );
    let payload_nonce = xray_kdf(
        cmd_key,
        &[
            HEADER_PAYLOAD_AEAD_IV,
            bytes_to_path(auth_id),
            bytes_to_path(&nonce),
        ],
    );
    let encrypted_payload =
        aes_128_gcm_seal(&payload_key, &payload_nonce[..NONCE_LEN], header, auth_id)?;

    let mut out = Vec::with_capacity(16 + encrypted_len.len() + 8 + encrypted_payload.len());
    out.extend_from_slice(auth_id);
    out.extend_from_slice(&encrypted_len);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&encrypted_payload);
    Ok(out)
}

pub(crate) fn open_xray_aead_header_length(
    cmd_key: &[u8; 16],
    auth_id: &[u8; 16],
    encrypted_len: &[u8; 18],
    nonce: &[u8; 8],
) -> Result<usize, Error> {
    let len_key = xray_kdf16(
        cmd_key,
        &[
            HEADER_LENGTH_AEAD_KEY,
            bytes_to_path(auth_id),
            bytes_to_path(nonce),
        ],
    );
    let len_nonce = xray_kdf(
        cmd_key,
        &[
            HEADER_LENGTH_AEAD_IV,
            bytes_to_path(auth_id),
            bytes_to_path(nonce),
        ],
    );
    let len_plain = aes_128_gcm_open(&len_key, &len_nonce[..NONCE_LEN], encrypted_len, auth_id)?;
    if len_plain.len() != 2 {
        return Err(Error::Protocol("vmess xray aead header len invalid"));
    }
    Ok(u16::from_be_bytes([len_plain[0], len_plain[1]]) as usize)
}

pub(crate) fn open_xray_aead_header_payload(
    cmd_key: &[u8; 16],
    auth_id: &[u8; 16],
    nonce: &[u8; 8],
    encrypted_payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let payload_key = xray_kdf16(
        cmd_key,
        &[
            HEADER_PAYLOAD_AEAD_KEY,
            bytes_to_path(auth_id),
            bytes_to_path(nonce),
        ],
    );
    let payload_nonce = xray_kdf(
        cmd_key,
        &[
            HEADER_PAYLOAD_AEAD_IV,
            bytes_to_path(auth_id),
            bytes_to_path(nonce),
        ],
    );
    aes_128_gcm_open(
        &payload_key,
        &payload_nonce[..NONCE_LEN],
        encrypted_payload,
        auth_id,
    )
}

pub(crate) fn seal_xray_response_header(
    response_key: &[u8],
    response_nonce: &[u8],
    header: &[u8],
) -> Result<Vec<u8>, Error> {
    let len_key = xray_kdf16(response_key, &[RESPONSE_HEADER_LENGTH_AEAD_KEY]);
    let len_nonce = xray_kdf(response_nonce, &[RESPONSE_HEADER_LENGTH_AEAD_IV]);
    let encrypted_len = aes_128_gcm_seal(
        &len_key,
        &len_nonce[..NONCE_LEN],
        &(header.len() as u16).to_be_bytes(),
        &[],
    )?;

    let payload_key = xray_kdf16(response_key, &[RESPONSE_HEADER_PAYLOAD_AEAD_KEY]);
    let payload_nonce = xray_kdf(response_nonce, &[RESPONSE_HEADER_PAYLOAD_AEAD_IV]);
    let encrypted_payload =
        aes_128_gcm_seal(&payload_key, &payload_nonce[..NONCE_LEN], header, &[])?;

    let mut out = Vec::with_capacity(encrypted_len.len() + encrypted_payload.len());
    out.extend_from_slice(&encrypted_len);
    out.extend_from_slice(&encrypted_payload);
    Ok(out)
}

pub(crate) fn open_xray_response_header_length(
    response_key: &[u8],
    response_nonce: &[u8],
    encrypted_len: &[u8; 18],
) -> Result<usize, Error> {
    let len_key = xray_kdf16(response_key, &[RESPONSE_HEADER_LENGTH_AEAD_KEY]);
    let len_nonce = xray_kdf(response_nonce, &[RESPONSE_HEADER_LENGTH_AEAD_IV]);
    let len_plain = aes_128_gcm_open(&len_key, &len_nonce[..NONCE_LEN], encrypted_len, &[])?;
    if len_plain.len() != 2 {
        return Err(Error::Protocol("vmess response header len invalid"));
    }
    Ok(u16::from_be_bytes([len_plain[0], len_plain[1]]) as usize)
}

pub(crate) fn open_xray_response_header_payload(
    response_key: &[u8],
    response_nonce: &[u8],
    encrypted_payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let payload_key = xray_kdf16(response_key, &[RESPONSE_HEADER_PAYLOAD_AEAD_KEY]);
    let payload_nonce = xray_kdf(response_nonce, &[RESPONSE_HEADER_PAYLOAD_AEAD_IV]);
    aes_128_gcm_open(
        &payload_key,
        &payload_nonce[..NONCE_LEN],
        encrypted_payload,
        &[],
    )
}

pub(crate) fn vmess_aead_kdf16(key: &[u8], label: &[u8]) -> [u8; 16] {
    xray_kdf16(key, &[label])
}

/// VMess body chunk reader/writer state.
///
/// The command and response headers consume nonce 0. Body frames start from
/// nonce 1 and use one nonce for the encrypted length and one for the payload.
pub(crate) struct BodyAead {
    key: Vec<u8>,
    length_key: Option<Vec<u8>>,
    nonce_prefix: [u8; NONCE_LEN],
    length_nonce_prefix: [u8; NONCE_LEN],
    nonce_counter: u64,
    cipher: VmessCipher,
    authenticated_length: bool,
    size_mask: Option<ShakeSizeMask>,
    global_padding: bool,
    pending_padding_len: usize,
    chunks_since_rekey: u64,
}

pub(crate) struct BodyAeadConfig {
    pub(crate) key: Vec<u8>,
    pub(crate) nonce_prefix: Vec<u8>,
    pub(crate) length_key_source: Vec<u8>,
    pub(crate) length_nonce_prefix: Vec<u8>,
    pub(crate) cipher: VmessCipher,
    pub(crate) authenticated_length: bool,
    pub(crate) chunk_masking: bool,
    pub(crate) global_padding: bool,
}

impl BodyAead {
    pub fn new_with_length_source(config: BodyAeadConfig) -> Result<Self, Error> {
        let BodyAeadConfig {
            key,
            nonce_prefix,
            length_key_source,
            length_nonce_prefix,
            cipher,
            authenticated_length,
            chunk_masking,
            global_padding,
        } = config;
        if nonce_prefix.len() < NONCE_LEN {
            return Err(Error::Protocol("vmess invalid body nonce length"));
        }
        let mut fixed_nonce = [0_u8; NONCE_LEN];
        fixed_nonce.copy_from_slice(&nonce_prefix[..NONCE_LEN]);
        if length_nonce_prefix.len() < NONCE_LEN {
            return Err(Error::Protocol("vmess invalid body length nonce length"));
        }
        let mut fixed_length_nonce = [0_u8; NONCE_LEN];
        fixed_length_nonce.copy_from_slice(&length_nonce_prefix[..NONCE_LEN]);
        let length_key = if authenticated_length {
            let key = vmess_aead_kdf16(&length_key_source, b"auth_len").to_vec();
            Some(match cipher {
                VmessCipher::Chacha20Poly1305 => derive_chacha20_poly1305_key(&key),
                _ => key,
            })
        } else {
            None
        };
        let key = match cipher {
            VmessCipher::Chacha20Poly1305 => derive_chacha20_poly1305_key(&key),
            _ => key,
        };
        Ok(Self {
            key,
            length_key,
            nonce_prefix: fixed_nonce,
            length_nonce_prefix: fixed_length_nonce,
            nonce_counter: 0,
            cipher,
            authenticated_length,
            size_mask: chunk_masking.then(|| ShakeSizeMask::new(&nonce_prefix)),
            global_padding,
            pending_padding_len: 0,
            chunks_since_rekey: 0,
        })
    }

    pub fn seal_chunk(&mut self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if payload.len() + GCM_TAG_LEN + 63 > MAX_BODY_PAYLOAD_SIZE {
            return Err(Error::Protocol("vmess body chunk too large"));
        }

        let nonce_payload = self.current_nonce();
        let enc_payload = aead_encrypt_with_nonce(&self.key, &nonce_payload, payload, self.cipher)?;
        if enc_payload.len() > u16::MAX as usize {
            return Err(Error::Protocol("vmess body chunk too large"));
        }
        let padding_len = self.next_padding_len();
        let len_bytes = self.seal_length(enc_payload.len() + padding_len)?;

        let mut out = Vec::with_capacity(len_bytes.len() + enc_payload.len() + padding_len);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(&enc_payload);
        out.resize(out.len() + padding_len, 0);

        self.finish_chunk();
        Ok(out)
    }

    pub fn seal_plain_chunk(&mut self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if payload.len() > u16::MAX as usize {
            return Err(Error::Protocol("vmess body chunk too large"));
        }
        let padding_len = self.next_padding_len();
        let len_bytes = self.seal_length(payload.len() + padding_len)?;
        let mut out = Vec::with_capacity(len_bytes.len() + payload.len() + padding_len);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(payload);
        out.resize(out.len() + padding_len, 0);
        self.finish_chunk();
        Ok(out)
    }

    pub fn open_length(&mut self, encrypted_len: &[u8]) -> Result<usize, Error> {
        self.pending_padding_len = self.next_padding_len();
        let payload_len = if self.authenticated_length {
            if encrypted_len.len() != 18 {
                return Err(Error::Protocol(
                    "vmess body authenticated length has invalid size",
                ));
            }
            let key = self.length_key.as_deref().ok_or(Error::Protocol(
                "vmess body authenticated length key missing",
            ))?;
            let nonce = self.current_length_nonce();
            let len_plain = aead_decrypt_with_nonce(key, &nonce, encrypted_len, self.cipher)?;
            if len_plain.len() != 2 {
                return Err(Error::Protocol("vmess body authenticated length invalid"));
            }
            u16::from_be_bytes([len_plain[0], len_plain[1]]) as usize
        } else {
            if encrypted_len.len() != 2 {
                return Err(Error::Protocol("vmess body length frame has invalid size"));
            }
            let mut size = u16::from_be_bytes(
                encrypted_len[..2]
                    .try_into()
                    .map_err(|_| Error::Protocol("vmess body chunk len decode"))?,
            );
            if let Some(mask) = &mut self.size_mask {
                size ^= mask.next();
            }
            size as usize
        };

        if payload_len > MAX_BODY_PAYLOAD_SIZE + 63 {
            return Err(Error::Protocol("vmess body chunk exceeds runtime limit"));
        }

        Ok(payload_len)
    }

    pub fn open_payload(
        &mut self,
        expected_len: usize,
        encrypted_payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let padding_len = self.pending_padding_len;
        self.pending_padding_len = 0;
        if expected_len < padding_len {
            return Err(Error::Protocol("vmess body padding exceeds frame size"));
        }
        let payload_len = expected_len - padding_len;
        if encrypted_payload.len() != expected_len {
            return Err(Error::Protocol("vmess body payload frame has invalid size"));
        }

        let nonce_payload = self.current_nonce();
        let payload_plain = aead_decrypt_with_nonce(
            &self.key,
            &nonce_payload,
            &encrypted_payload[..payload_len],
            self.cipher,
        )?;
        self.finish_chunk();
        Ok(payload_plain)
    }

    pub fn open_plain_payload(
        &mut self,
        expected_len: usize,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let padding_len = self.pending_padding_len;
        self.pending_padding_len = 0;
        if expected_len < padding_len {
            return Err(Error::Protocol("vmess body padding exceeds frame size"));
        }
        let payload_len = expected_len - padding_len;
        if payload.len() != expected_len {
            return Err(Error::Protocol(
                "vmess plain body payload frame has invalid size",
            ));
        }
        self.finish_chunk();
        Ok(payload[..payload_len].to_vec())
    }

    pub fn length_frame_size(&self) -> usize {
        if self.authenticated_length {
            18
        } else {
            2
        }
    }

    fn seal_length(&mut self, payload_len: usize) -> Result<Vec<u8>, Error> {
        let mut size = payload_len as u16;
        if let Some(mask) = &mut self.size_mask {
            size ^= mask.next();
        }
        let len_bytes = size.to_be_bytes();
        if !self.authenticated_length {
            return Ok(len_bytes.to_vec());
        }

        let key = self.length_key.as_deref().ok_or(Error::Protocol(
            "vmess body authenticated length key missing",
        ))?;
        let nonce = self.current_length_nonce();
        aead_encrypt_with_nonce(key, &nonce, &len_bytes, self.cipher)
    }

    fn next_padding_len(&mut self) -> usize {
        if self.global_padding {
            self.size_mask
                .as_mut()
                .map(|mask| (mask.next() % 64) as usize)
                .unwrap_or(0)
        } else {
            0
        }
    }

    fn current_nonce(&self) -> [u8; NONCE_LEN] {
        let mut nonce = self.nonce_prefix;
        nonce[..2].copy_from_slice(&(self.nonce_counter as u16).to_be_bytes());
        nonce
    }

    fn current_length_nonce(&self) -> [u8; NONCE_LEN] {
        let mut nonce = self.length_nonce_prefix;
        nonce[..2].copy_from_slice(&(self.nonce_counter as u16).to_be_bytes());
        nonce
    }

    fn finish_chunk(&mut self) {
        self.nonce_counter = self.nonce_counter.wrapping_add(1);
        self.chunks_since_rekey += 1;
        if self.chunks_since_rekey >= REKEY_INTERVAL {
            self.rekey();
        }
    }

    fn rekey(&mut self) {
        let counter_bytes = self.chunks_since_rekey.to_be_bytes();
        self.key = hkdf_expand(
            &self.key,
            b"VMess AEAD Body Key",
            &counter_bytes,
            self.cipher.key_len(),
        );
        self.chunks_since_rekey = 0;
    }
}

struct ShakeSizeMask {
    reader: Mutex<Box<dyn XofReader + Send>>,
}

impl ShakeSizeMask {
    fn new(seed: &[u8]) -> Self {
        let mut hasher = Shake128::default();
        hasher.update(seed);
        Self {
            reader: Mutex::new(Box::new(hasher.finalize_xof())),
        }
    }

    fn next(&mut self) -> u16 {
        let mut buf = [0_u8; 2];
        self.reader
            .lock()
            .expect("vmess shake mask poisoned")
            .read(&mut buf);
        u16::from_be_bytes(buf)
    }
}

fn derive_chacha20_poly1305_key(input: &[u8]) -> Vec<u8> {
    let first = md5::compute(input);
    let second = md5::compute(first.0);
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&first.0);
    key.extend_from_slice(&second.0);
    key
}

fn aead_encrypt_with_nonce(
    key: &[u8],
    nonce: &[u8; NONCE_LEN],
    plaintext: &[u8],
    cipher: VmessCipher,
) -> Result<Vec<u8>, Error> {
    let unbound = UnboundKey::new(cipher.aead_algorithm(), key)
        .map_err(|_| Error::Protocol("vmess invalid aead key"))?;
    let ring_nonce = Nonce::assume_unique_for_key(*nonce);
    let mut sealing_key = SealingKey::new(unbound, SingleNonce::new(ring_nonce));
    let mut buf = plaintext.to_vec();
    buf.reserve(GCM_TAG_LEN);
    sealing_key
        .seal_in_place_append_tag(Aad::empty(), &mut buf)
        .map_err(|_| Error::Protocol("vmess body aead encryption failed"))?;
    Ok(buf)
}

fn aead_decrypt_with_nonce(
    key: &[u8],
    nonce: &[u8; NONCE_LEN],
    ciphertext: &[u8],
    cipher: VmessCipher,
) -> Result<Vec<u8>, Error> {
    let unbound = UnboundKey::new(cipher.aead_algorithm(), key)
        .map_err(|_| Error::Protocol("vmess invalid aead key"))?;
    let ring_nonce = Nonce::assume_unique_for_key(*nonce);
    let mut opening_key = OpeningKey::new(unbound, SingleNonce::new(ring_nonce));
    let mut in_out = ciphertext.to_vec();
    let plaintext = opening_key
        .open_in_place(Aad::empty(), &mut in_out)
        .map_err(|_| Error::Protocol("vmess body aead decryption failed"))?;
    Ok(plaintext.to_vec())
}

struct SingleNonce([u8; NONCE_LEN]);

impl SingleNonce {
    fn new(nonce: Nonce) -> Self {
        let mut buf = [0u8; NONCE_LEN];
        buf.copy_from_slice(nonce.as_ref());
        Self(buf)
    }
}

impl NonceSequence for SingleNonce {
    fn advance(&mut self) -> Result<Nonce, ring::error::Unspecified> {
        Ok(Nonce::assume_unique_for_key(self.0))
    }
}

fn hkdf_expand(secret: &[u8], salt_label: &[u8], info: &[u8], len: usize) -> Vec<u8> {
    let salt = Salt::new(HKDF_SHA256, salt_label);
    let prk = salt.extract(secret);
    let info_slices = [info];
    let okm = prk.expand(&info_slices, HkdfLen(len)).expect("hkdf expand");
    let mut buf = vec![0u8; len];
    okm.fill(&mut buf).expect("hkdf fill");
    buf
}

fn xray_kdf16(key: &[u8], path: &[&[u8]]) -> [u8; 16] {
    let out = xray_kdf(key, path);
    out[..16].try_into().expect("xray kdf16")
}

fn xray_kdf(key: &[u8], path: &[&[u8]]) -> Vec<u8> {
    let mut keys = Vec::with_capacity(path.len() + 1);
    keys.push(VMESS_AEAD_KDF_SALT);
    keys.extend_from_slice(path);
    xray_hash_layer(key, &keys)
}

fn bytes_to_path(bytes: &[u8]) -> &[u8] {
    bytes
}

fn aes_128_gcm_seal(
    key: &[u8; 16],
    nonce: &[u8],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, Error> {
    let nonce: [u8; NONCE_LEN] = nonce
        .try_into()
        .map_err(|_| Error::Protocol("vmess xray invalid nonce length"))?;
    let unbound = UnboundKey::new(&ring::aead::AES_128_GCM, key)
        .map_err(|_| Error::Protocol("vmess xray invalid aead key"))?;
    let mut sealing_key = SealingKey::new(
        unbound,
        SingleNonce::new(Nonce::assume_unique_for_key(nonce)),
    );
    let mut out = plaintext.to_vec();
    out.reserve(GCM_TAG_LEN);
    sealing_key
        .seal_in_place_append_tag(Aad::from(aad), &mut out)
        .map_err(|_| Error::Protocol("vmess xray aead seal failed"))?;
    Ok(out)
}

fn aes_128_gcm_open(
    key: &[u8; 16],
    nonce: &[u8],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>, Error> {
    let nonce: [u8; NONCE_LEN] = nonce
        .try_into()
        .map_err(|_| Error::Protocol("vmess xray invalid nonce length"))?;
    let unbound = UnboundKey::new(&ring::aead::AES_128_GCM, key)
        .map_err(|_| Error::Protocol("vmess xray invalid aead key"))?;
    let mut opening_key = OpeningKey::new(
        unbound,
        SingleNonce::new(Nonce::assume_unique_for_key(nonce)),
    );
    let mut in_out = ciphertext.to_vec();
    let plain = opening_key
        .open_in_place(Aad::from(aad), &mut in_out)
        .map_err(|_| Error::Protocol("vmess xray aead open failed"))?;
    Ok(plain.to_vec())
}

pub(crate) fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) mod hex {
    pub fn encode(bytes: &[u8; 16]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

fn xray_hash_layer(data: &[u8], keys: &[&[u8]]) -> Vec<u8> {
    debug_assert!(!keys.is_empty());
    if keys.len() == 1 {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, keys[0]);
        return ring::hmac::sign(&key, data).as_ref().to_vec();
    }

    xray_hmac_with_hash(
        |input| xray_hash_layer(input, &keys[..keys.len() - 1]),
        keys[keys.len() - 1],
        data,
    )
}

fn xray_hmac_with_hash<F>(hash: F, key: &[u8], data: &[u8]) -> Vec<u8>
where
    F: Fn(&[u8]) -> Vec<u8>,
{
    const BLOCK_LEN: usize = 64;

    let mut block_key = if key.len() > BLOCK_LEN {
        hash(key)
    } else {
        key.to_vec()
    };
    block_key.resize(BLOCK_LEN, 0);

    let mut ipad = vec![0x36_u8; BLOCK_LEN];
    let mut opad = vec![0x5c_u8; BLOCK_LEN];
    for (idx, key_byte) in block_key.iter().enumerate() {
        ipad[idx] ^= key_byte;
        opad[idx] ^= key_byte;
    }

    ipad.extend_from_slice(data);
    let inner_hash = hash(&ipad);
    opad.extend_from_slice(&inner_hash);
    hash(&opad)
}
