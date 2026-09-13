use quinn_proto::crypto::{CryptoError, Keys};
use quinn_proto::{ConnectionId, Side};
use ring::{aead, hkdf};

mod protection;

const INITIAL_SALT_DRAFT_29: [u8; 20] = [
    0xaf, 0xbf, 0xec, 0x28, 0x99, 0x93, 0xd2, 0x4c, 0x9e, 0x97, 0x86, 0xf1, 0x9c, 0x61, 0x11, 0xe0,
    0x43, 0x90, 0xa8, 0x99,
];
const INITIAL_SALT_V1: [u8; 20] = [
    0x38, 0x76, 0x2c, 0xf7, 0xf5, 0x59, 0x34, 0xb3, 0x4d, 0x17, 0x9a, 0xe6, 0xa4, 0xc8, 0x0c, 0xad,
    0xcc, 0xbb, 0x7f, 0x0a,
];

const RETRY_KEY_DRAFT_29: [u8; 16] = [
    0xcc, 0xce, 0x18, 0x7e, 0xd0, 0x9a, 0x09, 0xd0, 0x57, 0x28, 0x15, 0x5a, 0x6c, 0xb9, 0x6b, 0xe1,
];
const RETRY_NONCE_DRAFT_29: [u8; 12] = [
    0xe5, 0x49, 0x30, 0xf9, 0x7f, 0x21, 0x36, 0xf0, 0x53, 0x0a, 0x8c, 0x1c,
];
const RETRY_KEY_V1: [u8; 16] = [
    0xbe, 0x0c, 0x69, 0x0b, 0x9f, 0x66, 0x57, 0x5a, 0x1d, 0x76, 0x6b, 0x54, 0xe3, 0x68, 0xc8, 0x4e,
];
const RETRY_NONCE_V1: [u8; 12] = [
    0x46, 0x15, 0x99, 0xd3, 0x5d, 0x63, 0x2b, 0xf2, 0x23, 0x98, 0x25, 0xbb,
];

const TAG_LEN: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CipherSuite {
    Aes128GcmSha256,
    Aes256GcmSha384,
    ChaCha20Poly1305Sha256,
}

impl CipherSuite {
    pub(super) fn from_name(name: &str) -> Option<Self> {
        match name {
            "TLS_AES_128_GCM_SHA256" => Some(Self::Aes128GcmSha256),
            "TLS_AES_256_GCM_SHA384" => Some(Self::Aes256GcmSha384),
            "TLS_CHACHA20_POLY1305_SHA256" => Some(Self::ChaCha20Poly1305Sha256),
            _ => None,
        }
    }

    pub(super) fn secret_len(self) -> usize {
        match self {
            Self::Aes128GcmSha256 | Self::ChaCha20Poly1305Sha256 => 32,
            Self::Aes256GcmSha384 => 48,
        }
    }

    fn hkdf(self) -> hkdf::Algorithm {
        match self {
            Self::Aes128GcmSha256 | Self::ChaCha20Poly1305Sha256 => hkdf::HKDF_SHA256,
            Self::Aes256GcmSha384 => hkdf::HKDF_SHA384,
        }
    }

    fn packet_algorithm(self) -> &'static aead::Algorithm {
        match self {
            Self::Aes128GcmSha256 => &aead::AES_128_GCM,
            Self::Aes256GcmSha384 => &aead::AES_256_GCM,
            Self::ChaCha20Poly1305Sha256 => &aead::CHACHA20_POLY1305,
        }
    }

    fn header_algorithm(self) -> &'static aead::quic::Algorithm {
        match self {
            Self::Aes128GcmSha256 => &aead::quic::AES_128,
            Self::Aes256GcmSha384 => &aead::quic::AES_256,
            Self::ChaCha20Poly1305Sha256 => &aead::quic::CHACHA20,
        }
    }

    fn confidentiality_limit(self) -> u64 {
        match self {
            Self::Aes128GcmSha256 | Self::Aes256GcmSha384 => 1 << 23,
            Self::ChaCha20Poly1305Sha256 => u64::MAX,
        }
    }

    fn integrity_limit(self) -> u64 {
        match self {
            Self::Aes128GcmSha256 | Self::Aes256GcmSha384 => 1 << 52,
            Self::ChaCha20Poly1305Sha256 => 1 << 36,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Version {
    Draft29,
    V1,
}

impl Version {
    pub(super) fn from_wire(version: u32) -> Result<Self, quinn_proto::crypto::UnsupportedVersion> {
        match version {
            0xff00_001d..=0xff00_0020 => Ok(Self::Draft29),
            0x0000_0001 | 0xff00_0021..=0xff00_0022 => Ok(Self::V1),
            _ => Err(quinn_proto::crypto::UnsupportedVersion),
        }
    }

    fn initial_salt(self) -> &'static [u8] {
        match self {
            Self::Draft29 => &INITIAL_SALT_DRAFT_29,
            Self::V1 => &INITIAL_SALT_V1,
        }
    }

    fn retry_material(self) -> (&'static [u8; 16], &'static [u8; 12]) {
        match self {
            Self::Draft29 => (&RETRY_KEY_DRAFT_29, &RETRY_NONCE_DRAFT_29),
            Self::V1 => (&RETRY_KEY_V1, &RETRY_NONCE_V1),
        }
    }
}

pub(super) fn initial_keys(
    version: Version,
    dst_cid: &ConnectionId,
    side: Side,
) -> Result<Keys, CryptoError> {
    let initial = hkdf::Salt::new(hkdf::HKDF_SHA256, version.initial_salt()).extract(dst_cid);
    let client = expand(&initial, b"client in", 32)?;
    let server = expand(&initial, b"server in", 32)?;
    let (local, remote) = if side.is_server() {
        (server, client)
    } else {
        (client, server)
    };
    keys_from_pair(version, CipherSuite::Aes128GcmSha256, &local, &remote)
}

pub(super) fn keys_from_pair(
    version: Version,
    suite: CipherSuite,
    local: &[u8],
    remote: &[u8],
) -> Result<Keys, CryptoError> {
    protection::keys_from_pair(version, suite, local, remote)
}

pub(super) fn next_secret(
    version: Version,
    suite: CipherSuite,
    secret: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let _ = version;
    expand(
        &hkdf::Prk::new_less_safe(suite.hkdf(), secret),
        b"quic ku",
        secret.len(),
    )
}

pub(super) fn retry_tag(version: Version, orig_dst_cid: &ConnectionId, packet: &[u8]) -> [u8; 16] {
    let (key, nonce) = version.retry_material();
    let mut aad = Vec::with_capacity(1 + orig_dst_cid.len() + packet.len());
    aad.push(orig_dst_cid.len() as u8);
    aad.extend_from_slice(orig_dst_cid);
    aad.extend_from_slice(packet);
    let key = aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_128_GCM, key).unwrap());
    let tag = key
        .seal_in_place_separate_tag(
            aead::Nonce::assume_unique_for_key(*nonce),
            aead::Aad::from(aad),
            &mut [],
        )
        .unwrap();
    let mut result = [0; 16];
    result.copy_from_slice(tag.as_ref());
    result
}

pub(super) fn valid_retry(
    version: Version,
    orig_dst_cid: &ConnectionId,
    header: &[u8],
    payload: &[u8],
) -> bool {
    let Some(tag_start) = payload.len().checked_sub(TAG_LEN) else {
        return false;
    };
    let mut packet = Vec::with_capacity(1 + orig_dst_cid.len() + header.len() + payload.len());
    packet.push(orig_dst_cid.len() as u8);
    packet.extend_from_slice(orig_dst_cid);
    packet.extend_from_slice(header);
    let tag_start = packet.len() + tag_start;
    packet.extend_from_slice(payload);
    let (key, nonce) = version.retry_material();
    let key = aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_128_GCM, key).unwrap());
    let (aad, tag) = packet.split_at_mut(tag_start);
    key.open_in_place(
        aead::Nonce::assume_unique_for_key(*nonce),
        aead::Aad::from(aad),
        tag,
    )
    .is_ok()
}

struct OutputLength(usize);

impl hkdf::KeyType for OutputLength {
    fn len(&self) -> usize {
        self.0
    }
}

fn expand(prk: &hkdf::Prk, label: &[u8], len: usize) -> Result<Vec<u8>, CryptoError> {
    let full_label_len = b"tls13 ".len() + label.len();
    let mut info = Vec::with_capacity(4 + full_label_len);
    info.extend_from_slice(&(len as u16).to_be_bytes());
    info.push(full_label_len.try_into().map_err(|_| CryptoError)?);
    info.extend_from_slice(b"tls13 ");
    info.extend_from_slice(label);
    info.push(0);
    let parts = [info.as_slice()];
    let mut output = vec![0; len];
    prk.expand(&parts, OutputLength(len))
        .map_err(|_| CryptoError)?
        .fill(&mut output)
        .map_err(|_| CryptoError)?;
    Ok(output)
}
