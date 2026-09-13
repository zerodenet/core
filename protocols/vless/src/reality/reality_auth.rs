use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::hkdf::{Salt, HKDF_SHA256};
use x25519_dalek::{PublicKey, StaticSecret};

use std::time::{SystemTime, UNIX_EPOCH};

/// Custom error type for REALITY cryptographic operations
#[derive(Debug)]
pub enum CryptoError {
    InvalidKeyLength,
    InvalidNonceLength,
    InvalidCiphertextLength,
    EncryptionFailed,
    DecryptionFailed,
    EcdhFailed,
    HkdfFailed,
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptoError::InvalidKeyLength => write!(f, "Invalid key length"),
            CryptoError::InvalidNonceLength => write!(f, "Invalid nonce length"),
            CryptoError::InvalidCiphertextLength => write!(f, "Invalid ciphertext length"),
            CryptoError::EncryptionFailed => write!(f, "Encryption failed"),
            CryptoError::DecryptionFailed => write!(f, "Decryption failed"),
            CryptoError::EcdhFailed => write!(f, "ECDH key exchange failed"),
            CryptoError::HkdfFailed => write!(f, "HKDF derivation failed"),
        }
    }
}

impl std::error::Error for CryptoError {}

impl From<CryptoError> for std::io::Error {
    fn from(err: CryptoError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
    }
}

/// Performs X25519 ECDH key exchange
///
/// # Arguments
/// * `private_key` - 32-byte X25519 private key
/// * `public_key` - 32-byte X25519 public key
///
/// # Returns
/// 32-byte shared secret
pub fn perform_ecdh(
    private_key: &[u8; 32],
    public_key: &[u8; 32],
) -> Result<[u8; 32], CryptoError> {
    let my_private_key = StaticSecret::from(*private_key);
    let peer_public_key = PublicKey::from(*public_key);
    let shared = my_private_key.diffie_hellman(&peer_public_key);
    if !shared.was_contributory() {
        return Err(CryptoError::EcdhFailed);
    }
    Ok(shared.to_bytes())
}

/// Derives authentication key using HKDF-SHA256
///
/// # Arguments
/// * `shared_secret` - 32-byte shared secret from ECDH
/// * `salt` - Salt bytes (must be exactly 20 bytes, from ClientHello.Random[0..20])
/// * `info` - Context string (should be b"REALITY")
///
/// # Returns
/// 32-byte derived authentication key
///
/// # Panics
/// Panics if salt is not exactly 20 bytes.
pub fn derive_auth_key(
    shared_secret: &[u8; 32],
    salt: &[u8],
    info: &[u8],
) -> Result<[u8; 32], CryptoError> {
    debug_assert_eq!(salt.len(), 20, "salt must be exactly 20 bytes");
    let salt = Salt::new(HKDF_SHA256, salt);
    let prk = salt.extract(shared_secret);
    let info_pieces = [info];
    let okm = prk
        .expand(&info_pieces, HKDF_SHA256)
        .map_err(|_| CryptoError::HkdfFailed)?;
    let mut auth_key = [0u8; 32];
    okm.fill(&mut auth_key)
        .map_err(|_| CryptoError::HkdfFailed)?;
    Ok(auth_key)
}

/// Encrypts SessionId using AES-256-GCM
///
/// # Arguments
/// * `plaintext` - 16-byte plaintext (first 16 bytes of SessionId)
/// * `auth_key` - 32-byte authentication key
/// * `nonce` - 12-byte nonce (ClientHello.Random[20..32])
/// * `aad` - Additional authenticated data (entire ClientHello)
///
/// # Returns
/// 32-byte result (16 bytes ciphertext + 16 bytes GCM tag)
///
/// # Panics
/// Panics if nonce is not exactly 12 bytes.
pub fn encrypt_session_id(
    plaintext: &[u8; 16],
    auth_key: &[u8; 32],
    nonce: &[u8],
    aad: &[u8],
) -> Result<[u8; 32], CryptoError> {
    debug_assert_eq!(nonce.len(), 12, "nonce must be exactly 12 bytes");
    let unbound_key =
        UnboundKey::new(&AES_256_GCM, auth_key).map_err(|_| CryptoError::EncryptionFailed)?;
    let sealing_key = LessSafeKey::new(unbound_key);

    let nonce_obj =
        Nonce::try_assume_unique_for_key(nonce).map_err(|_| CryptoError::InvalidNonceLength)?;

    let aad_obj = Aad::from(aad);

    // aws-lc-rs requires in-place encryption
    let mut in_out = plaintext.to_vec();
    sealing_key
        .seal_in_place_append_tag(nonce_obj, aad_obj, &mut in_out)
        .map_err(|_| CryptoError::EncryptionFailed)?;

    if in_out.len() != 32 {
        return Err(CryptoError::EncryptionFailed);
    }

    let mut result = [0u8; 32];
    result.copy_from_slice(&in_out);
    Ok(result)
}

/// Decrypts SessionId using AES-256-GCM
///
/// # Arguments
/// * `ciphertext_and_tag` - 32-byte encrypted data (16 ciphertext + 16 tag)
/// * `auth_key` - 32-byte authentication key
/// * `nonce` - 12-byte nonce (ClientHello.Random[20..32])
/// * `aad` - Additional authenticated data (entire ClientHello)
///
/// # Returns
/// 16-byte decrypted plaintext
///
/// # Panics
/// Panics if nonce is not exactly 12 bytes.
pub fn decrypt_session_id(
    ciphertext_and_tag: &[u8; 32],
    auth_key: &[u8; 32],
    nonce: &[u8],
    aad: &[u8],
) -> Result<[u8; 16], CryptoError> {
    debug_assert_eq!(nonce.len(), 12, "nonce must be exactly 12 bytes");
    let unbound_key =
        UnboundKey::new(&AES_256_GCM, auth_key).map_err(|_| CryptoError::DecryptionFailed)?;
    let opening_key = LessSafeKey::new(unbound_key);

    let nonce_obj =
        Nonce::try_assume_unique_for_key(nonce).map_err(|_| CryptoError::InvalidNonceLength)?;

    let aad_obj = Aad::from(aad);

    // aws-lc-rs requires in-place decryption
    let mut in_out = ciphertext_and_tag.to_vec();
    let plaintext = opening_key
        .open_in_place(nonce_obj, aad_obj, &mut in_out)
        .map_err(|_| CryptoError::DecryptionFailed)?;

    if plaintext.len() != 16 {
        return Err(CryptoError::DecryptionFailed);
    }

    let mut result = [0u8; 16];
    result.copy_from_slice(plaintext);
    Ok(result)
}

/// Creates a REALITY SessionId (test helper)
pub fn create_session_id(version: [u8; 3], timestamp: u32, short_id: &[u8; 8]) -> [u8; 32] {
    let mut session_id = [0u8; 32];
    session_id[0] = version[0]; // Major version
    session_id[1] = version[1]; // Minor version
    session_id[2] = version[2]; // Patch version
                                // session_id[3] = 0 (reserved)
    session_id[4..8].copy_from_slice(&timestamp.to_be_bytes());
    session_id[8..16].copy_from_slice(short_id);
    // session_id[16..32] remain zeros
    session_id
}

/// Gets current Unix timestamp (test helper)
pub fn get_current_timestamp() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as u32
}
