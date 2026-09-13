// TLS 1.3 AEAD encryption/decryption using ring.
// Record framing is handled by reality_records.rs.

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey};
use std::io::{self, Error, ErrorKind};

use crate::cipher::CipherSuite;
use crate::common::strip_content_type_with_padding;

/// AEAD key for TLS 1.3 encryption/decryption.
///
/// Wraps aws-lc-rs LessSafeKey and provides a cleaner API.
/// Create once per connection direction and reuse for all records.
pub struct AeadKey(LessSafeKey);

impl AeadKey {
    /// Create a new AEAD key from raw key bytes.
    pub fn new(cipher_suite: CipherSuite, key: &[u8]) -> io::Result<Self> {
        let algorithm = cipher_suite.algorithm();
        let expected_len = cipher_suite.key_len();

        if key.len() != expected_len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Invalid key length for {:?}: {} (expected {})",
                    cipher_suite,
                    key.len(),
                    expected_len
                ),
            ));
        }

        let unbound = UnboundKey::new(algorithm, key)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid key: {:?}", e)))?;

        Ok(Self(LessSafeKey::new(unbound)))
    }

    /// Encrypt in-place, appending 16-byte auth tag.
    ///
    /// The buffer is modified: plaintext -> ciphertext || tag
    #[inline]
    pub fn seal_in_place(
        &self,
        buf: &mut Vec<u8>,
        iv: &[u8],
        seq: u64,
        aad: &[u8],
    ) -> io::Result<()> {
        let nonce = Self::make_nonce(iv, seq)?;
        self.0
            .seal_in_place_append_tag(nonce, Aad::from(aad), buf)
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Encryption failed: {:?}", e),
                )
            })
    }

    /// Encrypt with copy, returning new Vec containing ciphertext || tag.
    ///
    /// Use this for small buffers where allocation overhead doesn't matter.
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))] // Used by test helpers
    pub fn seal(&self, plaintext: &[u8], iv: &[u8], seq: u64, aad: &[u8]) -> io::Result<Vec<u8>> {
        let mut buf = plaintext.to_vec();
        self.seal_in_place(&mut buf, iv, seq, aad)?;
        Ok(buf)
    }

    /// Decrypt in-place on a mutable slice, returning the plaintext portion.
    ///
    /// This is the zero-allocation decryption API. The slice is decrypted in-place
    /// and a sub-slice containing only the plaintext (without the auth tag) is returned.
    ///
    /// # Arguments
    /// * `buf` - Mutable slice containing ciphertext + auth tag
    /// * `iv` - 12-byte IV/nonce base
    /// * `seq` - Sequence number to XOR with IV
    /// * `aad` - Additional authenticated data
    ///
    /// # Returns
    /// Sub-slice of `buf` containing only the decrypted plaintext
    #[inline]
    pub fn open_in_place_slice<'a>(
        &self,
        buf: &'a mut [u8],
        iv: &[u8],
        seq: u64,
        aad: &[u8],
    ) -> io::Result<&'a mut [u8]> {
        let nonce = Self::make_nonce(iv, seq)?;
        self.0
            .open_in_place(nonce, Aad::from(aad), buf)
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Decryption failed: {:?}", e),
                )
            })
    }

    /// Decrypt with copy, returning new Vec containing plaintext.
    ///
    /// Used for handshake decryption and tests where allocation is acceptable.
    #[inline]
    pub fn open(&self, ciphertext: &[u8], iv: &[u8], seq: u64, aad: &[u8]) -> io::Result<Vec<u8>> {
        let mut buf = ciphertext.to_vec();
        let plaintext = self.open_in_place_slice(&mut buf, iv, seq, aad)?;
        let plaintext_len = plaintext.len();
        buf.truncate(plaintext_len);
        Ok(buf)
    }

    /// Construct TLS 1.3 nonce: IV XOR sequence_number
    fn make_nonce(iv: &[u8], seq: u64) -> io::Result<Nonce> {
        if iv.len() != 12 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Invalid IV length: {} (expected 12)", iv.len()),
            ));
        }

        let mut nonce_bytes = [0u8; 12];
        nonce_bytes.copy_from_slice(iv);

        // XOR last 8 bytes with sequence number (big-endian)
        let seq_bytes = seq.to_be_bytes();
        for i in 0..8 {
            nonce_bytes[4 + i] ^= seq_bytes[i];
        }

        Nonce::try_assume_unique_for_key(&nonce_bytes)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, format!("Invalid nonce: {:?}", e)))
    }
}

/// Decrypt a TLS 1.3 handshake message.
///
/// Builds the AAD from the record length and decrypts.
/// Returns plaintext with content type trailer stripped.
pub fn decrypt_handshake_message(
    cipher_suite: CipherSuite,
    key: &[u8],
    iv: &[u8],
    seq: u64,
    ciphertext: &[u8],
    record_len: u16,
) -> io::Result<Vec<u8>> {
    // Build AAD: TLS record header
    let aad = [
        0x17, // ApplicationData
        0x03,
        0x03, // TLS 1.2 version
        (record_len >> 8) as u8,
        (record_len & 0xff) as u8,
    ];

    let aead_key = AeadKey::new(cipher_suite, key)?;
    let mut plaintext = aead_key.open(ciphertext, iv, seq, &aad)?;

    // Strip content type and optional padding (external implementations may pad)
    if strip_content_type_with_padding(&mut plaintext)? != crate::common::CONTENT_TYPE_HANDSHAKE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected TLS handshake content",
        ));
    }

    Ok(plaintext)
}

pub fn encrypt_tls13_record(
    cipher_suite: CipherSuite,
    key: &[u8],
    iv: &[u8],
    seq: u64,
    plaintext: &[u8],
    aad: &[u8],
) -> io::Result<Vec<u8>> {
    AeadKey::new(cipher_suite, key)?.seal(plaintext, iv, seq, aad)
}

pub fn decrypt_tls13_record(
    cipher_suite: CipherSuite,
    key: &[u8],
    iv: &[u8],
    seq: u64,
    ciphertext: &[u8],
    aad: &[u8],
) -> io::Result<Vec<u8>> {
    AeadKey::new(cipher_suite, key)?.open(ciphertext, iv, seq, aad)
}
