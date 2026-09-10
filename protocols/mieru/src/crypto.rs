// Mieru protocol cryptography — crypto.rs
//
// Key derivation:
//   1. hashedPassword = SHA-256(password || 0x00 || username)
//   2. timeSalt      = SHA-256(uint64_be(unix_time_rounded_to_2min))
//   3. key           = PBKDF2(hashedPassword, timeSalt, 64 iter, 32 bytes, HMAC-SHA256)
//
// Nonce (matching upstream mieru v3.x):
//   - 24-byte XChaCha20-Poly1305 nonce
//   - Initial: 24 random bytes from ring::rand
//   - Increment: full 24-byte big-endian, carry from byte 23 backward
//   - User hint (nonce acceleration): last N bytes =
//       SHA-256(username || nonce[..16])[..N]

use alloc::vec::Vec;
use sha2::Digest;
use zero_core::Error;

/// Number of trailing nonce bytes replaced with user hint.
/// Original mieru uses 4 by default.
pub const USER_HINT_LEN: usize = 4;

/// Nonce pattern types matching upstream mieru.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NoncePattern {
    /// No modification — full 24 random bytes.
    #[default]
    Random,
    /// First N bytes rewritten to printable ASCII (0x20-0x7E).
    Printable { min_len: usize, max_len: usize },
    /// First N bytes rewritten to common 64-char subset
    /// (A-Z, a-z, 0-9, '+', '/').
    PrintableSubset { min_len: usize, max_len: usize },
    /// First N bytes replaced with a random hex string from the set.
    Fixed { prefixes: Vec<Vec<u8>> },
}

/// Configuration for nonce generation (patterns + user hint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonceConfig {
    pub pattern: NoncePattern,
    pub username: Option<String>,
}

impl Default for NonceConfig {
    fn default() -> Self {
        Self {
            pattern: NoncePattern::Random,
            username: None,
        }
    }
}

/// Mieru cipher state for one direction.
#[derive(Clone)]
pub struct MieruCipher {
    key: [u8; 32],
    username: Option<String>,
    /// Full 24-byte nonce — incremented as big-endian after each use.
    nonce: [u8; 24],
    /// Whether encrypted data should include the nonce prefix.
    include_nonce: bool,
}

impl MieruCipher {
    /// Create a new cipher with a random 24-byte initial nonce,
    /// optionally applying a nonce pattern and user hint.
    pub fn new(key: &[u8; 32]) -> Self {
        Self::with_config(key, &NonceConfig::default())
    }

    /// Create a new cipher with nonce patterns applied.
    pub fn with_config(key: &[u8; 32], config: &NonceConfig) -> Self {
        let mut nonce = [0u8; 24];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut nonce)
            .expect("ring::rand must not fail");

        // Apply nonce pattern
        apply_nonce_pattern(&mut nonce, &config.pattern);

        // Apply user hint
        if let Some(ref username) = config.username {
            let prefix: [u8; 16] = nonce[..16].try_into().unwrap();
            let hint = Self::compute_user_hint(username, &prefix);
            let start = 24 - USER_HINT_LEN;
            nonce[start..].copy_from_slice(&hint);
        }

        Self {
            key: *key,
            username: config.username.clone(),
            nonce,
            include_nonce: true,
        }
    }

    /// Create a cipher with an explicit nonce.
    pub fn with_nonce(key: &[u8; 32], nonce: [u8; 24]) -> Self {
        Self {
            key: *key,
            username: None,
            nonce,
            include_nonce: true,
        }
    }

    /// Create a cipher for the response direction.
    pub fn new_response(key: &[u8; 32]) -> Self {
        Self::new(key)
    }

    /// Set whether nonce is included in encrypted output.
    pub fn set_include_nonce(&mut self, include: bool) {
        self.include_nonce = include;
    }

    /// Get the current nonce value (before next encryption).
    pub fn current_nonce(&self) -> &[u8; 24] {
        &self.nonce
    }

    pub(crate) fn username(&self) -> &str {
        self.username.as_deref().unwrap_or("")
    }

    /// Compute the user hint for nonce acceleration.
    /// hint = SHA-256(username || nonce[..16])[..USER_HINT_LEN]
    pub fn compute_user_hint(username: &str, nonce_prefix: &[u8; 16]) -> [u8; USER_HINT_LEN] {
        let mut hasher = sha2::Sha256::new();
        hasher.update(username.as_bytes());
        hasher.update(nonce_prefix);
        let digest = hasher.finalize();
        let mut hint = [0u8; USER_HINT_LEN];
        hint.copy_from_slice(&digest[..USER_HINT_LEN]);
        hint
    }

    /// Apply user hint to the last USER_HINT_LEN bytes of the current nonce.
    pub fn apply_user_hint(&mut self, username: &str) {
        let prefix: [u8; 16] = self.nonce[..16].try_into().unwrap();
        let hint = Self::compute_user_hint(username, &prefix);
        let start = 24 - USER_HINT_LEN;
        self.nonce[start..].copy_from_slice(&hint);
    }

    /// Extract user hint from a nonce (last USER_HINT_LEN bytes).
    pub fn extract_user_hint(nonce: &[u8; 24]) -> [u8; USER_HINT_LEN] {
        let mut hint = [0u8; USER_HINT_LEN];
        hint.copy_from_slice(&nonce[24 - USER_HINT_LEN..]);
        hint
    }

    /// Encrypt plaintext, nonce handling depends on `include_nonce`:
    /// - If include_nonce: prepend 24-byte nonce to ciphertext, then increment
    /// - If !include_nonce: increment nonce, then encrypt (nonce not in output)
    ///   Returns: [nonce(24)?] || ciphertext || tag(16)
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        use chacha20poly1305::{
            aead::{Aead, KeyInit},
            XChaCha20Poly1305, XNonce,
        };

        let nonce_to_use = self.nonce;

        // Increment nonce BEFORE encryption (so next call uses incremented value)
        // This matches original mieru: increment first, then use.
        self.increment_nonce();

        let cipher = XChaCha20Poly1305::new_from_slice(&self.key)
            .map_err(|_| Error::Protocol("mieru: invalid key"))?;
        let xnonce = XNonce::from_slice(&nonce_to_use);
        let ct = cipher
            .encrypt(xnonce, plaintext)
            .map_err(|_| Error::Protocol("mieru: encryption failed"))?;

        let mut output = Vec::with_capacity(if self.include_nonce { 24 } else { 0 } + ct.len());
        if self.include_nonce {
            output.extend_from_slice(&nonce_to_use);
            self.include_nonce = false; // Only send nonce once per direction
        }
        output.extend_from_slice(&ct);
        Ok(output)
    }

    /// Decrypt data. If `expect_nonce` is true, read nonce from data[..24].
    /// If false, use the current implicit nonce.
    pub fn decrypt(&mut self, expect_nonce: bool, data: &[u8]) -> Result<Vec<u8>, Error> {
        use chacha20poly1305::{
            aead::{Aead, KeyInit},
            XChaCha20Poly1305, XNonce,
        };

        let (nonce_to_use, ct_start) = if expect_nonce {
            if data.len() < 24 + 16 {
                return Err(Error::Protocol("mieru: ciphertext too short"));
            }
            // Store the received nonce
            self.nonce.copy_from_slice(&data[..24]);
            // Increment for next operation
            let nonce = self.nonce;
            self.increment_nonce();
            (nonce, 24)
        } else {
            // Use current implicit nonce and increment
            let nonce = self.nonce;
            self.increment_nonce();
            (nonce, 0)
        };

        let ciphertext = &data[ct_start..];
        if ciphertext.len() < 16 {
            return Err(Error::Protocol("mieru: ciphertext too short for tag"));
        }

        let cipher = XChaCha20Poly1305::new_from_slice(&self.key)
            .map_err(|_| Error::Protocol("mieru: invalid key"))?;
        let xnonce = XNonce::from_slice(&nonce_to_use);
        cipher
            .decrypt(xnonce, ciphertext)
            .map_err(|_| Error::Protocol("mieru: decryption failed"))
    }

    /// Increment the 24-byte nonce as a big-endian integer.
    /// Carry propagates from byte 23 backward (matching original mieru).
    fn increment_nonce(&mut self) {
        for i in (0..24).rev() {
            self.nonce[i] = self.nonce[i].wrapping_add(1);
            if self.nonce[i] != 0 {
                break;
            }
        }
    }
}

// ── Nonce pattern helpers ────────────────────────────────────────────

/// Apply a nonce pattern to the first bytes of the nonce.
fn apply_nonce_pattern(nonce: &mut [u8; 24], pattern: &NoncePattern) {
    match pattern {
        NoncePattern::Random => { /* no modification */ }
        NoncePattern::Printable { min_len, max_len } => {
            let rewrite = random_pattern_len(*min_len, *max_len);
            for byte in nonce.iter_mut().take(rewrite) {
                *byte = to_printable(*byte);
            }
        }
        NoncePattern::PrintableSubset { min_len, max_len } => {
            let rewrite = random_pattern_len(*min_len, *max_len);
            for byte in nonce.iter_mut().take(rewrite) {
                *byte = to_common64(*byte);
            }
        }
        NoncePattern::Fixed { prefixes } => {
            if prefixes.is_empty() {
                return;
            }
            let idx = rand::Rng::gen_range(&mut rand::thread_rng(), 0..prefixes.len());
            let prefix = &prefixes[idx];
            let copy_len = prefix.len().min(nonce.len());
            nonce[..copy_len].copy_from_slice(&prefix[..copy_len]);
        }
    }
}

/// Generate a random length between min and max (inclusive, clamped to 24).
fn random_pattern_len(min: usize, max: usize) -> usize {
    let max = max.min(24);
    let min = min.min(max);
    rand::Rng::gen_range(&mut rand::thread_rng(), min..=max)
}

/// Map any byte to printable ASCII (0x20–0x7E).
fn to_printable(byte: u8) -> u8 {
    if (0x20..=0x7E).contains(&byte) {
        byte
    } else if byte & 0x80 != 0 && (0x20..=0x7E).contains(&(byte & 0x7F)) {
        byte & 0x7F
    } else {
        rand::Rng::gen_range(&mut rand::thread_rng(), 0x20..=0x7E)
    }
}

/// Map any byte to the common-64-character set.
const COMMON64: &[u8; 64] = b"!@#$%^&*()ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz<>";

fn to_common64(byte: u8) -> u8 {
    COMMON64[(byte & 0x3F) as usize]
}

// ── Key derivation ───────────────────────────────────────────────────

/// Derive a session key from username, password, and current time.
pub fn derive_key(username: &str, password: &str, unix_time_secs: u64) -> [u8; 32] {
    let rounded = (unix_time_secs / 120) * 120;
    derive_key_for_time(username, password, rounded)
}

/// Try multiple time windows (current ± 2 min, covers ±4 min total).
pub fn try_derive_keys(username: &str, password: &str, unix_time_secs: u64) -> [[u8; 32]; 3] {
    let rounded = (unix_time_secs / 120) * 120;
    [
        derive_key_for_time(username, password, rounded),
        derive_key_for_time(username, password, rounded.wrapping_sub(120)),
        derive_key_for_time(username, password, rounded.wrapping_add(120)),
    ]
}

fn derive_key_for_time(username: &str, password: &str, rounded_secs: u64) -> [u8; 32] {
    // Key derivation matching upstream mieru v3.x (cipher.HashPassword + pbkdf2Gen):
    //   1. hashedPassword = SHA-256(password || 0x00 || username)
    //   2. timeSalt       = SHA-256(uint64_be(rounded_secs))
    //   3. key            = PBKDF2-HMAC-SHA256(hashedPassword, timeSalt, 64 iter, 32 bytes)
    //
    // The username is mixed into the key via step 1 (cipher.HashPassword),
    // AND independently carried in the nonce tail as a user hint for fast
    // server-side user lookup (cipher.CheckUserFromHint).

    // Step 1: hashedPassword = SHA-256(password || 0x00 || username)
    let mut pw_hasher = sha2::Sha256::new();
    pw_hasher.update(password.as_bytes());
    pw_hasher.update([0x00]);
    pw_hasher.update(username.as_bytes());
    let hashed_password = pw_hasher.finalize();

    // Step 2: timeSalt = SHA-256(uint64_be(rounded_secs))
    let mut ts_hasher = sha2::Sha256::new();
    ts_hasher.update(rounded_secs.to_be_bytes());
    let time_salt = ts_hasher.finalize();

    // Step 3: key = PBKDF2(HMAC-SHA256, 64 iter, 32 bytes)
    let mut key = [0u8; 32];
    ring::pbkdf2::derive(
        ring::pbkdf2::PBKDF2_HMAC_SHA256,
        std::num::NonZeroU32::new(64).unwrap(),
        &time_salt,
        &hashed_password,
        &mut key,
    );

    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_key_deterministic() {
        let k1 = derive_key("testuser", "testpass", 1000000000);
        let k2 = derive_key("testuser", "testpass", 1000000000);
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = derive_key("user", "pass", 1000000000);
        let mut enc = MieruCipher::new(&key);
        // Copy nonce to decrypter so they start from same point
        let initial_nonce = *enc.current_nonce();
        let mut dec = MieruCipher::with_nonce(&key, initial_nonce);

        let pt = b"hello mieru";
        // First encrypt: includes nonce
        let ct = enc.encrypt(pt).unwrap();
        assert_eq!(&ct[..24], &initial_nonce);
        // Decrypt: expect nonce in data
        let got = dec.decrypt(true, &ct).unwrap();
        assert_eq!(&got, pt);

        // Second encrypt: implicit nonce (no nonce prefix)
        let pt2 = b"second message";
        let ct2 = enc.encrypt(pt2).unwrap();
        // Should NOT have nonce prefix
        assert!(ct2.len() == pt2.len() + 16);
        let got2 = dec.decrypt(false, &ct2).unwrap();
        assert_eq!(&got2, pt2);
    }

    #[test]
    fn test_nonce_increment_carry() {
        let mut c = MieruCipher::new(&[0u8; 32]);
        // Set nonce to trigger carry
        c.nonce = [0u8; 24];
        c.nonce[23] = 0xFFu8;
        c.increment_nonce();
        assert_eq!(c.nonce[23], 0);
        assert_eq!(c.nonce[22], 1);

        // Full carry
        c.nonce = [0xFFu8; 24];
        c.increment_nonce();
        assert_eq!(c.nonce, [0u8; 24]);
    }

    #[test]
    fn test_user_hint() {
        let hint = MieruCipher::compute_user_hint("testuser", &[0xAAu8; 16]);
        assert_eq!(hint.len(), 4);

        // Same username + prefix = same hint
        let hint2 = MieruCipher::compute_user_hint("testuser", &[0xAAu8; 16]);
        assert_eq!(hint, hint2);

        // Different username = different hint
        let hint3 = MieruCipher::compute_user_hint("other", &[0xAAu8; 16]);
        assert_ne!(hint, hint3);
    }
}
