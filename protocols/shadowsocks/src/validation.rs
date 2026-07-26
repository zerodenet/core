use alloc::format;
use alloc::string::{String, ToString};
#[cfg(feature = "blake3")]
use alloc::vec::Vec;

use zero_core::Error;

/// AEAD cipher methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CipherKind {
    Aes128Gcm,
    Aes256Gcm,
    Chacha20Poly1305,
    Blake3Aes128Gcm,
    Blake3Aes256Gcm,
    Blake3Chacha20Poly1305,
}

impl CipherKind {
    pub fn key_len(&self) -> usize {
        match self {
            Self::Aes128Gcm | Self::Blake3Aes128Gcm => 16,
            Self::Aes256Gcm | Self::Blake3Aes256Gcm => 32,
            Self::Chacha20Poly1305 | Self::Blake3Chacha20Poly1305 => 32,
        }
    }

    pub fn salt_len(&self) -> usize {
        self.key_len()
    }

    pub fn udp_salt_len(&self) -> usize {
        match self {
            Self::Blake3Aes128Gcm | Self::Blake3Aes256Gcm => 12,
            Self::Blake3Chacha20Poly1305 => 24,
            _ => self.salt_len(),
        }
    }

    pub fn tag_len(&self) -> usize {
        16
    }

    pub const fn is_blake3(&self) -> bool {
        matches!(
            self,
            Self::Blake3Aes128Gcm | Self::Blake3Aes256Gcm | Self::Blake3Chacha20Poly1305
        )
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "aes-128-gcm" => Some(Self::Aes128Gcm),
            "aes-256-gcm" => Some(Self::Aes256Gcm),
            "chacha20-ietf-poly1305" => Some(Self::Chacha20Poly1305),
            "2022-blake3-aes-128-gcm" => Some(Self::Blake3Aes128Gcm),
            "2022-blake3-aes-256-gcm" => Some(Self::Blake3Aes256Gcm),
            "2022-blake3-chacha20-poly1305" => Some(Self::Blake3Chacha20Poly1305),
            _ => None,
        }
    }
}

pub fn validate_cipher(cipher: &str) -> Result<CipherKind, String> {
    CipherKind::from_str(cipher).ok_or_else(|| format!("unknown cipher `{cipher}`"))
}

pub fn validate_password(cipher: &str, password: &str) -> Result<(), String> {
    let cipher = validate_cipher(cipher)?;
    if !cipher.is_blake3() {
        return Ok(());
    }

    #[cfg(feature = "blake3")]
    {
        let keys = password.split(':').collect::<Vec<_>>();
        if keys.iter().any(|key| key.is_empty()) {
            return Err("2022 password chain contains an empty PSK".into());
        }
        if keys.len() > 1 && cipher == CipherKind::Blake3Chacha20Poly1305 {
            return Err("SIP023 EIH requires a 2022 AES method".into());
        }
        keys.into_iter()
            .try_for_each(|key| decode_blake3_master_key(cipher, key.as_bytes()).map(|_| ()))
            .map_err(|error| error.to_string())
    }

    #[cfg(not(feature = "blake3"))]
    {
        let _ = password;
        Err("2022 cipher validation requires the `blake3` feature".into())
    }
}

#[cfg(feature = "blake3")]
pub(crate) fn decode_blake3_master_key(
    cipher: CipherKind,
    password: &[u8],
) -> Result<Vec<u8>, Error> {
    use base64::{
        alphabet,
        engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
        Engine,
    };

    let password = core::str::from_utf8(password)
        .map_err(|_| Error::Protocol("ss: 2022 password must be utf-8 base64"))?;
    let password = match cipher {
        CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm => {
            password.rsplit(':').next().unwrap_or(password)
        }
        CipherKind::Blake3Chacha20Poly1305 => password,
        _ => return Err(Error::Protocol("ss: cipher is not a 2022 method")),
    };

    const ENGINE: GeneralPurpose = GeneralPurpose::new(
        &alphabet::STANDARD,
        GeneralPurposeConfig::new()
            .with_encode_padding(true)
            .with_decode_padding_mode(DecodePaddingMode::Indifferent),
    );

    let key = ENGINE
        .decode(password)
        .map_err(|_| Error::Protocol("ss: invalid 2022 base64 password"))?;
    if key.len() != cipher.key_len() {
        return Err(Error::Protocol("ss: invalid 2022 password key length"));
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::{validate_cipher, validate_password, CipherKind};

    #[test]
    fn parses_known_ciphers() {
        assert_eq!(validate_cipher("aes-128-gcm"), Ok(CipherKind::Aes128Gcm));
        assert_eq!(
            validate_cipher("2022-blake3-chacha20-poly1305"),
            Ok(CipherKind::Blake3Chacha20Poly1305)
        );
        assert!(validate_cipher("nonexistent").is_err());
    }

    #[cfg(feature = "blake3")]
    #[test]
    fn validates_2022_passwords() {
        assert!(validate_password("2022-blake3-aes-128-gcm", "MDEyMzQ1Njc4OWFiY2RlZg==").is_ok());
        assert!(validate_password(
            "2022-blake3-aes-256-gcm",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        )
        .is_ok());
        assert!(validate_password(
            "2022-blake3-chacha20-poly1305",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
        )
        .is_ok());
        assert!(validate_password("2022-blake3-aes-128-gcm", "bad").is_err());
    }
}
