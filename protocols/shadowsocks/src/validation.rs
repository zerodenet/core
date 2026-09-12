use alloc::format;
use alloc::string::String;
#[cfg(feature = "blake3")]
use alloc::string::ToString;
#[cfg(feature = "blake3")]
use alloc::vec::Vec;

#[cfg(feature = "blake3")]
use zero_core::Error;

mod legacy;
mod limits;
mod plugin;
mod users;
pub use legacy::LegacyCipher;
pub use limits::StateLimits;
pub use plugin::{PluginConfig, PluginMode};
pub use users::validate_inbound_users;

/// Supported Shadowsocks methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CipherKind {
    None,
    Legacy(LegacyCipher),
    XChacha20Poly1305,
    Aes128Ccm,
    Aes256Ccm,
    Aes128GcmSiv,
    Aes256GcmSiv,
    Sm4Gcm,
    Sm4Ccm,

    Aes128Gcm,
    Aes256Gcm,
    Chacha20Poly1305,
    Blake3Aes128Gcm,
    Blake3Aes256Gcm,
    Blake3Chacha20Poly1305,
    Blake3Chacha8Poly1305,
}

impl CipherKind {
    pub const fn is_stream(self) -> bool {
        matches!(self, Self::None | Self::Legacy(_))
    }
    pub const fn is_extra_aead(self) -> bool {
        matches!(
            self,
            Self::XChacha20Poly1305
                | Self::Aes128Ccm
                | Self::Aes256Ccm
                | Self::Aes128GcmSiv
                | Self::Aes256GcmSiv
                | Self::Sm4Gcm
                | Self::Sm4Ccm
        )
    }
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Legacy(cipher) => cipher.name(),
            Self::XChacha20Poly1305 => "xchacha20-ietf-poly1305",
            Self::Aes128Ccm => "aes-128-ccm",
            Self::Aes256Ccm => "aes-256-ccm",
            Self::Aes128GcmSiv => "aes-128-gcm-siv",
            Self::Aes256GcmSiv => "aes-256-gcm-siv",
            Self::Sm4Gcm => "sm4-gcm",
            Self::Sm4Ccm => "sm4-ccm",
            Self::Aes128Gcm => "aes-128-gcm",
            Self::Aes256Gcm => "aes-256-gcm",
            Self::Chacha20Poly1305 => "chacha20-ietf-poly1305",
            Self::Blake3Aes128Gcm => "2022-blake3-aes-128-gcm",
            Self::Blake3Aes256Gcm => "2022-blake3-aes-256-gcm",
            Self::Blake3Chacha20Poly1305 => "2022-blake3-chacha20-poly1305",
            Self::Blake3Chacha8Poly1305 => "2022-blake3-chacha8-poly1305",
        }
    }

    pub const fn is_2022_chacha(&self) -> bool {
        matches!(
            self,
            Self::Blake3Chacha20Poly1305 | Self::Blake3Chacha8Poly1305
        )
    }

    pub fn key_len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Legacy(cipher) => cipher.key_len(),
            Self::XChacha20Poly1305 => 32,
            Self::Aes128Ccm => 16,
            Self::Aes256Ccm => 32,
            Self::Aes128GcmSiv => 16,
            Self::Aes256GcmSiv => 32,
            Self::Sm4Gcm => 16,
            Self::Sm4Ccm => 16,
            Self::Aes128Gcm | Self::Blake3Aes128Gcm => 16,
            Self::Aes256Gcm | Self::Blake3Aes256Gcm => 32,
            Self::Chacha20Poly1305 | Self::Blake3Chacha20Poly1305 | Self::Blake3Chacha8Poly1305 => {
                32
            }
        }
    }

    pub fn salt_len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Legacy(cipher) => cipher.iv_len(),
            _ => self.key_len(),
        }
    }

    pub fn udp_salt_len(&self) -> usize {
        match self {
            Self::Blake3Aes128Gcm | Self::Blake3Aes256Gcm => 12,
            Self::Blake3Chacha20Poly1305 | Self::Blake3Chacha8Poly1305 => 24,
            _ => self.salt_len(),
        }
    }

    pub fn tag_len(&self) -> usize {
        if self.is_stream() {
            0
        } else {
            16
        }
    }

    pub const fn is_blake3(&self) -> bool {
        matches!(
            self,
            Self::Blake3Aes128Gcm
                | Self::Blake3Aes256Gcm
                | Self::Blake3Chacha20Poly1305
                | Self::Blake3Chacha8Poly1305
        )
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" | "plain" => Some(Self::None),
            "" => Some(Self::Legacy(LegacyCipher::Table)),
            "xchacha20-ietf-poly1305" => Some(Self::XChacha20Poly1305),
            "aes-128-ccm" => Some(Self::Aes128Ccm),
            "aes-256-ccm" => Some(Self::Aes256Ccm),
            "aes-128-gcm-siv" => Some(Self::Aes128GcmSiv),
            "aes-256-gcm-siv" => Some(Self::Aes256GcmSiv),
            "sm4-gcm" => Some(Self::Sm4Gcm),
            "sm4-ccm" => Some(Self::Sm4Ccm),

            "aes-128-gcm" => Some(Self::Aes128Gcm),
            "aes-256-gcm" => Some(Self::Aes256Gcm),
            "chacha20-ietf-poly1305" => Some(Self::Chacha20Poly1305),
            "2022-blake3-aes-128-gcm" => Some(Self::Blake3Aes128Gcm),
            "2022-blake3-aes-256-gcm" => Some(Self::Blake3Aes256Gcm),
            "2022-blake3-chacha8-poly1305" => Some(Self::Blake3Chacha8Poly1305),
            "2022-blake3-chacha20-poly1305" => Some(Self::Blake3Chacha20Poly1305),
            name => LegacyCipher::parse(name).map(Self::Legacy),
        }
    }
}

pub fn validate_cipher(cipher: &str) -> Result<CipherKind, String> {
    CipherKind::from_str(cipher).ok_or_else(|| format!("unknown cipher `{cipher}`"))
}

pub fn is_plain(cipher: &str) -> bool {
    CipherKind::from_str(cipher) == Some(CipherKind::None)
}
pub fn validate_user_count(cipher: &str, count: usize) -> Result<(), String> {
    if validate_cipher(cipher)?.is_stream() && count > 1 {
        return Err("stream methods require one user per listener".into());
    }
    Ok(())
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
        if keys.len() > 1 && cipher.is_2022_chacha() {
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
        CipherKind::Blake3Chacha20Poly1305 | CipherKind::Blake3Chacha8Poly1305 => password,
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

/// Legacy replay policy. SS2022 always rejects replays independently of this setting.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum ReplayPolicy {
    #[default]
    Default,
    Ignore,
    Detect,
    Reject,
}
