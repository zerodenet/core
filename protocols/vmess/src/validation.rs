use zero_core::Error;

pub const DEFAULT_MUX_RESPONSE_BACKLOG_FRAMES: u32 = 32;
pub const DEFAULT_MUX_RESPONSE_BACKLOG_BYTES: u64 = 1024 * 1024;
pub const MAX_MUX_RESPONSE_BACKLOG_FRAMES: u32 = 4096;
pub const MIN_MUX_RESPONSE_BACKLOG_BYTES: u64 = 16 * 1024;
pub const MAX_MUX_RESPONSE_BACKLOG_BYTES: u64 = 64 * 1024 * 1024;

pub fn validate_mux_response_backlog(
    frames: Option<u32>,
    bytes: Option<u64>,
) -> Result<(), &'static str> {
    if frames.is_some_and(|value| value == 0 || value > MAX_MUX_RESPONSE_BACKLOG_FRAMES) {
        return Err("VMess MUX response backlog frames must be within 1..=4096");
    }
    if bytes.is_some_and(|value| {
        !(MIN_MUX_RESPONSE_BACKLOG_BYTES..=MAX_MUX_RESPONSE_BACKLOG_BYTES).contains(&value)
    }) {
        return Err("VMess MUX response backlog bytes must be within 16384..=67108864");
    }
    Ok(())
}

/// AEAD cipher variants for VMess header encryption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VmessCipher {
    Aes128Gcm,
    Chacha20Poly1305,
    None,
    Zero,
}

impl VmessCipher {
    pub fn key_len(self) -> usize {
        match self {
            VmessCipher::Aes128Gcm => 16,
            VmessCipher::Chacha20Poly1305 => 32,
            VmessCipher::None | VmessCipher::Zero => 16,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            VmessCipher::Aes128Gcm => "aes-128-gcm",
            VmessCipher::Chacha20Poly1305 => "chacha20-poly1305",
            VmessCipher::None => "none",
            VmessCipher::Zero => "zero",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "auto" => Some(VmessCipher::Aes128Gcm),
            "aes-128-gcm" => Some(VmessCipher::Aes128Gcm),
            "chacha20-poly1305" => Some(VmessCipher::Chacha20Poly1305),
            "none" => Some(VmessCipher::None),
            "zero" => Some(VmessCipher::Zero),
            _ => None,
        }
    }

    pub fn uses_plain_body(self) -> bool {
        matches!(self, VmessCipher::None | VmessCipher::Zero)
    }
}

pub fn parse_uuid(input: &str) -> Result<[u8; 16], Error> {
    let hex = input.replace('-', "");
    if hex.len() != 32 {
        return Err(Error::Protocol("vmess uuid must be 32 hex characters"));
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Protocol("vmess uuid contains invalid hex characters"))?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{parse_uuid, VmessCipher};

    #[test]
    fn parses_hyphenated_uuid() {
        let uuid = parse_uuid("123e4567-e89b-12d3-a456-426614174000").unwrap();
        assert_eq!(
            uuid,
            [
                0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17,
                0x40, 0x00,
            ]
        );
    }

    #[test]
    fn accepts_auto_cipher_alias() {
        assert_eq!(VmessCipher::from_name("auto"), Some(VmessCipher::Aes128Gcm));
    }
}
