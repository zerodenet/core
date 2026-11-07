use base64::{engine::general_purpose::STANDARD, Engine as _};
use core::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct Key([u8; 32]);

impl Key {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Key {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Key([REDACTED])")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    InvalidEncoding,
    InvalidLength,
}

pub fn parse_key(value: &str) -> Result<Key, KeyError> {
    if value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        let bytes = value.as_bytes();
        let mut key = Zeroizing::new([0_u8; 32]);
        for index in 0..32 {
            key[index] = (hex_nibble(bytes[index * 2]) << 4) | hex_nibble(bytes[index * 2 + 1]);
        }
        return Ok(Key(*key));
    }

    let mut key = Zeroizing::new([0_u8; 32]);
    match STANDARD.decode_slice(value, &mut *key) {
        Ok(32) => Ok(Key(*key)),
        Ok(_) | Err(base64::DecodeSliceError::OutputSliceTooSmall) => Err(KeyError::InvalidLength),
        Err(base64::DecodeSliceError::DecodeError(_)) => Err(KeyError::InvalidEncoding),
    }
}

/// Stable public identity independent of the accepted key text encoding.
/// Callers must only pass a peer's public key, never private/PSK material.
pub fn public_peer_id(value: &str) -> Result<alloc::string::String, KeyError> {
    let bytes = public_peer_id_bytes(value)?;
    Ok(alloc::string::String::from_utf8(bytes.to_vec()).expect("canonical peer identity is ASCII"))
}

/// Fixed canonical public identity for prepaid observation rows. As above,
/// callers must supply only a peer public key, never private/PSK material.
/// Parsing/encoding uses its original fixed 32-byte key and 54-byte output.
pub fn public_peer_id_bytes(value: &str) -> Result<[u8; 54], KeyError> {
    let key = parse_key(value)?;
    let mut bytes = [0; 54];
    bytes[..10].copy_from_slice(b"wireguard:");
    STANDARD
        .encode_slice(key.as_bytes(), &mut bytes[10..])
        .expect("fixed canonical base64 output");
    Ok(bytes)
}

#[cfg(feature = "runtime")]
pub(crate) fn validated_public_peer_id(key: &Key) -> alloc::string::String {
    alloc::format!("wireguard:{}", STANDARD.encode(key.as_bytes()))
}

const fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => 0,
    }
}
