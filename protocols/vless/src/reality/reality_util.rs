//! REALITY-specific utility functions — base64 keys, short IDs.
//! Generic TLS 1.3 utils are in ztls::util.

use base64::engine::{general_purpose::URL_SAFE_NO_PAD, Engine as _};

pub fn decode_public_key(encoded: &str) -> Result<[u8; 32], std::io::Error> {
    let decoded = URL_SAFE_NO_PAD.decode(encoded).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid base64: {}", e),
        )
    })?;
    if decoded.len() != 32 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid public key length: {}", decoded.len()),
        ));
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&decoded);
    Ok(key)
}

pub fn decode_private_key(encoded: &str) -> Result<[u8; 32], std::io::Error> {
    decode_public_key(encoded)
}

pub fn decode_short_id(encoded: &str) -> Result<[u8; 8], std::io::Error> {
    let hex = hex::decode(encoded).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid hex: {}", e),
        )
    })?;
    if hex.len() > 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "short_id must be at most 8 bytes",
        ));
    }
    let mut id = [0u8; 8];
    id[..hex.len()].copy_from_slice(&hex);
    Ok(id)
}

pub fn encode_key(key: &[u8; 32]) -> String {
    URL_SAFE_NO_PAD.encode(key)
}
