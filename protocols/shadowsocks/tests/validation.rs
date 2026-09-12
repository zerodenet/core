#![cfg(feature = "validation")]

use shadowsocks::validation::{validate_cipher, validate_password, CipherKind};

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
