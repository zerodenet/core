#![cfg(feature = "reality")]

use vless::reality::reality_util::{decode_public_key, decode_short_id};
use ztls::util::extract_client_random;

#[test]
fn test_decode_short_id() {
    let short_id = decode_short_id("0123456789abcdef").unwrap();
    assert_eq!(short_id, [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);
    // Shorter inputs are rejected (must be exactly 8 bytes / 16 hex chars)
    assert_eq!(
        decode_short_id("abcdef").unwrap(),
        [0xab, 0xcd, 0xef, 0, 0, 0, 0, 0]
    );
    assert_eq!(decode_short_id("").unwrap(), [0; 8]);
    assert!(decode_short_id("abc").is_err());
    assert!(decode_short_id("0123456789abcdef0").is_err());
}

#[test]
fn test_extract_client_random() {
    // ztls::util::extract_client_random reads 32 bytes from offset 11
    // (5-byte record header + 4-byte handshake header + 2-byte version)
    let mut client_hello = vec![0u8; 100];
    for i in 0..32 {
        client_hello[11 + i] = (i + 1) as u8;
    }
    let random = extract_client_random(&client_hello).unwrap();
    for (index, byte) in random.iter().enumerate() {
        assert_eq!(*byte, (index + 1) as u8);
    }
}

#[test]
fn test_decode_public_key() {
    use base64::engine::{general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let key_bytes = [0x42u8; 32];
    let encoded = URL_SAFE_NO_PAD.encode(key_bytes);
    let decoded = decode_public_key(&encoded).unwrap();
    assert_eq!(decoded, key_bytes);
    let short_key = [0x42u8; 16];
    let encoded_short = URL_SAFE_NO_PAD.encode(short_key);
    assert!(decode_public_key(&encoded_short).is_err());
    assert!(decode_public_key("not-valid-base64!!!").is_err());
}
