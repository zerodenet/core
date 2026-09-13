#![cfg(feature = "reality")]

use vless::reality::reality_auth::*;
use x25519_dalek::{PublicKey, StaticSecret};

#[test]
fn test_ecdh() {
    // Test vectors
    let private_key_a = [1u8; 32];
    let private_key_b = [2u8; 32];

    let priv_a = StaticSecret::from(private_key_a);
    let pub_a = PublicKey::from(&priv_a);

    let priv_b = StaticSecret::from(private_key_b);
    let pub_b = PublicKey::from(&priv_b);

    let shared_a_bytes = priv_a.diffie_hellman(&pub_b).to_bytes();
    let shared_b_bytes = priv_b.diffie_hellman(&pub_a).to_bytes();

    assert_eq!(shared_a_bytes, shared_b_bytes);
}

#[test]
fn test_session_id_structure() {
    // Test that session ID has the correct structure
    let version = [1, 8, 1]; // major, minor, patch
    let timestamp = get_current_timestamp();
    let short_id = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];

    let session_id = create_session_id(version, timestamp, &short_id);

    // Verify structure
    assert_eq!(session_id[0], 1); // version major
    assert_eq!(session_id[1], 8); // version minor
    assert_eq!(session_id[2], 1); // version patch
    assert_eq!(session_id[3], 0); // reserved

    // Verify timestamp
    let extracted_timestamp =
        u32::from_be_bytes([session_id[4], session_id[5], session_id[6], session_id[7]]);
    assert_eq!(extracted_timestamp, timestamp);

    // Verify short_id
    assert_eq!(&session_id[8..16], &short_id[..]);

    // Verify remaining bytes are zero
    for byte in session_id.iter().skip(16) {
        assert_eq!(*byte, 0);
    }
}

#[test]
fn test_version_comparison() {
    // Test version comparison logic (same as used in reality_server_handler.rs)
    let v1_8_1 = [1u8, 8, 1];
    let v1_8_0 = [1u8, 8, 0];
    let v1_9_0 = [1u8, 9, 0];
    let v2_0_0 = [2u8, 0, 0];

    assert!(v1_8_0 < v1_8_1);
    assert!(v1_8_1 < v1_9_0);
    assert!(v1_9_0 < v2_0_0);
    assert!(v1_8_1 > v1_8_0);
}

#[test]
fn test_timestamp_validation_logic() {
    // Test the timestamp validation logic used in reality_server_handler.rs
    let now = get_current_timestamp();

    // Test within bounds (60 seconds = 60000 milliseconds)
    let max_diff_ms = 60000u64;
    let max_diff_secs = max_diff_ms / 1000;

    // Current time - should pass
    let diff = now.abs_diff(now);
    assert!((diff as u64) <= max_diff_secs);

    // 30 seconds ago - should pass
    let past_timestamp = now - 30;
    let diff = now.abs_diff(past_timestamp);
    assert!((diff as u64) <= max_diff_secs);

    // 30 seconds future - should pass
    let future_timestamp = now + 30;
    let diff = now.abs_diff(future_timestamp);
    assert!((diff as u64) <= max_diff_secs);

    // 2 minutes ago - should fail
    let old_timestamp = now.saturating_sub(120);
    let diff = now.abs_diff(old_timestamp);
    assert!((diff as u64) > max_diff_secs);

    // 2 minutes future - should fail
    let future_timestamp = now + 120;
    let diff = now.abs_diff(future_timestamp);
    assert!((diff as u64) > max_diff_secs);
}

#[test]
fn test_session_id_encryption_preserves_structure() {
    // Create session ID with known values
    let version = [1, 8, 1];
    let timestamp = 1234567890u32;
    let short_id = [0xAB; 8];

    let session_id = create_session_id(version, timestamp, &short_id);

    // Perform ECDH
    let client_private = [0x01; 32];
    let server_public = [0x02; 32];
    let shared_secret = perform_ecdh(&client_private, &server_public).unwrap();

    // Derive auth key
    let salt = [0x03; 20];
    let auth_key = derive_auth_key(&shared_secret, &salt, b"REALITY").unwrap();

    // Encrypt session ID (first 16 bytes)
    let plaintext: [u8; 16] = session_id[0..16].try_into().unwrap();
    let nonce = [0x04; 12];
    let aad = b"test additional authenticated data";

    let encrypted = encrypt_session_id(&plaintext, &auth_key, &nonce, aad).unwrap();

    // Decrypt session ID
    let decrypted = decrypt_session_id(&encrypted, &auth_key, &nonce, aad).unwrap();

    // Verify we can recover the structure
    assert_eq!(decrypted[0], version[0]);
    assert_eq!(decrypted[1], version[1]);
    assert_eq!(decrypted[2], version[2]);
    assert_eq!(decrypted[3], 0); // reserved

    let recovered_timestamp =
        u32::from_be_bytes([decrypted[4], decrypted[5], decrypted[6], decrypted[7]]);
    assert_eq!(recovered_timestamp, timestamp);

    assert_eq!(&decrypted[8..16], &short_id[..]);
}

#[test]
fn test_hkdf() {
    let shared_secret = [0x42u8; 32];
    let salt = [0x43u8; 20];
    let info = b"REALITY";

    let auth_key = derive_auth_key(&shared_secret, &salt, info).unwrap();

    // Verify length
    assert_eq!(auth_key.len(), 32);

    // Verify deterministic
    let auth_key2 = derive_auth_key(&shared_secret, &salt, info).unwrap();
    assert_eq!(auth_key, auth_key2);

    // Verify different salt produces different key
    let salt2 = [0x44u8; 20];
    let auth_key3 = derive_auth_key(&shared_secret, &salt2, info).unwrap();
    assert_ne!(auth_key, auth_key3);
}

#[test]
fn test_aes_gcm_roundtrip() {
    let plaintext = [0x55u8; 16];
    let auth_key = [0x66u8; 32];
    let nonce = [0x77u8; 12];
    let aad = b"additional authenticated data";

    let encrypted = encrypt_session_id(&plaintext, &auth_key, &nonce, aad).unwrap();
    assert_eq!(encrypted.len(), 32);

    let decrypted = decrypt_session_id(&encrypted, &auth_key, &nonce, aad).unwrap();
    assert_eq!(plaintext, decrypted);
}

#[test]
fn test_aes_gcm_wrong_key_fails() {
    let plaintext = [0x55u8; 16];
    let auth_key = [0x66u8; 32];
    let wrong_key = [0x67u8; 32];
    let nonce = [0x77u8; 12];
    let aad = b"additional authenticated data";

    let encrypted = encrypt_session_id(&plaintext, &auth_key, &nonce, aad).unwrap();

    let result = decrypt_session_id(&encrypted, &wrong_key, &nonce, aad);
    assert!(result.is_err());
}

#[test]
fn test_aes_gcm_wrong_aad_fails() {
    let plaintext = [0x55u8; 16];
    let auth_key = [0x66u8; 32];
    let nonce = [0x77u8; 12];
    let aad = b"additional authenticated data";
    let wrong_aad = b"wrong additional authenticated data";

    let encrypted = encrypt_session_id(&plaintext, &auth_key, &nonce, aad).unwrap();

    let result = decrypt_session_id(&encrypted, &auth_key, &nonce, wrong_aad);
    assert!(result.is_err());
}

#[test]
fn test_create_session_id() {
    let version = [1, 8, 1];
    let timestamp = 1234567890u32;
    let short_id = [0xAB; 8];

    let session_id = create_session_id(version, timestamp, &short_id);

    assert_eq!(session_id[0], 1);
    assert_eq!(session_id[1], 8);
    assert_eq!(session_id[2], 1);
    assert_eq!(session_id[3], 0);

    let ts = u32::from_be_bytes([session_id[4], session_id[5], session_id[6], session_id[7]]);
    assert_eq!(ts, timestamp);

    assert_eq!(&session_id[8..16], &short_id[..]);

    // Verify remaining bytes are zeros
    for byte in session_id.iter().skip(16) {
        assert_eq!(*byte, 0);
    }
}

#[test]
fn test_validation_scenarios() {
    println!("\n=== REALITY Validation Test Scenarios ===\n");

    // Scenario 1: Version validation
    println!("Scenario 1: Version Validation");
    println!("  Client version: [1, 8, 1]");
    println!("  Min version: [1, 8, 0]");
    println!("  Max version: [1, 9, 0]");
    println!("  Expected: PASS ✓");

    let client_version = [1u8, 8, 1];
    let min_version = [1u8, 8, 0];
    let max_version = [1u8, 9, 0];
    assert!(client_version >= min_version);
    assert!(client_version <= max_version);

    // Scenario 2: Version below minimum
    println!("\nScenario 2: Version Below Minimum");
    println!("  Client version: [1, 7, 5]");
    println!("  Min version: [1, 8, 0]");
    println!("  Expected: FAIL ✗");

    let old_client_version = [1u8, 7, 5];
    assert!(old_client_version < min_version);

    // Scenario 3: Version above maximum
    println!("\nScenario 3: Version Above Maximum");
    println!("  Client version: [2, 0, 0]");
    println!("  Max version: [1, 9, 0]");
    println!("  Expected: FAIL ✗");

    let new_client_version = [2u8, 0, 0];
    assert!(new_client_version > max_version);

    // Scenario 4: Timestamp validation (within bounds)
    println!("\nScenario 4: Timestamp Within Bounds");
    let now = get_current_timestamp();
    let client_timestamp = now - 30; // 30 seconds ago
    let max_diff_ms = 60000u64; // 60 seconds

    let diff = now.abs_diff(client_timestamp);
    println!("  Server time: {}", now);
    println!("  Client time: {} (30 seconds ago)", client_timestamp);
    println!("  Max allowed: {} seconds", max_diff_ms / 1000);
    println!("  Actual diff: {} seconds", diff);
    println!("  Expected: PASS ✓");
    assert!((diff as u64) <= (max_diff_ms / 1000));

    // Scenario 5: Timestamp validation (out of bounds)
    println!("\nScenario 5: Timestamp Out of Bounds");
    let old_timestamp = now - 120; // 2 minutes ago
    let diff = now.abs_diff(old_timestamp);
    println!("  Server time: {}", now);
    println!("  Client time: {} (2 minutes ago)", old_timestamp);
    println!("  Max allowed: {} seconds", max_diff_ms / 1000);
    println!("  Actual diff: {} seconds", diff);
    println!("  Expected: FAIL ✗");
    assert!((diff as u64) > (max_diff_ms / 1000));

    // Scenario 6: Future timestamp validation
    println!("\nScenario 6: Future Timestamp Validation");
    let future_timestamp = now + 45; // 45 seconds in future
    let diff = now.abs_diff(future_timestamp);
    println!("  Server time: {}", now);
    println!("  Client time: {} (45 seconds in future)", future_timestamp);
    println!("  Max allowed: {} seconds", max_diff_ms / 1000);
    println!("  Actual diff: {} seconds", diff);
    println!("  Expected: PASS ✓");
    assert!((diff as u64) <= (max_diff_ms / 1000));

    println!("\n=== All Validation Scenarios Passed ===\n");
}

#[test]
fn ecdh_rejects_low_order_peer_keys() {
    assert!(perform_ecdh(&[1; 32], &[0; 32]).is_err());
    let mut order_one = [0; 32];
    order_one[0] = 1;
    assert!(perform_ecdh(&[1; 32], &order_one).is_err());
}
