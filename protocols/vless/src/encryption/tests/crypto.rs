// Vectors generated with the pinned reference dependency lukechampine.com/blake3
// v1.4.1 and Go crypto/aes. Context bytes deliberately include invalid UTF-8.
use super::crypto::{derive, AeadState};
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[test]
fn binary_context_derivation_and_aead_match_official_go() {
    let vectors = [
        (
            0,
            "5a57b0e5c15726d18f697e8d6738d7a216de67109f18663de7b1d10d1f39d0b3",
            "d1cd919babc6d4d85d8b1ee9096e43394131a4369318d22a893c9cd7d4b29c4645",
        ),
        (
            5,
            "b1f1d2f1ec4192a9906014f1cb3288308ba1e797d65b412966d67bd93d24a9c8",
            "710f322e5026a35e39e59fba4e8f143ccfbf6c0848b9f71e162f766d58ee1e3795",
        ),
        (
            16,
            "d2bd9e5c8107f8eabc2ccfd4398d0d95eb49de810e941150402657d2851cdfe5",
            "089c9b555e2caad7c7cd0df383afe9fd4819edf808550b52e3b2106d56704d4eb4",
        ),
        (
            1024,
            "144c95333e2347652b52da42c356a4bcd5af9a7678c3e611015e5a757d142efa",
            "ba40b8c29efe0283ec1789541fe6b293bf012b5f253d97cbbf2577d0870afadf03",
        ),
        (
            1216,
            "c3407f9df2ace91d10782d4f7d595b89795981ae15c10ea3c9cb8680d3bf0b03",
            "a0a3a5364d33c9c0344eae61f7f1d46dec50054dd851bc56a9ce5bccdaad002917",
        ),
    ];
    let key: Vec<u8> = (0..96).collect();
    for (length, expected, encrypted) in vectors {
        let context: Vec<u8> = (0..length).map(|i| (i * 31 + 255) as u8).collect();
        assert_eq!(hex(&derive(&context, &key)), expected);
        let mut state = AeadState::new(&context, &key, true);
        let sealed = state
            .seal(b"reference payload", &[23, 3, 3, 0, 33])
            .unwrap();
        assert_eq!(hex(&sealed), encrypted);
        let mut peer = AeadState::new(&context, &key, true);
        assert_eq!(
            peer.open(&sealed, &[23, 3, 3, 0, 33]).unwrap(),
            b"reference payload"
        );
    }
}
#[test]
fn both_ciphers_reject_tampered_records() {
    for aes in [false, true] {
        let mut state = AeadState::new(b"context", b"key", aes);
        let mut sealed = state.seal(b"content", b"header").unwrap();
        sealed[0] ^= 1;
        let mut peer = AeadState::new(b"context", b"key", aes);
        assert!(peer.open(&sealed, b"header").is_err());
    }
}
#[test]
fn replay_cache_rejects_duplicates_and_keeps_distinct_requests() {
    use super::state::ServerCache;
    let mut cache = ServerCache::default();
    cache.insert([1; 16], [2; 64], 60).unwrap();
    assert_eq!(cache.resume(&[1; 16], [3; 32]).unwrap(), Some([2; 64]));
    assert!(cache.resume(&[1; 16], [3; 32]).is_err());
    assert_eq!(cache.resume(&[1; 16], [4; 32]).unwrap(), Some([2; 64]));
    assert_eq!(cache.resume(&[9; 16], [3; 32]).unwrap(), None);
}
