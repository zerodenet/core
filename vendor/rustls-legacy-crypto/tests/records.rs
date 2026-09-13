use rustls_legacy_crypto::{Algorithm, RecordKey};
#[test]
fn cbc_rejects_corruption_replay_and_short_records_at_every_block_boundary() {
    for algorithm in [
        Algorithm::Aes128Sha1,
        Algorithm::Aes256Sha1,
        Algorithm::Aes128Sha256,
        Algorithm::Aes256Sha256,
        Algorithm::Aes256Sha384,
        Algorithm::TripleDesSha1,
    ] {
        let key = RecordKey::new(
            algorithm,
            &vec![7; algorithm.key_len()],
            &vec![9; algorithm.mac_len()],
        )
        .unwrap();
        let aad = [0, 0, 0, 0, 0, 0, 0, 0, 23, 3, 3];
        for n in [0, 1, 15, 16, 17, 31, 32, 255, 256, 16384] {
            let plain = vec![0x42; n];
            let record = key.seal(&aad, &plain).unwrap();
            assert_eq!(&*key.open(&aad, &record).unwrap(), &plain);
            let mut next = aad;
            next[7] = 1;
            assert!(key.open(&next, &record).is_err());
            for offset in [0, record.len() / 2, record.len() - 1] {
                let mut broken = record.clone();
                broken[offset] ^= 0x80;
                assert!(key.open(&aad, &broken).is_err());
            }
        }
        assert!(key.open(&aad, &[0; 7]).is_err());
        assert!(key.seal(&aad, &vec![0; 16385]).is_err());
    }
}
#[test]
fn finite_field_exchange_rejects_small_order_public_keys() {
    for bits in [2048, 3072] {
        let first = rustls_legacy_crypto::dh::Key::new(bits).unwrap();
        let second = rustls_legacy_crypto::dh::Key::new(bits).unwrap();
        let a = first.public_key().to_vec();
        let b = second.public_key().to_vec();
        assert_eq!(*first.complete(&b).unwrap(), *second.complete(&a).unwrap());
        for invalid in [&[0][..], &[1][..], &[][..]] {
            assert!(rustls_legacy_crypto::dh::Key::new(bits)
                .unwrap()
                .complete(invalid)
                .is_err());
        }
    }
}
