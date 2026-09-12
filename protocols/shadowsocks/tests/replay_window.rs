#![cfg(all(feature = "runtime", feature = "blake3"))]

use shadowsocks::{CipherKind, ReplayWindow};

#[test]
fn zero_and_inclusive_left_edge_remain_recorded() {
    let mut window = ReplayWindow::new_with_window(2048);
    assert!(window.check_and_update(0));
    assert!(!window.check_and_update(0));
    assert!(window.check_and_update(4096));
    assert!(!window.check_and_update(2047));
    assert!(window.check_and_update(2048));
    assert!(!window.check_and_update(2048));
    assert!(window.check_and_update(4097));
    assert!(!window.check_and_update(2048));
}

#[test]
fn zero_width_and_maximum_packet_ids_never_forget_current_packet() {
    let mut zero = ReplayWindow::new_with_window(0);
    assert!(zero.check_and_update(0));
    assert!(!zero.check_and_update(0));
    assert!(zero.check_and_update(1));
    assert!(!zero.check_and_update(0));
    assert!(!zero.check_and_update(1));
    let mut high = ReplayWindow::new_with_window(2048);
    assert!(!high.check_and_update(u64::MAX));
    assert!(high.check_and_update(u64::MAX - 1));
    assert!(!high.check_and_update(u64::MAX));
    assert!(high.check_and_update(u64::MAX - 2049));
    assert!(!high.check_and_update(u64::MAX - 2049));
    assert!(!high.check_and_update(u64::MAX - 2050));
}

fn packet(session: u64, id: u64) -> Vec<u8> {
    use aes::cipher::{BlockEncrypt, KeyInit};
    let master = b"0123456789abcdef";
    let mut header = [0u8; 16];
    header[..8].copy_from_slice(&session.to_be_bytes());
    header[8..].copy_from_slice(&id.to_be_bytes());
    let key = shadowsocks::derive_key_blake3(master, &header[..8], 16).unwrap();
    let mut plain = vec![0];
    plain.extend_from_slice(&shadowsocks::now_unix_seconds().to_be_bytes());
    plain.extend_from_slice(&[0, 0]);
    plain.extend_from_slice(
        &shadowsocks::build_target_data(
            &zero_core::Address::Domain("example.com".into()),
            53,
            b"query",
        )
        .unwrap(),
    );
    let body = shadowsocks::aead_encrypt(
        CipherKind::Blake3Aes128Gcm,
        &key,
        header[4..].try_into().unwrap(),
        &plain,
    )
    .unwrap();
    let mut encrypted = aes::cipher::Block::<aes::Aes128>::clone_from_slice(&header);
    aes::Aes128::new_from_slice(master)
        .unwrap()
        .encrypt_block(&mut encrypted);
    [encrypted.as_slice(), &body].concat()
}

#[test]
fn authenticated_udp_codec_rejects_boundary_replays_and_isolates_sessions() {
    let mut codec = shadowsocks::udp::ShadowsocksInboundUdpCodec::new(
        CipherKind::Blake3Aes128Gcm,
        b"MDEyMzQ1Njc4OWFiY2RlZg==",
    );
    for (session, id, accepted) in [
        (1, 0, true),
        (1, 0, false),
        (1, 4096, true),
        (1, 2048, true),
        (1, 2048, false),
        (2, 0, true),
        (2, 0, false),
    ] {
        assert_eq!(
            codec.decode_request(&packet(session, id)).is_ok(),
            accepted,
            "session={session} id={id}"
        );
    }
}

#[test]
fn reference_default_accepts_reordering_through_8128_and_rejects_terminal_id() {
    let mut window = ReplayWindow::new();
    assert!(window.check_and_update(20000));
    assert!(window.check_and_update(11872));
    assert!(!window.check_and_update(11871));
    assert!(!window.check_and_update(u64::MAX));
}

#[test]
fn bitmap_matches_an_exact_set_across_rotations_reordering_and_large_jumps() {
    let mut bitmap = ReplayWindow::new();
    let mut reference = std::collections::BTreeSet::new();
    let mut seed = 7u64;
    for step in 0..30000u64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let packet = if step % 503 == 0 {
            step * 100
        } else {
            (step * 100).saturating_sub(seed % 10000)
        };
        let ceiling = reference.last().copied().unwrap_or(packet).max(packet);
        let expected = ceiling - packet <= 8128 && !reference.contains(&packet);
        if expected {
            reference.insert(packet);
            reference.retain(|id| *id >= ceiling.saturating_sub(8128));
        }
        assert_eq!(bitmap.check_and_update(packet), expected, "packet={packet}");
    }
}
