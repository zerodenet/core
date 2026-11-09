use super::*;
use crate::{shared, udp::client::ShadowsocksDatagramCodec};
use zero_core::Address;
use zero_traits::{DatagramCodec, DatagramCodecBufferError, PacketBuffer};

#[test]
fn borrowed_chain_and_stack_kdf_match_original_all_2022_methods_and_eih_depths() {
    for cipher in [
        CipherKind::Blake3Aes128Gcm,
        CipherKind::Blake3Aes256Gcm,
        CipherKind::Blake3Chacha20Poly1305,
        CipherKind::Blake3Chacha8Poly1305,
    ] {
        for padding in [true, false] {
            for depth in [0, 1, 2, 128] {
                if cipher.is_2022_chacha() && depth != 0 {
                    continue;
                }
                let parts: Vec<_> = (0..=depth)
                    .map(|n| {
                        let encoded = ENGINE.encode(vec![n as u8; cipher.key_len()]);
                        if padding {
                            encoded
                        } else {
                            encoded.trim_end_matches('=').to_owned()
                        }
                    })
                    .collect();
                let password = parts.join(":");
                let old = shared::parse_2022_key_chain(cipher, password.as_bytes()).unwrap();
                let actual = Chain::new(cipher, password.as_bytes()).unwrap();
                assert_eq!(actual.identity_count(), depth);
                assert_eq!(actual.user.as_ref(), old.user_key);
                for (actual, expected) in actual.identities().zip(&old.identity_keys) {
                    assert_eq!(actual.unwrap().as_ref(), expected);
                }
                for material in [&[][..], &[5; 8][..], &[7; 32][..]] {
                    assert_eq!(
                        session_key(&actual.user, material).as_ref(),
                        shared::derive_key_blake3(&old.user_key, material, cipher.key_len())
                            .unwrap()
                    );
                }
            }
        }
    }
}

#[test]
fn malformed_and_oversized_keychains_preserve_original_validation_errors() {
    let user = ENGINE.encode([7; 16]);
    let oversized = ENGINE.encode([9; 1500]);
    let mut bad_tail = oversized.clone();
    bad_tail.push('!');
    let mut padded_middle = "AAAA=".to_owned();
    padded_middle.push_str(&oversized);
    let inputs = [
        vec![255],
        b"".to_vec(),
        b":".to_vec(),
        b"!".to_vec(),
        format!("{user}:").into_bytes(),
        format!(":{user}").into_bytes(),
        format!("bad:{user}").into_bytes(),
        format!("{user}:bad").into_bytes(),
        oversized.into_bytes(),
        bad_tail.into_bytes(),
        padded_middle.into_bytes(),
        b"AAAAAB==".to_vec(),
        b"AAAAA".to_vec(),
    ];
    for cipher in [
        CipherKind::Blake3Aes128Gcm,
        CipherKind::Blake3Chacha20Poly1305,
    ] {
        for password in &inputs {
            let expected = match shared::parse_2022_key_chain(cipher, password) {
                Err(error) => error.to_string(),
                Ok(_) => panic!("malformed fixture unexpectedly accepted"),
            };
            match Chain::new(cipher, password) {
                Err(error) => assert_eq!(error.to_string(), expected),
                Ok(_) => panic!("borrowed parser accepted malformed fixture"),
            }
        }
    }
}

#[test]
fn malformed_identity_and_short_storage_do_not_initialize_or_advance_actual_sender() {
    let cipher = CipherKind::Blake3Aes128Gcm;
    let password = ENGINE.encode([7; 16]);
    let mut codec = ShadowsocksDatagramCodec::new(cipher, format!("invalid:{password}"));
    let target = Address::Ipv4([127, 0, 0, 1]);
    let mut arbitrary: [PacketBuffer; 2] = [vec![0; 4096].into(), vec![0; 4096].into()];
    assert!(matches!(
        codec.encode_buffered(&target, 53, b"first actual request", &mut arbitrary),
        Err(DatagramCodecBufferError::Protocol(_))
    ));
    // Reuse that exact session after fixing only the invalid configuration.
    codec.password = password.as_bytes().to_vec();
    let layout = codec.encode_buffer_layout(&target, 53, 20).unwrap();
    for short in 0..2 {
        let mut buffers: [PacketBuffer; 2] = [vec![0; layout[0]].into(), vec![0; layout[1]].into()];
        buffers[short] = vec![0; layout[short] - 1].into();
        assert!(matches!(
            codec.encode_buffered(&target, 53, b"first actual request", &mut buffers),
            Err(DatagramCodecBufferError::Storage(_))
        ));
    }
    let mut buffers: [PacketBuffer; 2] = [vec![0; layout[0]].into(), vec![0; layout[1]].into()];
    let output = codec
        .encode_buffered(&target, 53, b"first actual request", &mut buffers)
        .unwrap();
    let request = shared::decode_udp_wire_2022(
        cipher,
        password.as_bytes(),
        &buffers[output.buffer][output.range],
    )
    .unwrap();
    assert_eq!(request.packet_id, 1);
    assert_eq!(request.payload, b"first actual request");
}
