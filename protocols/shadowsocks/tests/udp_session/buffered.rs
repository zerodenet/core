use super::*;
use zero_traits::{DatagramCodecBufferError, PacketBuffer};
fn buffers(lengths: &[usize]) -> Vec<PacketBuffer> {
    lengths.iter().map(|n| vec![0; *n].into()).collect()
}
fn methods() -> Vec<CipherKind> {
    use crate::validation::LegacyCipher::*;
    let mut all = vec![CipherKind::None];
    all.extend(
        [
            Table,
            Rc4,
            Rc4Md5,
            Chacha20,
            Aes128Ctr,
            Aes128Cfb1,
            Aes128Cfb8,
            Aes128Cfb,
            Aes128Ofb,
            Aes192Ctr,
            Aes192Cfb1,
            Aes192Cfb8,
            Aes192Cfb,
            Aes192Ofb,
            Aes256Ctr,
            Aes256Cfb1,
            Aes256Cfb8,
            Aes256Cfb,
            Aes256Ofb,
            Camellia128Ctr,
            Camellia128Cfb1,
            Camellia128Cfb8,
            Camellia128Cfb,
            Camellia128Ofb,
            Camellia192Ctr,
            Camellia192Cfb1,
            Camellia192Cfb8,
            Camellia192Cfb,
            Camellia192Ofb,
            Camellia256Ctr,
            Camellia256Cfb1,
            Camellia256Cfb8,
            Camellia256Cfb,
            Camellia256Ofb,
        ]
        .map(CipherKind::Legacy),
    );
    all.extend([
        CipherKind::Aes128Gcm,
        CipherKind::Aes256Gcm,
        CipherKind::Chacha20Poly1305,
        CipherKind::XChacha20Poly1305,
        CipherKind::Aes128Ccm,
        CipherKind::Aes256Ccm,
        CipherKind::Aes128GcmSiv,
        CipherKind::Aes256GcmSiv,
        CipherKind::Sm4Gcm,
        CipherKind::Sm4Ccm,
    ]);
    #[cfg(feature = "blake3")]
    all.extend([
        CipherKind::Blake3Aes128Gcm,
        CipherKind::Blake3Aes256Gcm,
        CipherKind::Blake3Chacha20Poly1305,
        CipherKind::Blake3Chacha8Poly1305,
    ]);
    all
}
fn password(cipher: CipherKind) -> Vec<u8> {
    #[cfg(feature = "blake3")]
    if cipher.is_blake3() {
        use base64::Engine;
        return base64::engine::general_purpose::STANDARD
            .encode(vec![7; cipher.key_len()])
            .into_bytes();
    }
    let _ = cipher;
    b"original fixed workspace password".to_vec()
}
fn response(
    codec: &ShadowsocksDatagramCodec,
    password: &[u8],
    target: &Address,
    payload: &[u8],
    wire: &[u8],
) -> Vec<u8> {
    #[cfg(feature = "blake3")]
    if codec.cipher.is_blake3() {
        let request = crate::shared::decode_udp_wire_2022(codec.cipher, password, wire).unwrap();
        assert_eq!(request.target, *target);
        assert_eq!(request.port, 53);
        assert_eq!(request.payload, payload);
        // Every call uses a new codec. Failed workspace admission must not
        // initialize or increment that codec's actual sender state.
        assert_eq!(request.packet_id, 1);
        return crate::shared::encode_udp_response_2022(
            codec.cipher,
            password,
            request.session_id,
            target,
            53,
            payload,
            (19, 1),
        )
        .unwrap();
    }
    let decoded = ShadowsocksDatagramCodec::new(codec.cipher, password)
        .decode(wire)
        .unwrap();
    assert_eq!(decoded, (target.clone(), 53, payload.to_vec()));
    ShadowsocksDatagramCodec::new(codec.cipher, password)
        .encode(target, 53, payload)
        .unwrap()
}
#[test]
fn all_existing_ss_udp_methods_use_fixed_workspaces_and_keep_authenticated_clone_state() {
    for cipher in methods() {
        let password = password(cipher);
        for target in [
            Address::Ipv4([127, 0, 0, 1]),
            Address::Ipv6([3; 16]),
            Address::Domain("same.actual.domain".into()),
        ] {
            for payload in [vec![], vec![71; 1480]] {
                let codec = ShadowsocksDatagramCodec::new(cipher, &password);
                let lengths = codec
                    .encode_buffer_lengths(&target, 53, payload.len())
                    .unwrap();
                for short in 0..lengths.len() {
                    let mut undersized = lengths.clone();
                    undersized[short] -= 1;
                    assert!(
                        matches!(
                            codec.encode_buffered(&target, 53, &payload, &mut buffers(&undersized)),
                            Err(DatagramCodecBufferError::Storage(_))
                        ),
                        "{cipher:?}"
                    );
                }
                let mut storage = buffers(&lengths);
                let output = codec
                    .encode_buffered(&target, 53, &payload, &mut storage)
                    .unwrap();
                let wire = &storage[output.buffer][output.range];
                let response = response(&codec, &password, &target, &payload, wire);
                let lengths = codec.decode_buffer_lengths(response.len()).unwrap();
                for short in 0..lengths.len() {
                    let mut undersized = lengths.clone();
                    undersized[short] -= 1;
                    assert!(matches!(
                        codec.decode_buffered(&response, &mut buffers(&undersized)),
                        Err(DatagramCodecBufferError::Storage(_))
                    ));
                }
                if !cipher.is_stream() {
                    let mut bad = response.clone();
                    let last = bad.len() - 1;
                    bad[last] ^= 1;
                    assert!(codec
                        .decode_buffered(&bad, &mut buffers(&lengths))
                        .unwrap()
                        .is_none());
                }
                let mut decoded = buffers(&lengths);
                let (actual, port, output) = codec
                    .clone()
                    .decode_buffered(&response, &mut decoded)
                    .unwrap()
                    .unwrap();
                assert_eq!(actual, target);
                assert_eq!(port, 53);
                assert_eq!(&decoded[output.buffer][output.range], payload);
                #[cfg(feature = "blake3")]
                if cipher.is_blake3() {
                    assert!(codec
                        .decode_buffered(&response, &mut buffers(&lengths))
                        .unwrap()
                        .is_none());
                }
            }
        }
    }
}
#[cfg(feature = "blake3")]
#[test]
fn fixed_workspace_ss2022_identity_chain_and_maximum_empty_padding_are_reserved() {
    use base64::Engine;
    let cipher = CipherKind::Blake3Aes128Gcm;
    let encode = |byte| base64::engine::general_purpose::STANDARD.encode([byte; 16]);
    let user = encode(7);
    let identity = encode(9);
    let codec = ShadowsocksDatagramCodec::new(cipher, format!("{identity}:{user}"));
    let target = Address::Domain("dns.same.example".into());
    let lengths = codec.encode_buffer_lengths(&target, 53, 0).unwrap();
    let target_length = 2 + "dns.same.example".len() + 2;
    assert_eq!(
        lengths,
        vec![
            11 + 899 + target_length,
            16 + 16 + 11 + 899 + target_length + 16
        ]
    );
    let mut server = crate::udp::ShadowsocksInboundUdpCodec::new_eih(
        cipher,
        identity.as_bytes(),
        user.as_bytes(),
    );
    for _ in 0..32 {
        let mut storage = buffers(&lengths);
        let output = codec
            .encode_buffered(&target, 53, &[], &mut storage)
            .unwrap();
        let packet = server
            .decode_request(&storage[output.buffer][output.range])
            .unwrap();
        assert_eq!(packet.target(), &target);
        assert!(packet.payload().is_empty());
    }
}

#[cfg(feature = "blake3")]
#[test]
fn fixed_workspace_reserves_every_actual_eih_key_before_sender_state_changes() {
    use base64::Engine;
    let cipher = CipherKind::Blake3Aes128Gcm;
    let encode = |byte| base64::engine::general_purpose::STANDARD.encode([byte; 16]);
    let password = format!("{}:{}:{}", encode(3), encode(5), encode(7));
    let codec = ShadowsocksDatagramCodec::new(cipher, &password);
    let target = Address::Ipv4([127, 0, 0, 1]);
    let lengths = codec.encode_buffer_lengths(&target, 53, 3).unwrap();
    assert_eq!(lengths, vec![11 + 7 + 3, 16 + 32 + 11 + 7 + 3 + 16]);
    let mut workspaces = buffers(&lengths);
    let output = codec
        .encode_buffered(&target, 53, b"dns", &mut workspaces)
        .unwrap();
    let wire = &workspaces[output.buffer][output.range];
    let keys = crate::shared::parse_2022_key_chain(cipher, password.as_bytes()).unwrap();
    let mut header = [0; 16];
    header.copy_from_slice(&wire[..16]);
    crate::shared::decrypt_aes_2022_header(cipher, &keys.identity_keys[0], &mut header).unwrap();
    for (i, key) in keys.identity_keys.iter().enumerate() {
        let mut identity = [0; 16];
        identity.copy_from_slice(&wire[16 + 16 * i..32 + 16 * i]);
        crate::shared::decrypt_aes_2022_header(cipher, key, &mut identity).unwrap();
        for (byte, separate) in identity.iter_mut().zip(header) {
            *byte ^= separate;
        }
        let next = keys
            .identity_keys
            .get(i + 1)
            .map_or(keys.user_key.as_slice(), Vec::as_slice);
        assert_eq!(&identity[..], &blake3::hash(next).as_bytes()[..16]);
    }
    let key =
        crate::shared::derive_key_blake3(&keys.user_key, &header[..8], cipher.key_len()).unwrap();
    let body = crate::shared::aead_decrypt_udp(cipher, &key, &header[4..16], &wire[48..]).unwrap();
    assert_eq!(
        &body[11..],
        &crate::shared::build_target_data(&target, 53, b"dns").unwrap()
    );
    assert_eq!(u64::from_be_bytes(header[8..].try_into().unwrap()), 1);
}
