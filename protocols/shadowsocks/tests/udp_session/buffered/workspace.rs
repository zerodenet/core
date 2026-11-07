use super::*;

#[test]
fn taken_domain_workspace_requires_fresh_storage_before_the_next_replay_admission() {
    let cipher = CipherKind::Blake3Aes128Gcm;
    let password = password(cipher);
    let codec = ShadowsocksDatagramCodec::new(cipher, &password);
    let target = Address::Domain("same.actual.source".into());
    let request = codec.encode(&target, 53, b"query").unwrap();
    let packet = crate::shared::decode_udp_wire_2022(cipher, &password, &request).unwrap();
    let make = |id| {
        crate::shared::encode_udp_response_2022(
            cipher,
            &password,
            packet.session_id,
            &target,
            53,
            b"same payload",
            (23, id),
        )
        .unwrap()
    };
    let first = make(1);
    let second = make(2);
    let lengths = codec.decode_buffer_lengths(first.len()).unwrap();
    let mut original = buffers(&lengths);
    let (held_target, _, output) = codec
        .decode_buffered(&first, &mut original)
        .unwrap()
        .unwrap();
    assert_eq!(held_target, target);
    assert_eq!(&original[output.buffer][output.range], b"same payload");
    assert!(
        original[1].is_empty(),
        "the actual unique backing allocation moved into held_target"
    );
    assert!(matches!(
        codec.decode_buffered(&second, &mut original),
        Err(DatagramCodecBufferError::Storage(_))
    ));
    // No accepted replay bit was committed by the rejected second operation.
    let mut fresh = buffers(&codec.decode_buffer_lengths(second.len()).unwrap());
    assert!(codec
        .clone()
        .decode_buffered(&second, &mut fresh)
        .unwrap()
        .is_some());
    assert!(codec
        .decode_buffered(&second, &mut buffers(&lengths))
        .unwrap()
        .is_none());
    assert_eq!(
        held_target, target,
        "fresh admission cannot invalidate the prior target backing"
    );
}
