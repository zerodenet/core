use super::*;
#[test]
fn headers_have_pinned_wire_order_and_stateful_dtls_sequence() {
    let masks = [
        Mask::Wireguard,
        Mask::Dtls,
        Mask::Custom {
            client: vec![Item::Bytes(vec![0x42, 0x43])],
            server: vec![Item::Bytes(vec![0x44, 0x45])],
        },
    ];
    let mut client = Codec::new(&masks, false).unwrap();
    let mut server = Codec::new(&masks, true).unwrap();
    for (sequence, length) in [(0, 17), (1, 34), (2, 51)] {
        let bytes = client.encode(b"packet").unwrap();
        assert_eq!(&bytes[..7], &[4, 0, 0, 0, 23, 254, 253]);
        assert_eq!(&bytes[9..11], &[0, 0]);
        assert_eq!(&bytes[11..15], &u32::to_be_bytes(sequence));
        assert_eq!(&bytes[15..19], &[0, length, 0x42, 0x43]);
        assert_eq!(server.decode(&bytes).unwrap(), b"packet");
    }
    let response = server.encode(b"reply").unwrap();
    assert_eq!(client.decode(&response).unwrap(), b"reply");
    let mut invalid = response;
    invalid[17] ^= 1;
    assert!(client.decode(&invalid).is_err());
}
#[test]
fn crypto_masks_preserve_binary_payload_and_reject_corruption() {
    let payload: Vec<_> = (0..1500).map(|i| (i % 256) as u8).collect();
    for mask in [
        Mask::MkcpOriginal,
        Mask::MkcpAes128Gcm {
            password: "key".into(),
        },
        Mask::Salamander {
            password: "key-long".into(),
        },
    ] {
        let mut codec = Codec::new(std::slice::from_ref(&mask), false).unwrap();
        let wire = codec.encode(&payload).unwrap();
        assert_eq!(codec.decode(&wire).unwrap(), payload);
        for length in 0..6 {
            assert!(codec.decode(&wire[..length]).is_err());
        }
        if !matches!(mask, Mask::Salamander { .. }) {
            let mut wire = wire;
            *wire.last_mut().unwrap() ^= 1;
            assert!(codec.decode(&wire).is_err());
        }
    }
}
#[test]
fn nested_crypto_and_headers_decode_in_configured_order() {
    for masks in [
        vec![
            Mask::Wireguard,
            Mask::MkcpAes128Gcm {
                password: "a".into(),
            },
            Mask::Srtp,
        ],
        vec![
            Mask::Salamander {
                password: "secret".into(),
            },
            Mask::MkcpOriginal,
            Mask::Dns {
                domain: "www.example.com".into(),
            },
        ],
    ] {
        let mut encoder = Codec::new(&masks, false).unwrap();
        let mut decoder = Codec::new(&masks, true).unwrap();
        let wire = encoder.encode(&[0xff; 1300]).unwrap();
        assert_eq!(decoder.decode(&wire).unwrap(), [0xff; 1300]);
        assert!(encoder.encode(&[0; 4096]).is_err());
    }
}
#[test]
fn dns_header_preserves_reference_defaults_empty_labels_and_escaping() {
    for (domain, name) in [
        ("", b"\x03www\x05baidu\x03com\0".as_slice()),
        ("a..b.", b"\x01a\0\x01b\0\0".as_slice()),
        (r"a\.b.c", b"\x03a.b\x01c\0".as_slice()),
        (r"a\", b"\0".as_slice()),
    ] {
        let mut codec = Codec::new(
            &[Mask::Dns {
                domain: domain.into(),
            }],
            false,
        )
        .unwrap();
        let wire = codec.encode(b"payload").unwrap();
        assert_eq!(&wire[12..12 + name.len()], name);
        assert_eq!(&wire[12 + name.len()..12 + name.len() + 4], &[0, 1, 0, 1]);
    }
}
