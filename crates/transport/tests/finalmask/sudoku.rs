use super::*;
#[test]
fn official_datagrams_decode_all_byte_values_in_all_layouts() {
    let cases: [(&[u8], &str, Vec<&str>); 4] = [
        (include_bytes!("vectors/entropy.bin"), "", vec![]),
        (include_bytes!("vectors/ascii.bin"), "ascii", vec![]),
        (include_bytes!("vectors/custom.bin"), "", vec!["xxppvvvv"]),
        (
            include_bytes!("vectors/rotation.bin"),
            "",
            vec!["xxppvvvv", "xpxpvvvv"],
        ),
    ];
    for (wire, ascii, patterns) in cases {
        let profile = Profile::new(&Settings {
            password: "reference".into(),
            ascii: ascii.into(),
            custom_tables: patterns.into_iter().map(str::to_owned).collect(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            profile.decode_datagram(wire).unwrap(),
            (0..=255).collect::<Vec<u8>>()
        );
        let mut decoder = profile.decoder();
        let mut decoded = Vec::new();
        for chunk in wire.chunks(7) {
            decoded.extend(decoder.decode(chunk).unwrap());
        }
        assert_eq!(decoded, (0..=255).collect::<Vec<u8>>());
        let encoded = profile.encode_datagram(&decoded);
        assert_eq!(profile.decode_datagram(&encoded).unwrap(), decoded);
    }
}
#[test]
fn packed_downlink_preserves_partial_write_boundaries_and_rotating_layouts() {
    for ascii in ["entropy", "ascii"] {
        let profile = Profile::new(&Settings {
            ascii: ascii.into(),
            custom_tables: vec!["xxppvvvv".into(), "xpxpvvvv".into()],
            padding_min: 100,
            padding_max: 100,
            ..Default::default()
        })
        .unwrap();
        let mut encoder = profile.packed_encoder();
        let mut decoder = profile.packed_decoder();
        let mut wire = Vec::new();
        let mut expected = Vec::new();
        for length in [1, 2, 3, 7, 16, 1025] {
            let bytes: Vec<_> = (0..length).map(|i| (i % 256) as u8).collect();
            wire.extend(encoder.encode(&bytes));
            expected.extend(bytes);
        }
        let mut output = Vec::new();
        for byte in wire {
            output.extend(decoder.decode(&[byte]));
        }
        assert_eq!(output, expected);
    }
}
#[test]
fn datagram_boundary_rejects_partial_hint_tuples_and_resets_table_rotation() {
    let profile = Profile::new(&Settings {
        password: "reference".into(),
        ..Default::default()
    })
    .unwrap();
    let wire = profile.encode_datagram(&[1, 2, 3]);
    assert!(profile.decode_datagram(&wire[..wire.len() - 1]).is_err());
    assert_eq!(
        profile
            .decode_datagram(&profile.encode_datagram(&[0xff]))
            .unwrap(),
        [0xff]
    );
}
