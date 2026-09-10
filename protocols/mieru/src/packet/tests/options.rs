use super::PacketCodec;
fn data(sequence: u32, ack: u32, window: u16, payload: &[u8]) -> crate::segment::Segment {
    let mut packet = super::data(sequence, ack, window, payload);
    packet.data_meta.as_mut().unwrap().timestamp =
        crate::session::MieruSession::timestamp_minutes();
    packet
}
use mieru_config::{
    MieruNoncePatternConfig, MieruNonceType, MieruTrafficPatternConfig, MieruTransportOptions,
};

fn fixed_options(all: bool) -> MieruTransportOptions {
    MieruTransportOptions {
        mtu: 1280,
        traffic_pattern: Some(MieruTrafficPatternConfig {
            seed: Some(1),
            nonce: Some(MieruNoncePatternConfig {
                kind: Some(MieruNonceType::Fixed),
                apply_to_all_udp_packet: Some(all),
                custom_hex_strings: vec!["000102030405060708090a0b".into()],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn udp_wire_stays_within_configured_outer_mtu_for_both_ip_families() {
    for ipv6 in [false, true] {
        for mtu in [1280, 1400, 1500] {
            let options = MieruTransportOptions {
                mtu,
                ..Default::default()
            };
            let codec = PacketCodec::configured([7; 32], "user", &options, ipv6).unwrap();
            let payload = vec![42; options.fragment_size(ipv6)];
            let wire = codec.encode(&data(0, 0, 128, &payload)).unwrap();
            assert_eq!(wire.len(), options.udp_payload_limit(ipv6));
            assert_eq!(codec.decode(&wire).unwrap().payload, payload);
            assert!(codec
                .encode(&data(0, 0, 128, &vec![42; payload.len() + 1]))
                .is_err());
            for size in [0, 1, 127] {
                for _ in 0..32 {
                    let wire = codec.encode(&data(1, 0, 128, &vec![11; size])).unwrap();
                    assert!(wire.len() <= options.udp_payload_limit(ipv6));
                    assert_eq!(codec.decode(&wire).unwrap().payload.len(), size);
                }
            }
        }
    }
}

#[test]
fn udp_nonce_pattern_first_packet_state_is_shared_with_codec_clones() {
    let prefix: Vec<u8> = (0..12).collect();
    for all in [false, true] {
        let codec = PacketCodec::configured([7; 32], "user", &fixed_options(all), false).unwrap();
        let clone = codec.clone();
        let packet = data(0, 0, 8, b"hello");
        assert_eq!(&codec.encode(&packet).unwrap()[..12], prefix);
        for _ in 0..8 {
            let wire = clone.encode(&packet).unwrap();
            assert_eq!(wire[..12] == prefix, all);
            assert_eq!(codec.decode(&wire).unwrap().payload, b"hello");
        }
    }
}

#[test]
fn receive_accepts_peer_mtu_larger_than_local_send_budget() {
    let small = PacketCodec::configured([7; 32], "u", &fixed_options(false), true).unwrap();
    let options = MieruTransportOptions {
        mtu: 1500,
        ..Default::default()
    };
    let large = PacketCodec::configured([7; 32], "u", &options, false).unwrap();
    let payload = vec![31; options.fragment_size(false)];
    assert_eq!(
        small
            .decode(&large.encode(&data(0, 0, 8, &payload)).unwrap())
            .unwrap()
            .payload,
        payload
    );
}
