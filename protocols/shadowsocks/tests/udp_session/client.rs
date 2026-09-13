use super::*;
use crate::udp::ShadowsocksInboundUdpCodec;
use base64::Engine;
fn ciphers() -> [CipherKind; 4] {
    [
        CipherKind::Blake3Aes128Gcm,
        CipherKind::Blake3Aes256Gcm,
        CipherKind::Blake3Chacha20Poly1305,
        CipherKind::Blake3Chacha8Poly1305,
    ]
}
fn password(cipher: CipherKind) -> String {
    base64::engine::general_purpose::STANDARD.encode(vec![7; cipher.key_len()])
}

#[test]
fn continuous_association_exceeds_old_1024_packet_limit_for_all_2022_ciphers() {
    for cipher in ciphers() {
        let password = password(cipher);
        let client = ShadowsocksDatagramCodec::new(cipher, &password);
        let clone = client.clone();
        let mut server = ShadowsocksInboundUdpCodec::new(cipher, password.as_bytes());
        let target = Address::Domain("example.com".into());
        let mut client_id = None;
        let mut server_id = None;
        for seq in 1..=2050 {
            let request = clone.encode(&target, 53, b"request").unwrap();
            let decoded =
                crate::shared::decode_udp_wire_2022(cipher, password.as_bytes(), &request).unwrap();
            assert_eq!(decoded.packet_id, seq);
            assert_eq!(
                *client_id.get_or_insert(decoded.session_id),
                decoded.session_id
            );
            let packet = server.decode_request(&request).unwrap();
            let response = server
                .encode_response(packet.client_session_id(), &target, 53, b"response")
                .unwrap();
            let decoded =
                crate::shared::decode_udp_wire_2022(cipher, password.as_bytes(), &response)
                    .unwrap();
            assert_eq!(decoded.packet_id, seq);
            assert_eq!(
                *server_id.get_or_insert(decoded.session_id),
                decoded.session_id
            );
            assert_eq!(client.decode(&response).unwrap().2, b"response");
            assert!(clone.decode(&response).is_none());
        }
    }
}

#[test]
fn direction_echo_and_window_are_checked_before_response_delivery() {
    for cipher in ciphers() {
        let password = password(cipher);
        let client = ShadowsocksDatagramCodec::new(cipher, &password);
        let mut server = ShadowsocksInboundUdpCodec::new(cipher, password.as_bytes());
        let target = Address::Ipv4([1, 2, 3, 4]);
        let request = client.encode(&target, 53, b"request").unwrap();
        assert!(client.decode(&request).is_none());
        let incoming = server.decode_request(&request).unwrap();
        let id = incoming.client_session_id().unwrap();
        let make = |echo, session, packet| {
            crate::shared::encode_udp_response_2022(
                cipher,
                password.as_bytes(),
                echo,
                &target,
                53,
                b"reply",
                (session, packet),
            )
            .unwrap()
        };
        let wrong_echo = make(id.wrapping_add(1), 10, 2);
        assert!(client.decode(&wrong_echo).is_none());
        let valid = make(id, 10, 2);
        assert!(server.decode_request(&valid).is_err());
        assert!(client.decode(&valid).is_some());
        assert!(client.decode(&valid).is_none());
        assert!(client.decode(&make(id, 10, 1)).is_some()); // in-window reordering
        assert!(client.decode(&make(id, 10, 16384)).is_some());
        assert!(client.decode(&make(id, 10, 3)).is_none()); // too old
        assert!(client.decode(&make(id, 11, 1)).is_some()); // independent server session
    }
}

#[test]
fn eih_requests_share_state_and_responses_verify_the_user_session() {
    let cipher = CipherKind::Blake3Aes128Gcm;
    let user = password(cipher);
    let identity = base64::engine::general_purpose::STANDARD.encode([9; 16]);
    let client = ShadowsocksDatagramCodec::new(cipher, format!("{identity}:{user}"));
    let mut server =
        ShadowsocksInboundUdpCodec::new_eih(cipher, identity.as_bytes(), user.as_bytes());
    for _ in 0..3 {
        let target = Address::Ipv4([1, 2, 3, 4]);
        let request = client.encode(&target, 53, b"query").unwrap();
        let packet = server.decode_request(&request).unwrap();
        let response = server
            .encode_response(packet.client_session_id(), &target, 53, b"reply")
            .unwrap();
        assert_eq!(client.decode(&response).unwrap().2, b"reply");
        assert!(client.decode(&response).is_none());
    }
}

#[test]
fn large_2022_payload_is_preserved_by_session_codecs() {
    for cipher in ciphers() {
        let password = password(cipher);
        let client = ShadowsocksDatagramCodec::new(cipher, &password);
        let mut server = ShadowsocksInboundUdpCodec::new(cipher, password.as_bytes());
        let target = Address::Ipv4([1, 2, 3, 4]);
        let payload = vec![9; 60_000];
        let request = client.encode(&target, 53, &payload).unwrap();
        let incoming = server.decode_request(&request).unwrap();
        assert_eq!(incoming.payload(), payload);
        let response = server
            .encode_response(incoming.client_session_id(), &target, 53, &payload)
            .unwrap();
        assert_eq!(client.decode(&response).unwrap().2, payload);
    }
}

#[test]
fn resume_packet_api_keeps_its_association_across_clones() {
    let cipher = CipherKind::Blake3Aes128Gcm;
    let password = password(cipher);
    let resume =
        crate::udp::ShadowsocksUdpFlowResume::new("test".into(), cipher, password.as_bytes());
    let clone = resume.clone();
    let mut server = ShadowsocksInboundUdpCodec::new(cipher, password.as_bytes());
    let target = Address::Ipv4([1, 2, 3, 4]);
    let packet = crate::udp::ShadowsocksUdpFlowPacket::from_parts(&target, 53, b"query");
    let mut id = None;
    for owner in [&resume, &clone] {
        let request = packet.encode_with(owner).unwrap();
        let incoming = server.decode_request(&request).unwrap();
        assert_eq!(
            *id.get_or_insert(incoming.client_session_id()),
            incoming.client_session_id()
        );
        let response = server
            .encode_response(incoming.client_session_id(), &target, 53, b"answer")
            .unwrap();
        assert_eq!(
            owner.decode_flow_packet(&response).unwrap().payload(),
            b"answer"
        );
        assert!(resume.decode_flow_packet(&response).is_none());
    }
}

#[test]
#[cfg(feature = "runtime")]
fn repeated_packet_path_builds_allocate_independent_associations() {
    let cipher = CipherKind::Blake3Aes128Gcm;
    let password = password(cipher);
    let plan = crate::transport::ShadowsocksTransportLeaf::new(
        "ss",
        "127.0.0.1",
        8388,
        "2022-blake3-aes-128-gcm",
        &password,
    )
    .udp_packet_path_plan()
    .unwrap();
    let first = plan.carrier_codec();
    let clone = first.clone();
    let second = plan.clone().carrier_codec();
    let third = plan
        .clone()
        .into_datagram_source_build()
        .into_shared_codec_parts()
        .4;
    let fourth = plan
        .into_datagram_source_build()
        .into_shared_codec_parts()
        .4;
    let target = Address::Ipv4([1, 2, 3, 4]);
    let mut ids = std::collections::HashSet::new();
    for codec in [first, second, third, fourth] {
        let packet = codec.encode(&target, 53, b"query").unwrap();
        let decoded =
            crate::shared::decode_udp_wire_2022(cipher, password.as_bytes(), &packet).unwrap();
        assert_eq!(decoded.packet_id, 1);
        assert!(
            ids.insert(decoded.session_id),
            "a new carrier reused another carrier's sender session"
        );
    }
    let packet = clone.encode(&target, 53, b"next").unwrap();
    let decoded =
        crate::shared::decode_udp_wire_2022(cipher, password.as_bytes(), &packet).unwrap();
    assert_eq!(decoded.packet_id, 2);
    assert!(ids.contains(&decoded.session_id));
}
