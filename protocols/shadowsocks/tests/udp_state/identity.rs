use super::*;
use crate::{
    shared, udp::ShadowsocksInboundUdpResponder, CipherKind, ShadowsocksInboundProfile,
    ShadowsocksInboundUserRef,
};
use zero_core::Address;

#[tokio::test]
async fn sip023_same_wire_session_is_isolated_between_users_and_migrates_per_user() {
    let identity = "MDEyMzQ1Njc4OWFiY2RlZg==";
    let keys = ["ZmVkY2JhOTg3NjU0MzIxMA==", "YWJjZGVmMDEyMzQ1Njc4OQ=="];
    let users = keys.map(|password| ShadowsocksInboundUserRef {
        password,
        principal_key: Some(password),
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: None,
    });
    let profile = ShadowsocksInboundProfile::from_config_users_with_identity(
        "2022-blake3-aes-128-gcm",
        Some(identity),
        users,
    )
    .unwrap();
    let mut responder = ShadowsocksInboundUdpResponder::from_profile(profile);
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut ids = Vec::new();
    let mut clients = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let password = format!("{identity}:{key}");
        let packet = shared::encode_udp_request_with_session(
            CipherKind::Blake3Aes128Gcm,
            password.as_bytes(),
            &Address::Ipv4([127, 0, 0, 1]),
            53,
            b"first",
            (99, 1),
        )
        .unwrap();
        client
            .send_to(&packet, socket.local_addr().unwrap())
            .await
            .unwrap();
        let dispatch = tokio::time::timeout(
            Duration::from_secs(1),
            responder.read_inbound_dispatch_from_socket_tokio(&socket),
        )
        .await
        .unwrap()
        .unwrap();
        ids.push(dispatch.client_session_id().unwrap());
        responder.record_pending_dispatch_success(index as u64 + 1, dispatch.client_session_id());
        clients.push(client);
    }
    assert_ne!(ids[0], ids[1]);
    let moved = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let packet = shared::encode_udp_request_with_session(
        CipherKind::Blake3Aes128Gcm,
        format!("{identity}:{}", keys[0]).as_bytes(),
        &Address::Ipv4([127, 0, 0, 1]),
        54,
        b"moved",
        (99, 2),
    )
    .unwrap();
    moved
        .send_to(&packet, socket.local_addr().unwrap())
        .await
        .unwrap();
    let dispatch = responder
        .read_inbound_dispatch_from_socket_tokio(&socket)
        .await
        .unwrap();
    assert_eq!(dispatch.client_session_id(), Some(ids[0]));
    responder.record_pending_dispatch_success(3, dispatch.client_session_id());
    for (flow, client, key) in [(1, &moved, keys[0]), (2, &clients[1], keys[1])] {
        responder
            .send_response_for_target_proxy_session_to_client_tokio(
                &socket,
                Some(flow),
                &Address::Ipv4([127, 0, 0, 1]),
                53,
                b"",
            )
            .await
            .unwrap();
        let mut bytes = [0; 2048];
        let (size, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let decoded = shared::decode_udp_wire_2022(
            CipherKind::Blake3Aes128Gcm,
            key.as_bytes(),
            &bytes[..size],
        )
        .unwrap();
        assert_eq!(decoded.client_session_id, Some(99));
        assert!(decoded.payload.is_empty());
        let other = if key == keys[0] { keys[1] } else { keys[0] };
        assert!(shared::decode_udp_wire_2022(
            CipherKind::Blake3Aes128Gcm,
            other.as_bytes(),
            &bytes[..size]
        )
        .is_err());
    }
}
