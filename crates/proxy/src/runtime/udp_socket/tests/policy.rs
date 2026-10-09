use super::*;

#[tokio::test]
async fn udp_reload_retires_only_changed_tag_and_rejects_old_policy_completion() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let first = policy(&proxy, Some("first"));
    let second = policy(&proxy, Some("second"));
    let mut buffer = [0; 32];
    for (session, policy) in [(1, &first), (2, &second)] {
        sockets
            .send_to_addr(b"before", target, session, policy)
            .await
            .unwrap();
        peer.recv_from(&mut buffer).await.unwrap();
    }
    let retained_id = sockets
        .sockets
        .iter()
        .find(|socket| socket.binding.policy == second)
        .unwrap()
        .id;
    proxy
        .engine()
        .reload_runtime_config(config("only_ipv4", "auto"))
        .unwrap();
    sockets.refresh_if_stale();
    assert_eq!(sockets.sockets.len(), 1);
    assert_eq!(sockets.sockets[0].id, retained_id);
    assert!(sockets
        .send_to_addr(b"stale", target, 1, &first)
        .await
        .is_err());
    assert_eq!(sockets.sockets.len(), 1);
    sockets
        .send_to_addr(b"unchanged", target, 2, &second)
        .await
        .unwrap();
    let (len, _) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..len], b"unchanged");
    assert_eq!(sockets.sockets[0].id, retained_id);
    // Returning to identical values must not revive a prepared old generation.
    proxy
        .engine()
        .reload_runtime_config(config("auto", "auto"))
        .unwrap();
    let current_first = policy(&proxy, Some("first"));
    assert_ne!(first.generation, current_first.generation);
    assert!(sockets
        .send_to_addr(b"stale again", target, 1, &first)
        .await
        .is_err());
    sockets
        .send_to_addr(b"current", target, 3, &current_first)
        .await
        .unwrap();
    let (len, _) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..len], b"current");
    assert_eq!(sockets.sockets.len(), 2);
}

#[tokio::test]
async fn forbidden_family_and_mapped_addresses_never_open_a_socket() {
    let proxy = crate::runtime::Proxy::new(config("only_ipv6", "only_ipv4")).unwrap();
    let mut sockets = socket_set(&proxy, None);
    let v6 = policy(&proxy, Some("first"));
    for target in ["127.0.0.1:53", "[::ffff:127.0.0.1]:53"] {
        assert!(sockets
            .send_to_addr(b"no", target.parse().unwrap(), 1, &v6)
            .await
            .is_err());
    }
    let v4 = policy(&proxy, Some("second"));
    assert!(sockets
        .send_to_addr(b"no", "[::1]:53".parse().unwrap(), 2, &v4)
        .await
        .is_err());
    assert!(sockets.sockets.is_empty());
}

#[tokio::test]
async fn default_route_is_distinct_from_explicit_tag_named_direct() {
    let config = zero_config::RuntimeConfig::parse(r#"{
        "outbounds": [{"tag":"direct","protocol":{"type":"direct"},"dial":{"address_family":"only_ipv6"}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let mut sockets = socket_set(&proxy, None);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    sockets
        .send_to_addr(b"implicit", target, 1, &policy(&proxy, None))
        .await
        .unwrap();
    assert!(sockets
        .send_to_addr(b"explicit", target, 2, &policy(&proxy, Some("direct")))
        .await
        .is_err());
    assert_eq!(sockets.sockets.len(), 1);
}

#[tokio::test]
async fn source_ip_survives_preferred_port_collision_and_mapped_peer_normalization() {
    let config = zero_config::RuntimeConfig::parse(r#"{
        "outbounds": [{"tag":"bound","protocol":{"type":"direct"},"dial":{"source_ip":"127.0.0.1"}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let occupied = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let preferred = occupied.local_addr().unwrap().port();
    let mut sockets = socket_set(&proxy, Some(preferred));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let mapped = SocketAddr::new(
        std::net::IpAddr::V6(std::net::Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
        target.port(),
    );
    let bound = policy(&proxy, Some("bound"));
    sockets
        .send_to_addr(b"bound", mapped, 1, &bound)
        .await
        .unwrap();
    let mut buffer = [0; 32];
    let (_, source) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(
        source.ip(),
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
    );
    assert_ne!(source.port(), preferred);
    assert_eq!(sockets.sockets.len(), 1);
    assert!(!sockets.sockets[0].binding.ipv6);
    assert_eq!(
        sockets.sockets[0].socket.local_addr().unwrap().ip(),
        source.ip()
    );
    assert!(sockets
        .send_to_addr(b"forbidden", "[::1]:53".parse().unwrap(), 1, &bound)
        .await
        .is_err());
    assert_eq!(sockets.sockets.len(), 1);
    peer.send_to(b"response", source).await.unwrap();
    let (_, response) = sockets.recv_from_addr(&mut buffer).await.unwrap();
    assert_eq!(response.sender, target);
    assert_eq!(response.session_id, Some(1));
}

#[tokio::test]
async fn completed_send_stays_successful_after_reload_without_registering_stale_reply() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let before = policy(&proxy, Some("first"));
    let index = sockets.socket_for(target, 1, &before).await.unwrap();
    proxy
        .engine()
        .reload_runtime_config(config("only_ipv4", "auto"))
        .unwrap();
    // Model a successful kernel send whose completion arrives after reload.
    // Its result must remain success, or outbound fallback would replay data.
    assert_eq!(sockets.record_sent_packet(index, target, 1, &before, 7), 7);
    assert!(sockets.response_flows.is_empty());
}
