use super::*;

async fn available_port() -> u16 {
    let reservation = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    reservation.local_addr().unwrap().port()
}

#[tokio::test]
async fn default_auto_reuses_preferred_port_through_repeated_final_flow_retirement() {
    let proxy = proxy();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let preferred = available_port().await;
    let mut sockets = socket_set(&proxy, Some(preferred));
    let policy = policy(&proxy, Some("first"));
    let mut buffer = [0; 32];
    for session in 1..=512 {
        sockets
            .send_to_addr(b"new flow", target, session, &policy)
            .await
            .unwrap();
        let (_, local) = peer.recv_from(&mut buffer).await.unwrap();
        assert_eq!(local.port(), preferred);
        sockets.retire_session(session);
        assert!(sockets.sockets.is_empty());
        assert!(sockets.response_flows.is_empty());
    }
}

#[tokio::test]
async fn policy_reload_discards_old_socket_queue_and_allows_normal_port_reuse() {
    let proxy = proxy();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let preferred = available_port().await;
    let mut sockets = socket_set(&proxy, Some(preferred));
    let old_policy = policy(&proxy, Some("first"));
    let mut buffer = [0; 32];
    sockets
        .send_to_addr(b"old policy", target, 1, &old_policy)
        .await
        .unwrap();
    let (_, old_local) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(old_local.port(), preferred);
    // This packet is already queued on the old socket before reload. Unlike
    // an arbitrarily delayed network packet, its old ownership is knowable.
    peer.send_to(b"queued old reply", old_local).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    proxy
        .engine()
        .reload_runtime_config(config("only_ipv4", "auto"))
        .unwrap();
    let new_policy = policy(&proxy, Some("first"));
    sockets
        .send_to_addr(b"new policy", target, 2, &new_policy)
        .await
        .unwrap();
    let (_, new_local) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(new_local.port(), preferred);
    peer.send_to(b"current reply", new_local).await.unwrap();
    let (len, response) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sockets.recv_from_addr(&mut buffer),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.session_id, Some(2));
    assert_eq!(&buffer[..len], b"current reply");
}

#[tokio::test]
async fn read_reply_token_cannot_revive_after_retire_and_same_port_rebind() {
    let proxy = proxy();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let preferred = available_port().await;
    let mut sockets = socket_set(&proxy, Some(preferred));
    let policy = policy(&proxy, Some("first"));
    let mut buffer = [0; 32];
    sockets
        .send_to_addr(b"old flow", target, 1, &policy)
        .await
        .unwrap();
    let (_, old_local) = peer.recv_from(&mut buffer).await.unwrap();
    peer.send_to(b"already read", old_local).await.unwrap();
    let (_, old_response) = sockets.recv_from_addr(&mut buffer).await.unwrap();
    assert!(old_response.is_current());
    // Further sends on the same live mapping must preserve read-token validity.
    sockets
        .send_to_addr(b"same flow", target, 1, &policy)
        .await
        .unwrap();
    peer.recv_from(&mut buffer).await.unwrap();
    assert!(old_response.is_current());
    sockets.retire_session(1);
    assert!(!old_response.is_current());
    sockets
        .send_to_addr(b"new flow", target, 2, &policy)
        .await
        .unwrap();
    let (_, new_local) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(old_local, new_local);
    assert!(!old_response.is_current());
    peer.send_to(b"new response", new_local).await.unwrap();
    let (_, response) = sockets.recv_from_addr(&mut buffer).await.unwrap();
    assert!(response.is_current());
    assert_eq!(response.session_id, Some(2));
}

#[tokio::test]
async fn read_reply_token_observes_policy_and_egress_generation_changes() {
    for change_policy in [true, false] {
        let proxy = proxy();
        let mut sockets = socket_set(&proxy, None);
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = peer.local_addr().unwrap();
        let policy = policy(&proxy, Some("first"));
        let mut buffer = [0; 32];
        sockets
            .send_to_addr(b"request", target, 1, &policy)
            .await
            .unwrap();
        let (_, local) = peer.recv_from(&mut buffer).await.unwrap();
        peer.send_to(b"read response", local).await.unwrap();
        let (_, response) = sockets.recv_from_addr(&mut buffer).await.unwrap();
        assert!(response.is_current());
        if change_policy {
            proxy
                .engine()
                .reload_runtime_config(config("only_ipv4", "auto"))
                .unwrap();
        } else {
            proxy.egress_interface.replace_for(
                false,
                Some(zero_platform_tokio::EgressInterface::new("test-egress", 1).unwrap()),
            );
        }
        assert!(!response.is_current());
    }
}
