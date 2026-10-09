use super::*;

async fn available_port() -> u16 {
    let reservation = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    reservation.local_addr().unwrap().port()
}

#[tokio::test]
async fn changed_policy_never_reuses_closed_preferred_port_for_the_same_remote() {
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
    assert_ne!(old_local.port(), new_local.port());
    peer.send_to(b"delayed old", old_local).await.unwrap();
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
async fn final_owner_retirement_does_not_recycle_the_closed_preferred_port() {
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
    assert_eq!(old_local.port(), preferred);
    sockets.retire_session(1);
    assert!(sockets.sockets.is_empty());
    sockets
        .send_to_addr(b"new flow", target, 2, &policy)
        .await
        .unwrap();
    let (_, new_local) = peer.recv_from(&mut buffer).await.unwrap();
    assert_ne!(old_local.port(), new_local.port());
    peer.send_to(b"delayed old", old_local).await.unwrap();
    peer.send_to(b"new response", new_local).await.unwrap();
    let (len, response) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sockets.recv_from_addr(&mut buffer),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.session_id, Some(2));
    assert_eq!(&buffer[..len], b"new response");
}

#[test]
fn source_port_history_is_bounded_per_family_and_survives_socket_refresh() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let v4 = "127.0.0.1:12345".parse().unwrap();
    let v6 = "[::1]:12345".parse().unwrap();
    assert!(sockets.reserve_local_port(v4));
    assert!(sockets.reserve_local_port(v6));
    assert!(!sockets.reserve_local_port(v4));
    assert!(!sockets.reserve_local_port(v6));
    sockets.refresh_if_stale();
    assert!(sockets.port_was_used(false, 12345));
    assert!(sockets.port_was_used(true, 12345));
    assert_eq!(std::mem::size_of_val(&*sockets.used_ports), 16 * 1024);
}
