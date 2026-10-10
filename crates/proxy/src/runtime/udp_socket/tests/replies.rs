use super::*;

#[tokio::test]
async fn direct_udp_tags_isolate_sockets_and_replies_to_the_same_remote() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let mut buffer = [0; 32];
    let mut locals = Vec::new();
    for (tag, session) in [("first", 41), ("second", 42)] {
        sockets
            .send_to_addr(b"request", target, session, &policy(&proxy, Some(tag)))
            .await
            .unwrap();
        let (_, local) = peer.recv_from(&mut buffer).await.unwrap();
        locals.push(local);
    }
    assert_ne!(locals[0], locals[1]);
    assert_eq!(sockets.sockets.len(), 2);
    let stranger = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    stranger.send_to(b"unknown", locals[0]).await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(30),
            sockets.recv_from_addr(&mut buffer),
        )
        .await
        .is_err(),
        "unscoped sockets reject unregistered peers too"
    );
    for (index, session) in [(1, 42), (0, 41)] {
        peer.send_to(&[session as u8], locals[index]).await.unwrap();
        let (len, source) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sockets.recv_from_addr(&mut buffer),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(source.session_id, Some(session));
        assert_eq!(source.sender, target);
        assert_eq!(&buffer[..len], &[session as u8]);
    }
    sockets.retire_session(41);
    peer.send_to(b"retired", locals[0]).await.unwrap();
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(30),
        sockets.recv_from_addr(&mut buffer),
    )
    .await
    .is_err());
}

#[tokio::test]
async fn same_policy_reuses_endpoint_independent_mapping_and_scopes_associations() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let policy = policy(&proxy, Some("first"));
    let first = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut buffer = [0; 32];
    sockets
        .send_to_addr(b"one", first.local_addr().unwrap(), 1, &policy)
        .await
        .unwrap();
    let (_, first_local) = first.recv_from(&mut buffer).await.unwrap();
    sockets
        .send_to_addr(b"two", second.local_addr().unwrap(), 2, &policy)
        .await
        .unwrap();
    let (_, second_local) = second.recv_from(&mut buffer).await.unwrap();
    assert_eq!(first_local, second_local);
    assert_eq!(sockets.sockets.len(), 1);
    for (session, association) in [(3, 70), (4, 80)] {
        sockets.isolate_association(session, association);
        sockets
            .send_to_addr(b"isolated", first.local_addr().unwrap(), session, &policy)
            .await
            .unwrap();
        let (_, local) = first.recv_from(&mut buffer).await.unwrap();
        assert_ne!(local, first_local);
        first.send_to(b"reply", local).await.unwrap();
        let (_, source) = sockets.recv_from_addr(&mut buffer).await.unwrap();
        assert_eq!(source.session_id, Some(session));
    }
    assert_eq!(sockets.sockets.len(), 3);
    sockets.retire_session(3);
    assert_eq!(sockets.sockets.len(), 2);
    sockets.retire_session(4);
    assert_eq!(sockets.sockets.len(), 1);
}

#[tokio::test]
async fn same_policy_remote_collision_keeps_each_logical_flow_on_its_own_socket() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let policy = policy(&proxy, Some("first"));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let other_peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let mut buffer = [0; 32];
    let mut locals = Vec::new();
    for session in [1, 2] {
        sockets
            .send_to_addr(b"same remote", target, session, &policy)
            .await
            .unwrap();
        let (_, local) = peer.recv_from(&mut buffer).await.unwrap();
        locals.push(local);
    }
    assert_ne!(locals[0], locals[1]);
    // A different endpoint still reuses the normal mapping.
    sockets
        .send_to_addr(
            b"different remote",
            other_peer.local_addr().unwrap(),
            3,
            &policy,
        )
        .await
        .unwrap();
    let (_, other_local) = other_peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(other_local, locals[0]);
    for (index, session) in [(1, 2), (0, 1)] {
        peer.send_to(b"response", locals[index]).await.unwrap();
        let (_, response) = sockets.recv_from_addr(&mut buffer).await.unwrap();
        assert_eq!(response.session_id, Some(session));
    }
    // A freed slot in the first mapping cannot steal the surviving flow's
    // established mapping on its next upload.
    sockets.retire_session(1);
    sockets
        .send_to_addr(b"same flow", target, 2, &policy)
        .await
        .unwrap();
    let (_, local) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(local, locals[1]);
}

#[tokio::test]
async fn retired_peer_delayed_reply_cannot_be_attributed_to_its_replacement_flow() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let policy = policy(&proxy, Some("first"));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let anchor = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let mut buffer = [0; 32];
    sockets
        .send_to_addr(b"old flow", target, 1, &policy)
        .await
        .unwrap();
    let (_, old_local) = peer.recv_from(&mut buffer).await.unwrap();
    sockets
        .send_to_addr(b"keep alive", anchor.local_addr().unwrap(), 2, &policy)
        .await
        .unwrap();
    let (_, anchor_local) = anchor.recv_from(&mut buffer).await.unwrap();
    assert_eq!(old_local, anchor_local);
    sockets.retire_session(1);
    assert_eq!(
        sockets.sockets.len(),
        1,
        "unaffected peer keeps its mapping"
    );
    sockets
        .send_to_addr(b"replacement", target, 3, &policy)
        .await
        .unwrap();
    let (_, new_local) = peer.recv_from(&mut buffer).await.unwrap();
    assert_ne!(
        old_local, new_local,
        "a retired remote cannot acquire a new owner on the live socket"
    );
    peer.send_to(b"delayed old reply", old_local).await.unwrap();
    peer.send_to(b"new reply", new_local).await.unwrap();
    let (len, response) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sockets.recv_from_addr(&mut buffer),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.session_id, Some(3));
    assert_eq!(&buffer[..len], b"new reply");
    anchor.send_to(b"anchor reply", anchor_local).await.unwrap();
    let (_, response) = sockets.recv_from_addr(&mut buffer).await.unwrap();
    assert_eq!(response.session_id, Some(2));
    sockets.retire_session(3);
    assert_eq!(sockets.sockets.len(), 1);
    sockets.retire_session(2);
    assert!(
        sockets.sockets.is_empty(),
        "last owner retirement releases all tombstones"
    );
}

#[tokio::test]
async fn retired_peer_tombstones_have_bounded_admission_without_expiring_live_owners() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let policy = policy(&proxy, Some("first"));
    let anchor = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let newcomer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut buffer = [0; 32];
    sockets
        .send_to_addr(b"anchor", anchor.local_addr().unwrap(), 1, &policy)
        .await
        .unwrap();
    let (_, anchor_local) = anchor.recv_from(&mut buffer).await.unwrap();
    for port in 1..super::super::MAX_DIRECT_UDP_PEERS_PER_SOCKET {
        sockets.sockets[0]
            .retired_peers
            .insert(SocketAddr::from(([192, 0, 2, 1], port as u16)));
    }
    let original_id = sockets.sockets[0].id;
    sockets
        .send_to_addr(b"new peer", newcomer.local_addr().unwrap(), 2, &policy)
        .await
        .unwrap();
    let (_, new_local) = newcomer.recv_from(&mut buffer).await.unwrap();
    assert_ne!(anchor_local, new_local);
    assert_eq!(sockets.sockets[0].id, original_id);
    assert_eq!(sockets.sockets.len(), 2);
    sockets
        .send_to_addr(b"same anchor", anchor.local_addr().unwrap(), 1, &policy)
        .await
        .unwrap();
    let (_, same_anchor) = anchor.recv_from(&mut buffer).await.unwrap();
    assert_eq!(
        same_anchor, anchor_local,
        "saturated sockets preserve existing mappings"
    );
}
