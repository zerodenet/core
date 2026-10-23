use super::super::{ownership::DirectUdpReplies, DirectUdpResponseFlow};
use super::*;
use std::time::Duration;

mod benchmark;

#[test]
fn repeated_registration_preserves_owner_and_retirement_revokes_only_its_token() {
    let proxy = proxy();
    let services = socket_set(&proxy, None).services.unwrap();
    let policy = policy(&proxy, Some("first"));
    let generation = services.egress_generation();
    let mut replies = DirectUdpReplies::default();
    let first = "192.0.2.1:53".parse().unwrap();
    let other = "192.0.2.2:53".parse().unwrap();
    for (peer, session) in [(first, 1), (other, 2)] {
        replies.register(peer, || {
            DirectUdpResponseFlow::new(session, services.clone(), policy.clone(), generation)
        });
    }
    let first_guard = replies.flow(&first).unwrap().guard.clone();
    let other_guard = replies.flow(&other).unwrap().guard.clone();
    replies.register(first, || panic!("repeated send must not replace an owner"));
    assert_eq!(replies.flow(&first).unwrap().session_id, 1);
    assert!(first_guard.is_current());
    replies.retire_session(1);
    assert!(!first_guard.is_current());
    assert!(other_guard.is_current());
    assert!(!replies.can_admit(&first), "retired tuple remains reserved");
    assert!(replies.can_admit(&"192.0.2.3:53".parse().unwrap()));
    replies.retire_session(1);
    assert!(other_guard.is_current());
    drop(replies);
    assert!(
        !other_guard.is_current(),
        "socket-table destruction revokes survivors"
    );
}

#[test]
fn live_and_retired_peers_share_the_same_bounded_admission_budget() {
    let proxy = proxy();
    let services = socket_set(&proxy, None).services.unwrap();
    let policy = policy(&proxy, Some("first"));
    let generation = services.egress_generation();
    let mut replies = DirectUdpReplies::default();
    for port in 1..=super::super::MAX_DIRECT_UDP_PEERS_PER_SOCKET {
        replies.register(SocketAddr::from(([192, 0, 2, 1], port as u16)), || {
            DirectUdpResponseFlow::new(
                (port % 2) as u64,
                services.clone(),
                policy.clone(),
                generation,
            )
        });
    }
    let survivor = replies
        .flow(&SocketAddr::from(([192, 0, 2, 1], 2)))
        .unwrap()
        .guard
        .clone();
    replies.retire_session(1);
    assert!(survivor.is_current());
    assert!(!replies.can_admit(&"192.0.2.2:53".parse().unwrap()));
    assert!(!replies.can_admit(&SocketAddr::from(([192, 0, 2, 1], 1))));
    replies.retire_session(0);
    assert!(replies.is_empty());
    assert!(!survivor.is_current());
}

#[tokio::test]
async fn retiring_one_session_across_sockets_preserves_an_unrelated_shared_owner() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let keeper = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let mut output = [0; 32];
    let mut retired_guards = Vec::new();
    let mut locals = Vec::new();
    for tag in ["first", "second"] {
        sockets
            .send_to_addr(b"old", target, 9, &policy(&proxy, Some(tag)))
            .await
            .unwrap();
        let (_, local) = peer.recv_from(&mut output).await.unwrap();
        locals.push(local);
        peer.send_to(b"read", local).await.unwrap();
        let (_, source) = sockets.recv_from_addr(&mut output).await.unwrap();
        assert_eq!(source.session_id, Some(9));
        retired_guards.push(source);
    }
    sockets
        .send_to_addr(
            b"keep",
            keeper.local_addr().unwrap(),
            42,
            &policy(&proxy, Some("first")),
        )
        .await
        .unwrap();
    let (_, local) = keeper.recv_from(&mut output).await.unwrap();
    assert_eq!(local, locals[0]);
    keeper.send_to(b"read", local).await.unwrap();
    let (_, surviving_guard) = sockets.recv_from_addr(&mut output).await.unwrap();
    let surviving_id = sockets.sockets[0].id;
    sockets.retire_session(9);
    sockets.retire_session(9);
    assert_eq!(sockets.sockets.len(), 1);
    assert_eq!(sockets.sockets[0].id, surviving_id);
    assert!(retired_guards.iter().all(|source| !source.is_current()));
    assert!(surviving_guard.is_current());
    peer.send_to(b"delayed", local).await.unwrap();
    keeper.send_to(b"valid", local).await.unwrap();
    let (size, source) =
        tokio::time::timeout(Duration::from_secs(2), sockets.recv_from_addr(&mut output))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(&output[..size], b"valid");
    assert_eq!(source.session_id, Some(42));
    sockets.retire_session(42);
    assert!(sockets.sockets.is_empty());
    assert!(!surviving_guard.is_current());
}

#[tokio::test]
async fn bulk_policy_retirement_keeps_survivor_tables_and_topology_clear_revokes_all() {
    let proxy = proxy();
    let mut sockets = socket_set(&proxy, None);
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut output = [0; 16];
    let mut sources = Vec::new();
    let mut surviving_ids = Vec::new();
    for session in 0..16 {
        let tag = if session % 2 == 0 { "first" } else { "second" };
        sockets
            .send_to_addr(
                b"request",
                peer.local_addr().unwrap(),
                session,
                &policy(&proxy, Some(tag)),
            )
            .await
            .unwrap();
        let (_, local) = peer.recv_from(&mut output).await.unwrap();
        if tag == "second" {
            surviving_ids.push(sockets.sockets.last().unwrap().id);
        }
        peer.send_to(b"read", local).await.unwrap();
        let (_, source) = sockets.recv_from_addr(&mut output).await.unwrap();
        assert_eq!(source.session_id, Some(session));
        sources.push(source);
    }
    proxy
        .engine()
        .reload_runtime_config(config("only_ipv4", "auto"))
        .unwrap();
    sockets.refresh_if_stale();
    assert_eq!(
        sockets
            .sockets
            .iter()
            .map(|socket| socket.id)
            .collect::<Vec<_>>(),
        surviving_ids
    );
    for (session, source) in sources.iter().enumerate() {
        assert_eq!(source.is_current(), session % 2 != 0);
    }
    proxy.egress_interface.replace_for(
        false,
        Some(zero_platform_tokio::EgressInterface::new("test-egress", 1).unwrap()),
    );
    assert!(sources.iter().all(|source| !source.is_current()));
    sockets.refresh_if_stale();
    assert!(sockets.sockets.is_empty());
    assert_eq!(sockets.receive.lock().await.scratch.capacity(), 0);
}
