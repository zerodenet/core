use super::*;
use crate::quic::owned::packet::{PacketSegment, PacketSocket};
use std::time::Duration;
use tokio::time::Instant;
use zero_platform_tokio::contract::{component::GrantedMetadataListener, scope::TaskScope};

#[tokio::test]
async fn packet_wire_view_preserves_finite_queue_backpressure_without_a_native_worker() {
    let peer = "127.0.0.1:443".parse().unwrap();
    let (socket, _queues) = PacketSocket::prepare(peer).unwrap();
    let frame = quinn::udp::Transmit {
        destination: peer,
        ecn: None,
        contents: b"one queued datagram",
        segment_size: None,
        src_ip: None,
    };
    for _ in 0..256 {
        socket.socket.try_send(&frame).unwrap();
    }
    assert_eq!(
        socket.socket.try_send(&frame).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock,
        "the next actual queue submission must wait; it is not reported as delivered"
    );
}

#[tokio::test]
async fn packet_segment_returns_real_views_and_join_before_original_native_close() {
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = peer.local_addr().unwrap();
    let mut native = native_owner();
    let mut grant = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let retained = grant.tx.clone();
    let local = grant.tx.local_addr().unwrap();
    let (issuer, claim) = grant
        .control
        .claim_views(scope.registrar().scope_identity())
        .unwrap();
    let observation = grant.control.take_observation().unwrap();
    let (socket, queues) = PacketSocket::prepare(peer_addr).unwrap();
    let mut segment = match PacketSegment::prepare(
        GrantedMetadataListener {
            scope: scope.registrar().scope_identity(),
            rx: grant.rx,
            tx: grant.tx,
            observation,
            users_claim: Some(claim),
        },
        queues,
        scope.registrar(),
        peer_addr,
        peer_addr,
    ) {
        Ok(value) => value,
        Err(_) => panic!("same actual native grant and scope"),
    };
    segment.start().unwrap();
    socket
        .socket
        .try_send(&quinn::udp::Transmit {
            destination: peer_addr,
            ecn: None,
            contents: b"same original",
            segment_size: None,
            src_ip: None,
        })
        .unwrap();
    let mut wire = [0; 64];
    let (count, source) = tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut wire))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&wire[..count], b"same original");
    assert_eq!(source, local);
    segment.stop();
    let exit = scope.next_exit().await.unwrap();
    exit.result.unwrap();
    let error = match segment
        .retire(Instant::now() + Duration::from_secs(1))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("actual retained TX must block the original view receipt"),
    };
    assert_eq!(error.reason, zero_platform_tokio::contract::Reason::Timeout);
    assert!(retained.local_addr().is_ok());
    drop(retained);
    let (_queues, receipt) = segment
        .retire(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(issuer.validates_users(&receipt, &scope.registrar()));
    assert_eq!(
        native.occupied_main(),
        1,
        "TaskScope join and returned views are not native close"
    );
    let original = grant.control.close().unwrap();
    assert_eq!(original.reservation(), grant.control.reservation());
    assert_eq!(native.take_report().main.confirmed_closed, 1);
    scope.begin_shutdown();
    assert!(scope.next_exit().await.is_none());
    assert!(scope
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .is_complete());
}

#[cfg(target_os = "linux")]
#[path = "packet_metadata.rs"]
mod metadata;
