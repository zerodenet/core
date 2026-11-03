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
#[tokio::test]
async fn packet_segment_preserves_actual_native_ecn_source_destination_gso_and_pmtu_facts() {
    use quinn::udp::{EcnCodepoint, RecvMeta, Transmit};
    use std::io::IoSliceMut;
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut peer = socket();
    let peer_wire = peer.socket();
    let peer_address = peer_wire.local_addr().unwrap();
    let mut native = native_owner();
    let mut grant = native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let local = grant.tx.local_addr().unwrap();
    let capabilities = grant.tx.capabilities();
    let (issuer, claim) = grant
        .control
        .claim_views(scope.registrar().scope_identity())
        .unwrap();
    let observation = grant.control.take_observation().unwrap();
    let (logical, queues) = PacketSocket::prepare(peer_address).unwrap();
    let mut segment = PacketSegment::prepare(
        GrantedMetadataListener {
            scope: scope.registrar().scope_identity(),
            rx: grant.rx,
            tx: grant.tx,
            observation,
            users_claim: Some(claim),
        },
        queues,
        scope.registrar(),
        peer_address,
        peer_address,
    )
    .unwrap_or_else(|_| panic!("same native owner"));
    assert_eq!(
        logical.socket.max_transmit_segments(),
        capabilities.transmit_segments.max(1)
    );
    assert_eq!(logical.socket.may_fragment(), capabilities.may_fragment);
    assert_eq!(
        logical.socket.max_receive_segments(),
        1,
        "the native worker splits actual GRO into complete metadata-bearing queue packets"
    );
    eprintln!("actual packet native capabilities: {capabilities:?}");
    segment.start().unwrap();
    async fn submit(
        socket: &Arc<dyn quinn::AsyncUdpSocket>,
        destination: std::net::SocketAddr,
        contents: &[u8],
        ecn: EcnCodepoint,
        segment: Option<usize>,
    ) {
        let frame = Transmit {
            destination,
            ecn: Some(ecn),
            contents,
            segment_size: segment,
            src_ip: Some(std::net::Ipv4Addr::LOCALHOST.into()),
        };
        let mut poller = socket.clone().create_io_poller();
        loop {
            match socket.try_send(&frame) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    poll_fn(|cx| poller.as_mut().poll_writable(cx))
                        .await
                        .unwrap();
                }
                result => {
                    result.unwrap();
                    break;
                }
            }
        }
    }
    async fn packets(
        socket: &Arc<dyn quinn::AsyncUdpSocket>,
        expected: &[&[u8]],
        ecn: EcnCodepoint,
    ) {
        let mut received = Vec::new();
        while received.len() < expected.len() {
            let mut bytes = [0; 64];
            let mut meta = [RecvMeta::default()];
            let count = tokio::time::timeout(
                Duration::from_secs(1),
                poll_fn(|cx| socket.poll_recv(cx, &mut [IoSliceMut::new(&mut bytes)], &mut meta)),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(count, 1);
            assert_eq!(meta[0].ecn, Some(ecn));
            assert_eq!(meta[0].dst_ip, Some(std::net::Ipv4Addr::LOCALHOST.into()));
            if meta[0].len == 0 {
                received.push(Vec::new());
            } else {
                for packet in bytes[..meta[0].len].chunks(meta[0].stride) {
                    received.push(packet.to_vec());
                }
            }
        }
        assert_eq!(
            received,
            expected
                .iter()
                .map(|bytes| bytes.to_vec())
                .collect::<Vec<_>>()
        );
    }
    if capabilities.transmit_segments >= 2 {
        submit(
            &logical.socket,
            peer_address,
            b"aabb",
            EcnCodepoint::Ect0,
            Some(2),
        )
        .await;
    } else {
        submit(
            &logical.socket,
            peer_address,
            b"aa",
            EcnCodepoint::Ect0,
            None,
        )
        .await;
        submit(
            &logical.socket,
            peer_address,
            b"bb",
            EcnCodepoint::Ect0,
            None,
        )
        .await;
    }
    packets(&peer_wire, &[b"aa", b"bb"], EcnCodepoint::Ect0).await;
    if peer_wire.max_transmit_segments() >= 2 {
        submit(&peer_wire, local, b"ccdd", EcnCodepoint::Ect1, Some(2)).await;
    } else {
        submit(&peer_wire, local, b"cc", EcnCodepoint::Ect1, None).await;
        submit(&peer_wire, local, b"dd", EcnCodepoint::Ect1, None).await;
    }
    packets(&logical.socket, &[b"cc", b"dd"], EcnCodepoint::Ect1).await;
    submit(&peer_wire, local, b"", EcnCodepoint::Ect0, None).await;
    packets(&logical.socket, &[b""], EcnCodepoint::Ect0).await;
    segment.stop();
    let exit = scope.next_exit().await.unwrap();
    exit.result.unwrap();
    let (_, receipt) = segment
        .retire(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(issuer.validates_users(&receipt, &scope.registrar()));
    assert_eq!(native.occupied_main(), 1);
    let closed = grant.control.close().unwrap();
    assert_eq!(closed.reservation(), grant.control.reservation());
    assert_eq!(native.take_report().main.confirmed_closed, 1);
    drop(peer_wire);
    assert_eq!(peer.close_io().main.confirmed_closed, 1);
    scope.begin_shutdown();
    assert!(scope.next_exit().await.is_none());
    assert!(scope
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .is_complete());
}
