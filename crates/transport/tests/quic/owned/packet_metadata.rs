use super::*;

#[tokio::test]
async fn packet_segment_preserves_actual_native_ecn_source_destination_gso_and_pmtu_facts() {
    use quinn::udp::{EcnCodepoint, RecvMeta, Transmit};
    use std::io::IoSliceMut;
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut peer = socket();
    let peer_wire = peer.socket();
    let peer_address = peer_wire.local_addr().unwrap();
    let mut native = native_owner();
    let mut grant = native.bind("0.0.0.0:0".parse().unwrap()).unwrap();
    let source_ip = std::net::IpAddr::from([127, 0, 0, 2]);
    let peer_ip = peer_address.ip();
    let local = std::net::SocketAddr::new(source_ip, grant.tx.local_addr().unwrap().port());
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
        source_ip: std::net::IpAddr,
    ) {
        let frame = Transmit {
            destination,
            ecn: Some(ecn),
            contents,
            segment_size: segment,
            src_ip: Some(source_ip),
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
        source_ip: std::net::IpAddr,
        destination_ip: std::net::IpAddr,
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
            assert_eq!(meta[0].addr.ip(), source_ip);
            assert_eq!(meta[0].dst_ip, Some(destination_ip));
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
            source_ip,
        )
        .await;
    } else {
        submit(
            &logical.socket,
            peer_address,
            b"aa",
            EcnCodepoint::Ect0,
            None,
            source_ip,
        )
        .await;
        submit(
            &logical.socket,
            peer_address,
            b"bb",
            EcnCodepoint::Ect0,
            None,
            source_ip,
        )
        .await;
    }
    packets(
        &peer_wire,
        &[b"aa", b"bb"],
        EcnCodepoint::Ect0,
        source_ip,
        peer_ip,
    )
    .await;
    if peer_wire.max_transmit_segments() >= 2 {
        submit(
            &peer_wire,
            local,
            b"ccdd",
            EcnCodepoint::Ect1,
            Some(2),
            peer_ip,
        )
        .await;
    } else {
        submit(&peer_wire, local, b"cc", EcnCodepoint::Ect1, None, peer_ip).await;
        submit(&peer_wire, local, b"dd", EcnCodepoint::Ect1, None, peer_ip).await;
    }
    packets(
        &logical.socket,
        &[b"cc", b"dd"],
        EcnCodepoint::Ect1,
        peer_ip,
        source_ip,
    )
    .await;
    submit(&peer_wire, local, b"", EcnCodepoint::Ect0, None, peer_ip).await;
    packets(
        &logical.socket,
        &[b""],
        EcnCodepoint::Ect0,
        peer_ip,
        source_ip,
    )
    .await;
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
    let report = native.take_report();
    assert!(report.is_complete());
    assert_eq!(report.main.confirmed_closed, 1);
    drop(peer_wire);
    let report = peer.close_io();
    assert!(report.is_complete());
    assert_eq!(report.main.confirmed_closed, 1);
    scope.begin_shutdown();
    assert!(scope.next_exit().await.is_none());
    assert!(scope
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .is_complete());
}
