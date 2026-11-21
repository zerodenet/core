use super::*;
use zero_platform_tokio::contract::datagram::{recv, send};
#[tokio::test]
async fn callback_datagram_uses_both_real_socks_originals_and_preserves_requested_association_and_empty_frames(
) {
    timeout(Duration::from_secs(20), async {
        for kind in Kind::all() {
            for business in [true, false] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let fixture = Fixture::new(listener.local_addr().unwrap(), kind, business).await;
                let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let relay = socket.local_addr().unwrap();
                let peer = tokio::spawn(async move {
                    let (tcp, _) = listener.accept().await.unwrap();
                    let mut tcp = zero_platform_tokio::TokioSocket::new(tcp);
                    assert!(matches!(
                        socks5::Socks5Inbound
                            .accept_command(&mut tcp)
                            .await
                            .unwrap(),
                        socks5::Socks5Request::UdpAssociate(_)
                    ));
                    socks5::Socks5Inbound
                        .send_success_response_with_bound(
                            &mut tcp,
                            &Address::Ipv4([127, 0, 0, 1]),
                            relay.port(),
                        )
                        .await
                        .unwrap();
                    let mut tcp = tcp.into_inner().unwrap();
                    let codec = socks5::udp::Socks5InboundUdpCodec;
                    let mut bytes = [0; 65535];
                    for payload in [b"".as_slice(), b"actual", b""] {
                        let (size, source) = socket.recv_from(&mut bytes).await.unwrap();
                        let frame = codec.decode_request(&bytes[..size]).unwrap();
                        assert_eq!(frame.payload(), payload);
                        assert_eq!(frame.target(), &Address::Ipv4([203, 0, 113, 9]));
                        assert_eq!(frame.port(), 443);
                        let reply = codec
                            .encode_response_to_client(
                                frame.target(),
                                frame.port(),
                                frame.payload(),
                            )
                            .unwrap();
                        socket.send_to(&reply, source).await.unwrap();
                    }
                    let mut rest = Vec::new();
                    tcp.read_to_end(&mut rest).await.unwrap();
                    assert!(rest.is_empty());
                    tcp.shutdown().await.unwrap();
                });
                let association = AssociationId(917);
                let target = fixture.prefix.expected_endpoint.clone();
                let mut channel = fixture
                    .prefix
                    .open_datagram(
                        &fixture.prefix.expected_binding,
                        DatagramRequest {
                            association,
                            targets: DatagramTargets::Fixed(target.clone()),
                        },
                        fixture.call(),
                    )
                    .await
                    .unwrap();
                let originals = fixture.originals();
                assert_eq!(originals.len(), 2);
                assert!(originals.iter().all(|original| original.closed().is_none()));
                for payload in [b"".as_slice(), b"actual", b""] {
                    let frame = zero_core::packet::Datagram {
                        association,
                        address: target.clone(),
                        payload: payload.to_vec().into(),
                    };
                    let receipt = send(
                        channel.tx.as_mut(),
                        channel.control.as_mut(),
                        &frame,
                        &channel.limits,
                    )
                    .await
                    .unwrap();
                    assert_eq!(receipt.accepted_bytes, payload.len());
                    let reply = recv(channel.rx.as_mut()).await.unwrap().unwrap();
                    assert_eq!(reply.association, association);
                    assert_eq!(reply.address, target);
                    assert_eq!(reply.payload.as_ref(), payload);
                }
                let zero_platform_tokio::contract::datagram::DatagramSession {
                    tx,
                    rx,
                    mut control,
                    ..
                } = channel;
                drop(tx);
                drop(rx);
                control.cancel(CloseReason::Shutdown);
                let report = control
                    .wait_closed(Instant::now() + Duration::from_secs(5))
                    .await
                    .unwrap();
                assert!(report.is_complete());
                drop(control);
                peer.await.unwrap();
                fixture.finish(originals).await;
            }
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn callback_rejects_multiple_or_changed_binding_before_original_acquisition() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fixture = Fixture::new(listener.local_addr().unwrap(), Kind::Accepted, true).await;
    let request = || DatagramRequest {
        association: AssociationId(31),
        targets: DatagramTargets::Multiple,
    };
    assert_eq!(
        fixture
            .prefix
            .open_datagram(&fixture.prefix.expected_binding, request(), fixture.call())
            .await
            .err()
            .unwrap()
            .reason,
        Reason::Unsupported
    );
    let mut stale = fixture.prefix.expected_binding.clone();
    stale.prefix.clear();
    assert_eq!(
        fixture
            .prefix
            .open_stream(
                &stale,
                fixture.prefix.expected_endpoint.clone(),
                fixture.call()
            )
            .await
            .err()
            .unwrap()
            .reason,
        Reason::StalePlan
    );
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(listener.poll_accept(&mut cx).is_pending());
    assert!(fixture.originals().is_empty());
    fixture.finish(vec![]).await;
}

#[tokio::test]
async fn callback_datagram_parent_and_additional_cancel_wake_original_pending_receive() {
    timeout(Duration::from_secs(15), async {
        for parent in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let fixture = Fixture::new(listener.local_addr().unwrap(), Kind::Accepted, true).await;
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let relay = socket.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut tcp = zero_platform_tokio::TokioSocket::new(tcp);
                assert!(matches!(
                    socks5::Socks5Inbound
                        .accept_command(&mut tcp)
                        .await
                        .unwrap(),
                    socks5::Socks5Request::UdpAssociate(_)
                ));
                socks5::Socks5Inbound
                    .send_success_response_with_bound(
                        &mut tcp,
                        &Address::Ipv4([127, 0, 0, 1]),
                        relay.port(),
                    )
                    .await
                    .unwrap();
                let mut tcp = tcp.into_inner().unwrap();
                let mut rest = Vec::new();
                tcp.read_to_end(&mut rest).await.unwrap();
                assert!(rest.is_empty());
                drop(socket);
            });
            let call = fixture.call();
            let extra = call.cancel.clone();
            let mut channel = fixture
                .prefix
                .open_datagram(
                    &fixture.prefix.expected_binding,
                    DatagramRequest {
                        association: AssociationId(918),
                        targets: DatagramTargets::Fixed(fixture.prefix.expected_endpoint.clone()),
                    },
                    call,
                )
                .await
                .unwrap();
            let originals = fixture.originals();
            assert_eq!(originals.len(), 2);
            {
                let receiving = recv(channel.rx.as_mut());
                tokio::pin!(receiving);
                assert!(
                    futures_util::poll!(receiving.as_mut()).is_pending(),
                    "actual granted UDP receive has no incoming datagram"
                );
                if parent {
                    fixture.prefix.context.cancel.cancel();
                } else {
                    extra.cancel();
                }
                assert!(receiving.await.unwrap().is_none());
            }
            assert!(
                originals.iter().all(|original| original.closed().is_none()),
                "cancellation alone cannot close retained unique views"
            );
            let zero_platform_tokio::contract::datagram::DatagramSession {
                tx,
                rx,
                mut control,
                ..
            } = channel;
            drop(tx);
            drop(rx);
            let report = control
                .wait_closed(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
            assert!(report.is_complete());
            drop(control);
            peer.await.unwrap();
            fixture.finish(originals).await;
        }
    })
    .await
    .unwrap();
}
