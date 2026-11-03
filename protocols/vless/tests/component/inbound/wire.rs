use super::*;
#[tokio::test]
async fn standard_tcp_and_udp_keep_wire_facts_and_original_native_close() {
    timeout(Duration::from_secs(10), async {
        for udp in [false, true] {
            let mut host = fixture::Host::new(false).await;
            let (mut client, accepted) = host.accept().await;
            client
                .write_all(&header(if udp { 2 } else { 1 }))
                .await
                .unwrap();
            let (request, _) = accepted.await.unwrap();
            if udp {
                let AcceptedRequest::Datagram(request) = request else {
                    panic!("Datagram")
                };
                assert_eq!(request.facts().session, SessionId(701));
                let mut session = request
                    .respond(ReplyOutcome::LocalChannelReady)
                    .await
                    .unwrap()
                    .unwrap();
                let mut header = [0; 2];
                client.read_exact(&mut header).await.unwrap();
                assert_eq!(header, [0, 0]);
                client.write_all(b"\0").await.unwrap();
                {
                    let pending =
                        zero_platform_tokio::contract::datagram::recv(session.io.rx.as_mut());
                    tokio::pin!(pending);
                    std::future::poll_fn(|cx| {
                        assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
                        std::task::Poll::Ready(())
                    })
                    .await;
                }
                client.write_all(b"\x03udp").await.unwrap();
                let packet = zero_platform_tokio::contract::datagram::recv(session.io.rx.as_mut())
                    .await
                    .unwrap()
                    .unwrap();
                let InboundDatagramEvent::Packet { dispatch, .. } = packet else {
                    panic!("packet")
                };
                assert_eq!(dispatch.payload(), b"udp");
                assert_eq!(
                    session.negotiation.read_bytes,
                    super::header(2).len() as u64
                );
                assert_eq!(session.negotiation.written_bytes, 2);
                let response = InboundDatagramResponse {
                    source: Destination::new(Address::Ipv4([127, 0, 0, 1]), 53),
                    client_session_id: None,
                    payload: b"reply",
                };
                let receipt =
                    std::future::poll_fn(|cx| session.io.tx.as_mut().poll_send(cx, &response))
                        .await
                        .unwrap();
                assert_eq!(receipt.protocol_bytes, 7);
                let mut reply = [0; 7];
                client.read_exact(&mut reply).await.unwrap();
                assert_eq!(&reply, b"\0\x05reply");
                drop(session.io.tx);
                drop(session.io.rx);
                assert!(session
                    .io
                    .control
                    .wait_closed(Instant::now() + Duration::from_secs(1))
                    .await
                    .unwrap()
                    .is_complete());
            } else {
                let AcceptedRequest::Stream(request) = request else {
                    panic!("Stream")
                };
                assert_eq!(request.facts().session, SessionId(701));
                let mut stream = request
                    .respond(ReplyOutcome::LocalChannelReady)
                    .await
                    .unwrap()
                    .unwrap();
                let mut header = [0; 2];
                client.read_exact(&mut header).await.unwrap();
                assert_eq!(header, [0, 0]);
                client.write_all(b"tcp").await.unwrap();
                let mut bytes = [0; 3];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"tcp");
                stream.write_all(&bytes).await.unwrap();
                client.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"tcp");
                drop(stream);
            }
            host.close(1).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn standard_mux_has_one_original_driver_and_never_repeats_vless_response_on_logical_tcp() {
    timeout(Duration::from_secs(10), async {
        let mut host = fixture::Host::new(false).await;
        let (mut client, accepted) = host.accept().await;
        client.write_all(&header(3)).await.unwrap();
        let (request, context) = accepted.await.unwrap();
        let AcceptedRequest::Multiplex(mut source) = request else {
            panic!("MUX")
        };
        let mut response = [0; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, [0, 0]);
        client.write_all(&frame(1, 1, b"tcp")).await.unwrap();
        let pending = source
            .next(AcceptContext {
                deadline: None,
                cancel: host.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let AcceptedRequest::Stream(request) = pending.complete(context).await.unwrap() else {
            panic!("Stream")
        };
        let mut stream = request
            .respond(ReplyOutcome::LocalChannelReady)
            .await
            .unwrap()
            .unwrap();
        let mut bytes = [0; 3];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"tcp");
        stream.write_all(b"reply").await.unwrap();
        stream.flush().await.unwrap();
        let length = client.read_u16().await.unwrap();
        let mut meta = vec![0; length as usize];
        client.read_exact(&mut meta).await.unwrap();
        assert_eq!(&meta[..4], &[0, 1, 2, 1]);
        let size = client.read_u16().await.unwrap();
        let mut payload = vec![0; size as usize];
        client.read_exact(&mut payload).await.unwrap();
        assert_eq!(payload, b"reply");
        drop(stream);
        drop(source);
        host.close(2).await;
    })
    .await
    .unwrap();
}
