use super::*;
use zero_transport::{finalmask, mkcp};
#[tokio::test]
async fn standard_mkcp_mux_has_only_the_real_peer_and_mux_drivers() {
    timeout(Duration::from_secs(10), async {
        let request = request(USER, false)
            .with_mkcp(Some(mkcp::Settings::default()))
            .unwrap();
        let mut host = packet_fixture::Host::new(request).await;
        let sockets = zero_transport::OutboundDatagramSocketFactory::new(Default::default());
        let mut client = mkcp::connect(
            "127.0.0.1",
            host.endpoint.port(),
            mkcp::Settings::default(),
            &sockets,
        )
        .await
        .unwrap();
        client.write_all(&header(3)).await.unwrap();
        let (request, context, peer) = host.accept().await;
        let AcceptedRequest::Multiplex(mut conversations) = request else {
            panic!("mKCP conversations")
        };
        let pending = conversations
            .next(AcceptContext {
                deadline: None,
                cancel: host.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let AcceptedRequest::Multiplex(mut mux) = pending.complete(context.clone()).await.unwrap()
        else {
            panic!("VLESS MUX")
        };
        let mut response = [0; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, [0, 0]);
        client.write_all(&frame(5, 1, b"tcp")).await.unwrap();
        let pending = mux
            .next(AcceptContext {
                deadline: None,
                cancel: host.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let AcceptedRequest::Stream(request) = pending.complete(context).await.unwrap() else {
            panic!("logical TCP")
        };
        let mut stream = request
            .respond(ReplyOutcome::LocalChannelReady)
            .await
            .unwrap()
            .unwrap();
        let mut payload = [0; 3];
        stream.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"tcp");
        stream.write_all(b"reply").await.unwrap();
        stream.flush().await.unwrap();
        let n = client.read_u16().await.unwrap();
        let mut meta = vec![0; n as usize];
        client.read_exact(&mut meta).await.unwrap();
        assert_eq!(&meta[..4], &[0, 5, 2, 1]);
        assert_eq!(client.read_u16().await.unwrap(), 5);
        let mut bytes = [0; 5];
        client.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"reply");
        drop(stream);
        drop(mux);
        drop(conversations);
        drop(client);
        host.close(peer, 3).await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn standard_mkcp_tcp_udp_keep_actual_peer_scope_framing_masks_and_original_close() {
    timeout(Duration::from_secs(15), async {
        for udp in [false, true] {
            let masks = vec![
                finalmask::udp::Mask::Wireguard,
                finalmask::udp::Mask::MkcpOriginal,
            ];
            let profile = finalmask::Profile::from_udp(masks.clone()).unwrap();
            let request = request(USER, false)
                .with_final_mask(finalmask::Settings {
                    udp: masks,
                    tcp: vec![],
                })
                .unwrap()
                .with_mkcp(Some(mkcp::Settings::default()))
                .unwrap();
            let mut host = packet_fixture::Host::new(request).await;
            let sockets = zero_transport::OutboundDatagramSocketFactory::new(Default::default())
                .with_final_mask(profile);
            let mut client = mkcp::connect(
                "127.0.0.1",
                host.endpoint.port(),
                mkcp::Settings::default(),
                &sockets,
            )
            .await
            .unwrap();
            client
                .write_all(&header(if udp { 2 } else { 1 }))
                .await
                .unwrap();
            let (request, context, peer) = host.accept().await;
            assert_eq!(
                context.accepted.as_ref().unwrap().origin().unwrap(),
                zero_platform_tokio::AcceptedOrigin::Datagram
            );
            let AcceptedRequest::Multiplex(mut source) = request else {
                panic!("native mKCP conversations")
            };
            let pending = source
                .next(AcceptContext {
                    deadline: None,
                    cancel: host.stop.clone(),
                })
                .await
                .unwrap()
                .unwrap();
            let request = pending.complete(context).await.unwrap();
            if udp {
                let AcceptedRequest::Datagram(request) = request else {
                    panic!("standard UDP")
                };
                let mut session = request
                    .respond(ReplyOutcome::LocalChannelReady)
                    .await
                    .unwrap()
                    .unwrap();
                let mut response = [0; 2];
                client.read_exact(&mut response).await.unwrap();
                assert_eq!(response, [0, 0]);
                client.write_all(b"\0\x03udp").await.unwrap();
                let InboundDatagramEvent::Packet {
                    dispatch, sender, ..
                } = zero_platform_tokio::contract::datagram::recv(session.io.rx.as_mut())
                    .await
                    .unwrap()
                    .unwrap()
                else {
                    panic!("UDP facts")
                };
                assert_eq!(
                    sender,
                    SocketAddr::from((
                        [127, 0, 0, 1],
                        zero_platform_tokio::ClientStream::local_addr(&client)
                            .unwrap()
                            .port()
                    ))
                );
                assert_eq!(dispatch.payload(), b"udp");
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
                    panic!("standard TCP")
                };
                assert_eq!(request.facts().session, SessionId(801));
                let mut stream = request
                    .respond(ReplyOutcome::LocalChannelReady)
                    .await
                    .unwrap()
                    .unwrap();
                let mut response = [0; 2];
                client.read_exact(&mut response).await.unwrap();
                assert_eq!(response, [0, 0]);
                client.write_all(b"tcp").await.unwrap();
                let mut bytes = [0; 3];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"tcp");
                stream.write_all(b"tcp").await.unwrap();
                client.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"tcp");
                drop(stream);
            }
            drop(source);
            drop(client);
            host.close(peer, 2).await;
        }
    })
    .await
    .unwrap();
}
