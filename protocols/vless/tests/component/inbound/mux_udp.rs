use super::*;
#[tokio::test]
async fn standard_mux_udp_uses_actual_target_framing_and_original_control_return() {
    timeout(Duration::from_secs(10), async {
        let mut host = fixture::Host::new(false).await;
        let (mut client, accepted) = host.accept().await;
        client.write_all(&header(3)).await.unwrap();
        let (request, context) = accepted.await.unwrap();
        let AcceptedRequest::Multiplex(mut source) = request else {
            panic!("MUX")
        };
        let mut header = [0; 2];
        client.read_exact(&mut header).await.unwrap();
        assert_eq!(header, [0, 0]);
        client.write_all(&frame(7, 2, b"udp")).await.unwrap();
        let pending = source
            .next(AcceptContext {
                deadline: None,
                cancel: host.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let AcceptedRequest::Datagram(request) = pending.complete(context).await.unwrap() else {
            panic!("Datagram")
        };
        let mut session = request
            .respond(ReplyOutcome::LocalChannelReady)
            .await
            .unwrap()
            .unwrap();
        let InboundDatagramEvent::Packet { dispatch, .. } =
            zero_platform_tokio::contract::datagram::recv(session.io.rx.as_mut())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("packet")
        };
        assert_eq!(dispatch.payload(), b"udp");
        let response = InboundDatagramResponse {
            source: Destination::new(Address::Ipv4([127, 0, 0, 1]), 53),
            client_session_id: None,
            payload: b"reply",
        };
        let receipt = std::future::poll_fn(|cx| session.io.tx.as_mut().poll_send(cx, &response))
            .await
            .unwrap();
        assert_eq!(
            receipt.protocol_bytes, 5,
            "shared MUX framing is accounted by its physical driver"
        );
        let length = client.read_u16().await.unwrap();
        let mut meta = vec![0; length as usize];
        client.read_exact(&mut meta).await.unwrap();
        assert_eq!(&meta[..5], &[0, 7, 2, 1, 2]);
        let len = client.read_u16().await.unwrap();
        let mut bytes = vec![0; len as usize];
        client.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"reply");
        drop(session.io.tx);
        drop(session.io.rx);
        assert!(session
            .io
            .control
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        drop(source);
        host.close(2).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn standard_xudp_retains_authenticated_continuity_and_original_termination() {
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
        let mut packet = frame(7, 2, b"udp");
        let len = u16::from_be_bytes([packet[0], packet[1]]) as usize;
        packet.splice(2 + len..2 + len, [1, 2, 3, 4, 5, 6, 7, 8]);
        packet[..2].copy_from_slice(&((len + 8) as u16).to_be_bytes());
        client.write_all(&packet).await.unwrap();
        let pending = source
            .next(AcceptContext {
                deadline: None,
                cancel: host.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let AcceptedRequest::Datagram(request) = pending.complete(context).await.unwrap() else {
            panic!("Datagram")
        };
        let continuity = request.continuity().expect("authenticated XUDP identity");
        let mut expected = crate::parse_uuid(USER).unwrap().to_vec();
        expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(continuity.key.as_bytes(), expected);
        assert_eq!(
            continuity.termination.reason(),
            zero_core::InboundMuxUdpTermination::TransportDetached
        );
        let session = request
            .respond(ReplyOutcome::LocalChannelReady)
            .await
            .unwrap()
            .unwrap();
        client.write_all(&[0, 4, 0, 7, 3, 0]).await.unwrap();
        let mut rx = session.io.rx;
        // The queued opening packet precedes END in the original driver.
        let _ = zero_platform_tokio::contract::datagram::recv(rx.as_mut())
            .await
            .unwrap();
        assert!(zero_platform_tokio::contract::datagram::recv(rx.as_mut())
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            continuity.termination.reason(),
            zero_core::InboundMuxUdpTermination::ExplicitEnd
        );
        drop(rx);
        drop(session.io.tx);
        let mut control = session.io.control;
        assert!(control
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        drop(source);
        host.close(2).await;
    })
    .await
    .unwrap();
}
