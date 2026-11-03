//! Real reverse wire, original accepted Scope and retained historical views.
use super::*;
#[tokio::test]
async fn standard_reverse_tcp_udp_share_original_scope_and_retire_held_logical_views() {
    timeout(Duration::from_secs(10), async {
        let request = request_options(USER, None, None, Some("portal"));
        let mut host = fixture::Host::with_request(request).await;
        let mut client = TcpStream::connect(host.endpoint).await.unwrap();
        let pending = host
            .instance
            .roles
            .inbound
            .as_mut()
            .unwrap()
            .next(AcceptContext {
                deadline: None,
                cancel: host.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let stop = host.stop.clone();
        pending
            .spawn(TaskClass::Auxiliary, move |pending, tasks| async move {
                struct NoBusiness;
                impl SessionIdentitySource for NoBusiness {
                    fn allocate(&self) -> Result<SessionId, ChannelError> {
                        panic!("reverse auth is not business traffic")
                    }
                }
                let request = pending
                    .complete(HandshakeContext {
                        accepted: Some(tasks.caller()?),
                        connection: None,
                        inbound: RoleInstanceId {
                            component: ComponentId(11),
                            generation: ResourceGeneration(1),
                            role: RoleId::Inbound(InboundId(11)),
                        },
                        identities: Arc::new(NoBusiness),
                        deadline: Instant::now() + Duration::from_secs(3),
                        cancel: stop,
                    })
                    .await?;
                let AcceptedRequest::Control(control) = request else {
                    panic!("reverse control")
                };
                control.run().await.map_err(datagram::failure)
            })
            .unwrap();
        // Use the real wire handshake rather than exposing a private mux constructor.
        let mut header = vec![0];
        header.extend(crate::parse_uuid(USER).unwrap());
        header.extend([0, 4]);
        client.write_all(&header).await.unwrap();
        let mut reply = [0; 2];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(reply, [0, 0]);
        // A control heartbeat is emitted only after original scoped attachment.
        let heartbeat = crate::mux::read_mux_frame_tokio(&mut client).await.unwrap();
        assert_eq!(
            heartbeat.target.as_ref().unwrap().address,
            Address::Domain("reverse".into())
        );
        let session = zero_core::Session::new(
            701,
            Address::Ipv4([127, 0, 0, 1]),
            53,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("vless"),
        );
        let portal = host.portal.clone();
        let mut tcp = portal.open_tcp(&session).unwrap();
        tcp.write_all(b"tcp").await.unwrap();
        let opened = crate::mux::read_mux_frame_tokio(&mut client).await.unwrap();
        let data = crate::mux::read_mux_frame_tokio(&mut client).await.unwrap();
        assert_eq!(opened.session_id, data.session_id);
        assert_eq!(data.payload, b"tcp");
        client
            .write_all(&crate::mux::encode_data_frame(opened.session_id, b"back").unwrap())
            .await
            .unwrap();
        let mut back = [0; 4];
        tcp.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"back");
        let udp = portal.open_udp_connection(&session).unwrap();
        let mut responses = udp.subscribe_responses();
        let sent = udp.send(&session.target, 53, b"udp");
        let bridge = async {
            let packet = crate::mux::read_mux_frame_tokio(&mut client).await.unwrap();
            assert_eq!(packet.payload, b"udp");
            client
                .write_all(
                    &crate::mux::encode_udp_data_frame(
                        packet.session_id,
                        &Address::Ipv4([127, 0, 0, 1]),
                        53,
                        b"reply",
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        };
        let (sent, ()) = tokio::join!(sent, bridge);
        assert_eq!(sent.unwrap(), 3);
        let packet = responses.recv().await.unwrap();
        assert_eq!(packet.2, b"reply");
        let mut byte = [0];
        let mut waiting = Box::pin(tcp.read(&mut byte));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(waiting.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        // Keep historical logical handles while originals are joined/closed.
        host.close_at_least(5, None).await;
        assert!(matches!(waiting.await, Ok(0) | Err(_)));
        assert!(udp
            .send(&Address::Ipv4([127, 0, 0, 1]), 53, b"late")
            .await
            .is_err());
        assert!(portal.open_tcp(&session).is_err());
        assert!(portal.open_udp_connection(&session).is_err());
    })
    .await
    .unwrap();
}
