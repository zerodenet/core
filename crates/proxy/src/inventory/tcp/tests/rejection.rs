use super::*;

#[tokio::test]
async fn mixed_standard_final_rejection_preserves_actual_identity_source_and_never_replays() {
    timeout(Duration::from_secs(5), async {
        let (proxy, session, services, mut listeners) = fixtures::setup(2).await;
        let prepared = prepare(&proxy, &services, 0);
        let first = listeners.remove(0);
        let last = listeners.remove(0);
        let last_port = last.local_addr().unwrap().port();
        let slot = services
            .execution()
            .components
            .unwrap()
            .configured("node-1")
            .unwrap();
        let forward = tokio::spawn(async move {
            let (mut io, _) = first.accept().await.unwrap();
            assert_eq!(
                fixtures::request(&mut io).await,
                (zero_core::Address::Domain("127.0.0.1".into()), last_port)
            );
            let mut next = tokio::net::TcpStream::connect(("127.0.0.1", last_port))
                .await
                .unwrap();
            fixtures::accept(&mut io).await;
            tokio::io::copy_bidirectional(&mut io, &mut next)
                .await
                .unwrap();
        });
        let reject = tokio::spawn(async move {
            let (mut io, _) = last.accept().await.unwrap();
            fixtures::greeting(&mut io).await;
            io.write_all(&[5, 255]).await.unwrap();
            assert_eq!(
                io.read_u8().await.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof
            );
        });
        let failure = match crate::runtime::tcp_dispatch::relay::dispatch_prepared_tcp_relay_chain(
            services.execution(),
            &session,
            prepared,
            crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic,
        )
        .await
        {
            Ok(_) => panic!("actual peer rejected the handshake"),
            Err(failure) => failure,
        };
        assert_eq!(failure.stage, "relay_last");
        let channel = crate::transport::channel_failure(&failure.error).unwrap();
        assert_eq!(channel.component, Some(slot.id));
        assert_eq!(
            channel.session,
            Some(zero_core::component::SessionId(session.id))
        );
        assert_eq!(
            channel.reason,
            zero_platform_tokio::contract::Reason::Protocol
        );
        assert_eq!(
            channel.delivery,
            zero_platform_tokio::contract::DeliveryState::Unknown
        );
        assert!(!channel.retry_permitted());
        assert!(std::error::Error::source(channel).is_some());
        assert!(failure
            .error
            .to_string()
            .contains(&format!("upstream_port={last_port}")));
        reject.await.unwrap();
        forward.await.unwrap();
        proxy
            .protocols
            .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    })
    .await
    .expect("real rejection must release both actual sockets");
}
