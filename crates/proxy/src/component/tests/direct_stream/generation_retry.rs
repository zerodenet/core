//! Business Direct retries DNS resolution across an egress generation bump
//! without failing the refreshed candidate plan as StalePlan.
use super::*;

#[tokio::test]
async fn actual_direct_role_retries_domain_resolution_across_egress_generation_bump() {
    timeout(Duration::from_secs(12), async {
        let dns_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dns_addr = dns_socket.local_addr().unwrap();
        let config = zero_config::RuntimeConfig::parse(&format!(
            r#"{{"runtime":{{"dns":{{"servers":{{"actual":{{"type":"udp","host":"127.0.0.1","port":{}}}}},"default_server":"actual","policy":{{"address_family":"ipv4_only"}}}}}},"route":{{"final":{{"type":"direct"}}}}}}"#,
            dns_addr.port()
        ))
        .unwrap();
        let proxy = Proxy::new(config).unwrap();
        let running = proxy.spawn();
        tokio::time::timeout_at(deadline(), async {
            while proxy
                .protocols
                .current_components(&proxy.engine().config())
                .is_none()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let egress = proxy.egress_interface.clone();
        let (stop_dns, mut stopped_dns) = tokio::sync::oneshot::channel();
        let dns_server = tokio::spawn(async move {
            let mut queries = 0usize;
            loop {
                let mut bytes = [0u8; 2048];
                let (size, client) = tokio::select! { biased;
                    _ = &mut stopped_dns => break,
                    received = dns_socket.recv_from(&mut bytes) => received.unwrap(),
                };
                queries += 1;
                if queries <= 2 {
                    egress.invalidate_network();
                }
                let mut end = 12;
                while bytes[end] != 0 {
                    end += 1 + usize::from(bytes[end]);
                }
                end += 5;
                assert!(end <= size);
                let mut response = bytes[..end].to_vec();
                response[2] |= 0x80;
                response[3] |= 0x80;
                response[6..8].copy_from_slice(&1u16.to_be_bytes());
                response[8..12].fill(0);
                // ttl=0 so retry resolution executes at the new generation as well.
                response.extend_from_slice(&[0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4, 127, 0, 0, 1]);
                dns_socket.send_to(&response, client).await.unwrap();
            }
            queries
        });

        let policy = zero_traits::DialPolicy {
            source_ip: Some("127.0.0.1".parse().unwrap()),
            ..Default::default()
        };
        let options = DirectOptions::validate(policy.clone())
            .unwrap()
            .controlled(NonZeroUsize::new(4).unwrap(), one(), Duration::from_secs(1))
            .unwrap();
        let captured = Arc::new(Captured::default());
        let slot = Arc::new(ComponentSlot::from_recipe(
            ComponentId(2822),
            ResourceGeneration(1),
            ComponentRecipe::from_validated(Arc::new(Factory(captured.clone())), Arc::new(options)),
            Default::default(),
            "controlled-direct-retry",
        ));
        let host = TestContext::new([slot.as_ref()]);
        let role = host.drive(slot.stream(deadline())).await.unwrap();
        host.publish();
        let actual = captured.0.lock().unwrap().clone().unwrap();

        let session = Session::new(
            0,
            Address::Domain("direct.fixture.invalid".into()),
            peer.port(),
            Network::Tcp,
            ProtocolType::new("verified"),
        );
        let mut caller =
            crate::component::tests::business_carrier::sources::Caller::new_for_session(
                &proxy,
                crate::component::tests::business_carrier::sources::Kind::Accepted,
                session,
            )
            .await;
        let session = caller.session().clone();
        let invocation = caller.invocation().clone();
        let initial_generation = proxy
            .tcp_runtime_services()
            .execution()
            .upstream()
            .network_generation();
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: policy,
            network_generation: NetworkGeneration(initial_generation),
        };
        let destination = Destination::new(session.target.clone(), session.port);
        let demand = BusinessCarrierRequest::new(proxy.engine(), &slot, &invocation).unwrap();
        let opener = Arc::new(
            DirectOpener::new(
                proxy.tcp_runtime_services().execution().upstream(),
                session.clone(),
            )
            .with_business(demand),
        );
        let role_clone = role.clone();
        let opener_clone = opener.clone();
        let mut connected = host
            .drive(caller.run(move |invocation| async move {
                role_clone
                    .connect(
                        ConnectRequest { destination },
                        OpenContext {
                            identity: invocation.identity.clone(),
                            deadline: None,
                            cancel: invocation.cancellation(),
                            observer: None,
                            upstream: UpstreamTransport::new(binding, opener_clone),
                        },
                    )
                    .await
            }))
            .await
            .expect("retried Direct resolution should establish connection on new generation");
        assert_eq!(connected.peer_confirmation, PeerConfirmation::Confirmed);

        let evidence = opener
            .take_evidence()
            .unwrap()
            .unwrap_or_else(|_| panic!("Direct network evidence"));
        assert!(
            evidence
                .network
                .egress
                .as_ref()
                .expect("egress observation")
                .generation
                >= initial_generation + 2
        );

        let (mut remote, _) = listener.accept().await.unwrap();
        let server = tokio::spawn(async move {
            let mut buf = Vec::new();
            remote.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, b"ping");
            remote.write_all(b"pong").await.unwrap();
            remote.shutdown().await.unwrap();
        });
        host.drive(async {
            connected.io.write_all(b"ping").await.unwrap();
            connected.io.shutdown().await.unwrap();
            let mut reply = Vec::new();
            connected.io.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"pong");
        })
        .await;

        let originals: Vec<_> = actual
            .observe()
            .unwrap()
            .slots()
            .iter()
            .map(|entry| entry.original().clone())
            .collect();
        assert_eq!(originals.len(), 1);
        drop(connected);
        host.drive(caller.join()).await;
        proxy
            .engine()
            .track_session(session.id)
            .finish(zero_engine::SessionOutcome::DirectRelayed)
            .unwrap();
        server.await.unwrap();

        stop_dns.send(()).unwrap();
        let queries = dns_server.await.unwrap();
        assert!(queries >= 1);

        let closed = host.drive(slot.shutdown(deadline())).await.unwrap();
        assert!(closed.is_complete());
        for original in originals {
            let receipt = original.closed().expect("native control closed");
            assert!(original.matches_closed(&receipt));
        }
        caller.verify_carrier_completion();
        running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
