use super::*;
#[tokio::test]
async fn controlled_vmess_grpc_ca_uses_a_fresh_real_client_per_connect_and_preserves_confirmation_and_half_close(
) {
    timeout(Duration::from_secs(10), async {
        for multi in [false, true] {
            let directory = Directory::new();
            let path = directory.0.join("identity.pem");
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let peer = listener.local_addr().unwrap();
            let (acceptor, pem) = tls_server(true);
            let mut tls = tls_profile();
            tls.alpn = vec!["h2".into()];
            let grpc = profile(multi);
            let server_grpc = grpc.clone();
            let runtime = vmess::transport::VmessTransportRuntime::default();
            let mut fixture = Fixture::cold(
                peer,
                &runtime,
                Some(&tls),
                &grpc,
                Some(&directory.0),
                (pem.len(), 1),
                None,
            );
            assert_eq!(fixture.observations.file_reads.load(Ordering::Relaxed), 0);
            std::fs::write(&path, pem).unwrap();
            fixture.start().await;
            assert_eq!(fixture.observations.file_reads.load(Ordering::Relaxed), 1);
            std::fs::remove_file(path).unwrap();
            let (finish, finished) = tokio::sync::watch::channel(false);
            let server = tokio::spawn(async move {
                let mut workers = tokio::task::JoinSet::new();
                let mut sources = Vec::new();
                for _ in 0..2 {
                    let (io, source) = listener.accept().await.unwrap();
                    sources.push(source);
                    let acceptor = acceptor.clone();
                    let profile = server_grpc.clone();
                    let mut finished = finished.clone();
                    workers.spawn(async move {
                        let tls = acceptor.accept(io).await.unwrap();
                        assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
                        let incoming =
                            zero_transport::grpc::accept_grpc_connection(tls, &profile).unwrap();
                        let carrier = incoming.accept().await.unwrap();
                        let (facts, mut io) = inbound()
                            .accept_request_stream(
                                vmess::inbound::VmessInbound,
                                zero_platform_tokio::TcpRelayStream::new(carrier),
                            )
                            .await
                            .unwrap();
                        assert_eq!(facts.destination, destination());
                        let mut payload = Vec::new();
                        io.read_to_end(&mut payload).await.unwrap();
                        assert_eq!(payload, b"original-payload");
                        io.write_all(b"reverse-after-eof").await.unwrap();
                        io.shutdown().await.unwrap();
                        if !*finished.borrow() {
                            let _ = finished.wait_for(|done| *done).await.unwrap();
                        }
                        incoming.close();
                    });
                }
                assert_ne!(
                    sources[0], sources[1],
                    "two demands create two actual TCP/H2 Clients"
                );
                while let Some(result) = workers.join_next().await {
                    result.unwrap();
                }
            });
            for _ in 0..2 {
                let mut connected = fixture
                    .owner
                    .roles
                    .stream
                    .as_ref()
                    .unwrap()
                    .connect(
                        ConnectRequest {
                            destination: destination(),
                        },
                        fixture.context(),
                    )
                    .await
                    .unwrap();
                assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
                assert_eq!(
                    fixture.observations.protocol.lock().unwrap().as_ref(),
                    Some(&(0, REQUEST_LEN))
                );
                connected.io.write_all(b"original-payload").await.unwrap();
                connected.io.shutdown().await.unwrap();
                let mut bytes = Vec::new();
                connected.io.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, b"reverse-after-eof");
                assert_eq!(
                    fixture.observations.facts.lock().unwrap().last(),
                    Some(&(None, ConnectFact::PeerConfirmed))
                );
                drop(connected);
                let denied = fixture
                    .owner
                    .roles
                    .stream
                    .as_ref()
                    .unwrap()
                    .connect(
                        ConnectRequest {
                            destination: destination(),
                        },
                        fixture.context(),
                    )
                    .await
                    .err()
                    .unwrap();
                assert_eq!(
                    denied.reason,
                    Reason::Capacity,
                    "Drop/body exit alone cannot refund the original native slot"
                );
                let exit = fixture.scope.next_exit().await.unwrap();
                assert_eq!(
                    exit.class,
                    zero_platform_tokio::contract::scope::TaskClass::Auxiliary
                );
                assert!(exit.result.is_ok());
            }
            assert_eq!(fixture.observations.file_reads.load(Ordering::Relaxed), 1);
            assert!(fixture.observations.outer.load(Ordering::Relaxed) > 2 * REQUEST_LEN);
            assert_eq!(fixture.observations.boundaries.load(Ordering::Relaxed), 2);
            let (physical, tasks) = fixture.close().await;
            // The first matching private close receipt was consumed during
            // admission reap. Shutdown reports the remaining actual carrier;
            // the registry does not retain an unbounded closed-I/O history.
            assert_eq!(physical.closed_io.len(), 1);
            assert!(
                tasks.exited_tasks.is_empty(),
                "both real exits were already consumed above"
            );
            finish.send_replace(true);
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}
