use super::*;
#[tokio::test]
async fn standard_service_tcp_and_real_udp_wire_share_original_pool_and_defer_only_real_response() {
    run(false).await;
}
#[tokio::test]
async fn standard_service_and_udp_wire_keep_the_original_tls_h2_profile_and_shared_driver() {
    run(true).await;
}
async fn run(secure: bool) {
    timeout(Duration::from_secs(8), async {
        let runtime = crate::transport::VlessTransportRuntime::default();
        let grpc = OwnedGrpcProfile {
            service_names: vec!["actual".into()],
            multi_mode: true,
            ..Default::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let server_profile = grpc.clone();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let acceptor = secure.then(acceptor);
        let tls = secure.then(|| OwnedClientTlsProfile {
            options: Default::default(),
            server_name: Some("localhost".into()),
            disable_sni: false,
            ca_cert_path: None,
            insecure: true,
            alpn: vec![],
            client_fingerprint: None,
        });
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let io = match acceptor {
                Some(acceptor) => {
                    let io = acceptor.accept(io).await.unwrap();
                    assert_eq!(io.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
                    zero_platform_tokio::TcpRelayStream::new(io)
                }
                None => zero_platform_tokio::TcpRelayStream::new(io),
            };
            let incoming =
                zero_transport::grpc::accept_grpc_connection(io, &server_profile).unwrap();
            let mut workers = tokio::task::JoinSet::new();
            for command in [1, 2] {
                let mut io = incoming.accept().await.unwrap();
                workers.spawn(async move {
                    let expected = golden(command);
                    let mut header = vec![0; expected.len()];
                    io.read_exact(&mut header).await.unwrap();
                    assert_eq!(header, expected);
                    let mut bytes = Vec::new();
                    io.read_to_end(&mut bytes).await.unwrap();
                    if command == 1 {
                        assert_eq!(bytes, vec![0x64; 196_613]);
                    } else {
                        assert_eq!(bytes, b"\0\x04real");
                    }
                    io.write_all(&[0, 0]).await.unwrap();
                    io.write_all(&bytes).await.unwrap();
                    io.shutdown().await.unwrap();
                });
            }
            while let Some(done) = workers.join_next().await {
                done.unwrap();
            }
            finished.await.unwrap();
            incoming.close();
        });
        let mut options = input(peer.port(), &grpc);
        options.tls = tls.as_ref();
        let prepared = PreparedGrpc::from_options_refs(
            options,
            &runtime,
            None,
            Default::default(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();
        let mut assembly = Assembly::cold(prepared, peer);
        assembly.start().await;
        let role = assembly.role.roles.stream.clone().unwrap();
        let connected = role
            .connect(
                ConnectRequest {
                    destination: destination(),
                },
                assembly.context(),
            )
            .await
            .unwrap();
        assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
        assert_eq!(
            *assembly.observations.facts.lock().unwrap(),
            [
                (None, ConnectFact::CarrierReady),
                (None, ConnectFact::RequestSubmitted)
            ]
        );
        assert_eq!(
            *assembly.observations.traffic.lock().unwrap(),
            [(None, 0, golden(1).len() as u64)]
        );
        let mut tcp = connected.io;
        let mut udp = assembly
            .pool_options
            .open(
                assembly.actual,
                zero_transport::grpc::Demand {
                    deadline: deadline(),
                    cancel: CancellationToken::new(),
                    binding: assembly.upstream.binding().clone(),
                },
            )
            .await
            .unwrap();
        let tcp_transfer = async {
            tcp.write_all(&vec![0x64; 196_613]).await.unwrap();
            tcp.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            tcp.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, vec![0x64; 196_613]);
        };
        let udp_transfer = async {
            udp.write_all(&golden(2)).await.unwrap();
            udp.write_all(b"\0\x04real").await.unwrap();
            udp.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            udp.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, b"\0\0\0\x04real");
        };
        tokio::join!(tcp_transfer, udp_transfer);
        assert_eq!(
            *assembly.observations.facts.lock().unwrap(),
            [
                (None, ConnectFact::CarrierReady),
                (None, ConnectFact::RequestSubmitted),
                (None, ConnectFact::PeerConfirmed)
            ]
        );
        drop(tcp);
        drop(udp);
        let (pool, tasks) = assembly.close().await;
        assert!(pool.is_complete(), "{pool:?}");
        assert_eq!(pool.closed_io.len(), 1);
        assert!(pool.exited_tasks.is_empty());
        assert!(tasks.is_complete());
        assert_eq!(tasks.exited_tasks.len(), 1);
        assert!(tasks.failed_tasks.is_empty());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

fn acceptor() -> tokio_rustls::TlsAcceptor {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec()];
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}
