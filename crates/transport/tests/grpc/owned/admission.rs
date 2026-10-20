use super::*;
#[tokio::test]
async fn native_owner_refuses_actual_legacy_pool_without_retiring_its_cache_or_admitted_streams() {
    timeout(Duration::from_secs(6), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let profile = crate::profile::OwnedGrpcProfile {
            service_names: vec!["owned".into()],
            ..Default::default()
        };
        let server_profile = profile.clone();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming = crate::grpc::accept_grpc_connection(io, &server_profile).unwrap();
            let mut streams = tokio::task::JoinSet::new();
            for _ in 0..2 {
                let mut io = incoming.accept().await.unwrap();
                streams.spawn(async move {
                    let (mut rx, mut tx) = tokio::io::split(&mut io);
                    tokio::io::copy(&mut rx, &mut tx).await.unwrap();
                });
            }
            finished.await.unwrap();
            streams.shutdown().await;
            incoming.close();
        });
        let pool = GrpcPool::default();
        let opens = AtomicUsize::new(0);
        let open = || {
            opens.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, crate::RuntimeError>(tokio::net::TcpStream::connect(peer).await?) }
        };
        let mut old = pool.open(&profile, "localhost", open).await.unwrap();
        let mut candidate = Fixture::on_pool(pool.clone(), peer, 1, 2, None, None);
        assert_eq!(
            candidate
                .owner
                .lifecycle
                .start(deadline())
                .await
                .unwrap_err()
                .reason,
            Reason::StalePlan
        );
        let (protocol, tasks) = candidate.close().await;
        assert!(protocol.is_complete());
        assert!(protocol.closed_io.is_empty());
        assert!(tasks.exited_tasks.is_empty());
        let mut still_same = pool.open(&profile, "localhost", open).await.unwrap();
        for io in [&mut old, &mut still_same] {
            io.write_all(b"kept").await.unwrap();
            io.flush().await.unwrap();
            let mut bytes = [0; 4];
            io.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"kept");
        }
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        finish.send(()).unwrap();
        drop(old);
        drop(still_same);
        drop(pool);
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn binding_generation_mismatch_is_rejected_before_real_native_acquisition() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut fixture = Fixture::cold(listener.local_addr().unwrap(), 1, 2, None, None);
    fixture.start().await;
    let mut demand = fixture.demand();
    demand.binding.network_generation = NetworkGeneration(demand.binding.network_generation.0 + 1);
    let failure = fixture
        .options
        .open(fixture.actual, demand)
        .await
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(
        failure.delivery,
        zero_platform_tokio::contract::DeliveryState::NotSubmitted
    );
    assert_eq!(fixture.traffic.boundaries.load(Ordering::SeqCst), 0);
    assert!(fixture.managed().records.lock().unwrap().is_empty());
    let (protocol, tasks) = fixture.close().await;
    assert!(protocol.is_complete());
    assert!(protocol.closed_io.is_empty());
    assert!(tasks.exited_tasks.is_empty());
}
#[test]
fn explicit_limits_and_unowned_material_are_never_silently_replaced() {
    let pool = GrpcPool::default();
    let settings = NativePoolSettings {
        server: "127.0.0.1".into(),
        port: 443,
        grpc: Default::default(),
        tls: None,
        dial: Default::default(),
    };
    let cap = NonZeroUsize::new(1).unwrap();
    let mut invalid = settings.clone();
    invalid.server = "needs-dns.example".into();
    assert!(NativePoolOptions::from_pool(&pool, invalid, cap, cap).is_err());
    let mut invalid = settings.clone();
    invalid.port = 0;
    assert!(NativePoolOptions::from_pool(&pool, invalid, cap, cap).is_err());
    let mut invalid = settings;
    invalid.tls = Some(OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: Some("owned/file.pem".into()),
        insecure: false,
        alpn: vec!["h2".into()],
        client_fingerprint: None,
    });
    assert!(NativePoolOptions::from_pool(&pool, invalid, cap, cap).is_err());
}
