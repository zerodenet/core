use super::*;
pub(super) async fn server(listener: TcpListener, mut release: tokio::sync::watch::Receiver<bool>) {
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..3 {
        let (stream, _) = listener.accept().await.unwrap();
        let mut release = release.clone();
        tasks.spawn(async move {
            let profile = zero_transport::profile::OwnedGrpcProfile {
                service_names: vec!["same.owner/Tun".into()],
                multi_mode: true,
                ..Default::default()
            };
            let incoming = zero_transport::grpc::accept_grpc_connection(stream, &profile).unwrap();
            let carrier = incoming.accept().await.unwrap();
            let profile = vmess::inbound::VmessInboundProfile::from_config_users([(
                ID.into(),
                "aes-128-gcm".into(),
                None::<String>,
                None,
                None,
                None,
                None,
                None,
            )])
            .unwrap();
            let (facts, mut io) = profile
                .accept_request_stream(
                    vmess::inbound::VmessInbound,
                    zero_platform_tokio::TcpRelayStream::new(carrier),
                )
                .await
                .unwrap();
            assert_eq!(facts.destination.address, Address::Ipv4([203, 0, 113, 9]));
            let bytes = if facts.destination.port == 53 {
                let size = io.read_u16().await.unwrap();
                let mut body = vec![0; size as usize];
                io.read_exact(&mut body).await.unwrap();
                assert_eq!(&body[..7], &[1, 203, 0, 113, 9, 0, 53]);
                assert_eq!(&body[7..], b"live");
                io.write_u16(size).await.unwrap();
                body
            } else {
                assert_eq!(facts.destination.port, 443);
                let mut body = vec![0; 4];
                io.read_exact(&mut body).await.unwrap();
                assert_eq!(&body, b"live");
                body
            };
            io.write_all(&bytes).await.unwrap();
            io.flush().await.unwrap();
            let mut byte = [0];
            if let Ok(n) = io.read(&mut byte).await {
                assert_eq!(n, 0);
            }
            // The peer stays alive until the original host has independently
            // confirmed its own native close and exact transport driver join.
            if !*release.borrow() {
                release.wait_for(|done| *done).await.unwrap();
            }
            incoming.close();
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.unwrap();
    }
    if !*release.borrow() {
        release.wait_for(|done| *done).await.unwrap();
    }
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(listener.poll_accept(&mut cx).is_pending());
}
pub(super) async fn business(
    proxy: &Proxy,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    use tokio::sync::oneshot;

    let (ready, opened) = oneshot::channel();
    let (retire, retired) = oneshot::channel();
    let proxy = proxy.clone();
    let worker = tokio::spawn(async move {
        crate::runtime::udp_ingress::test_source::run(&proxy, |_, source| async move {
            let mut session = Session::new(
                0,
                Address::Ipv4([203, 0, 113, 9]),
                443,
                Network::Tcp,
                ProtocolType::new("test"),
            );
            let runtime = source.tcp_runtime("incoming");
            let mut route = runtime.open_tcp_upstream(&mut session).await.unwrap();
            assert_eq!(route.outbound_tag, "node");
            exchange(&mut route.upstream).await;
            ready.send(()).unwrap();
            retired.await.unwrap();
            drop(route);
            assert!(runtime
                .track_session(session.id)
                .finish(zero_engine::SessionOutcome::ChainedRelayed)
                .is_some());
        })
        .await;
    });
    opened.await.unwrap();
    (retire, worker)
}
pub(super) async fn service(proxy: &Proxy) -> zero_platform_tokio::contract::stream::BoxStream {
    use zero_platform_tokio::contract::{network::CarrierContext, CancellationToken};
    let services = proxy.tcp_runtime_services();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node".into()))
        .unwrap();
    let plan =
        crate::runtime::tcp_dispatch::service::prepare(services.protocols(), &services, &resolved)
            .unwrap_or_else(|error| panic!("{}", error.error))
            .expect("actual Node Stream");
    crate::runtime::tcp_dispatch::service::connect(
        services.execution(),
        zero_core::request::Destination::new(Address::Ipv4([203, 0, 113, 9]), 443),
        plan,
        zero_core::request::ServicePurpose::DiagnosticProbe,
        CarrierContext {
            session: None,
            deadline: Some(Instant::now() + Duration::from_secs(5)),
            cancel: CancellationToken::new(),
        },
    )
    .await
    .unwrap()
    .stream
}
pub(super) async fn exchange(io: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin)) {
    io.write_all(b"live").await.unwrap();
    io.flush().await.unwrap();
    let mut bytes = [0; 4];
    io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"live");
}
