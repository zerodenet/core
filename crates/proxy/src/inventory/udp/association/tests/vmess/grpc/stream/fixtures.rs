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
pub(super) async fn business(proxy: &Proxy) -> zero_platform_tokio::TcpRelayStream {
    let mut session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 9]),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    let receipt = proxy
        .engine()
        .prepare_session_admitted(&mut session, "incoming")
        .unwrap();
    let demand = crate::component::business_demand::AdmittedBusinessDemand::new(
        proxy.engine(),
        zero_core::component::SessionId(session.id),
        receipt,
    )
    .unwrap();
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_admitted_business_identity(&session, demand)
        .unwrap();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node".into()))
        .unwrap();
    let crate::inventory::PreparedTcpOutbound::Single(prepared) = services
        .prepare_tcp_outbound(&resolved, RouteMode::Flow)
        .unwrap_or_else(|error| panic!("{}", error.error))
    else {
        panic!("single native owner")
    };
    let crate::inventory::PreparedTcpCandidateExecution::Native(slot) = &prepared.execution else {
        panic!("new TCP cannot use compatibility execution")
    };
    crate::component::stream_bridge::connect(&services.execution(), &session, slot, &prepared)
        .await
        .unwrap_or_else(|error| panic!("{}: {}", error.stage, error.error))
        .into_relay_stream()
        .unwrap()
}
pub(super) async fn service(proxy: &Proxy) -> zero_platform_tokio::TcpRelayStream {
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
pub(super) async fn exchange(io: &mut zero_platform_tokio::TcpRelayStream) {
    io.write_all(b"live").await.unwrap();
    io.flush().await.unwrap();
    let mut bytes = [0; 4];
    io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"live");
}
