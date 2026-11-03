use super::*;
pub(super) const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) async fn proxy(port: u16, route: serde_json::Value, cipher: &str) -> Proxy {
    proxy_with_udp(port, route, cipher, true).await
}
pub(super) async fn proxy_with_udp(
    port: u16,
    route: serde_json::Value,
    cipher: &str,
    enabled: bool,
) -> Proxy {
    proxy_with_protocol(serde_json::json!({"type":"vmess","server":"127.0.0.1","port":port,"id":ID,"cipher":cipher}), route,enabled).await
}
pub(super) async fn proxy_with_protocol(
    protocol: serde_json::Value,
    route: serde_json::Value,
    enabled: bool,
) -> Proxy {
    proxy_with_protocol_at(protocol, route, enabled, None).await
}
pub(super) async fn proxy_with_protocol_at(
    protocol: serde_json::Value,
    route: serde_json::Value,
    enabled: bool,
    source_dir: Option<std::path::PathBuf>,
) -> Proxy {
    proxy_with_configure(protocol, route, enabled, source_dir, |_| {}).await
}
pub(super) async fn proxy_with_configure(
    protocol: serde_json::Value,
    route: serde_json::Value,
    enabled: bool,
    source_dir: Option<std::path::PathBuf>,
    configure: impl FnOnce(&mut zero_config::RuntimeConfig),
) -> Proxy {
    proxy_with_configure_mode(
        protocol,
        route,
        enabled,
        source_dir,
        false,
        Box::new(configure),
    )
    .await
}
pub(super) async fn proxy_with_configure_mode(
    protocol: serde_json::Value,
    route: serde_json::Value,
    enabled: bool,
    source_dir: Option<std::path::PathBuf>,
    native: bool,
    configure: Box<dyn FnOnce(&mut zero_config::RuntimeConfig) + '_>,
) -> Proxy {
    let mut config=zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({
        "outbounds":[{"tag":"node","udp":{"enabled":enabled},"protocol":protocol}],"outbound_groups":[{"tag":"choice","type":"selector","outbounds":["node"]},{"tag":"backup","type":"fallback","outbounds":["choice"]}],"route":route
    }).to_string(),source_dir).unwrap();
    configure(&mut config);
    config.validate().unwrap();
    let proxy = if native {
        Proxy::from_engine_with_background(zero_engine::Engine::new(config).unwrap(), true)
    } else {
        Proxy::new(config)
    }
    .unwrap();
    let config = proxy.engine().runtime_snapshot().config().clone();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    proxy
}
pub(super) fn input(port: u16, payload: &[u8]) -> UdpPipeInput<'_> {
    UdpPipeInput {
        peer_identity: None,
        target: Address::Ipv4([203, 0, 113, 9]),
        port,
        payload,
        route_target: None,
        sniffed_original_target: None,
        sniffed_host_source: None,
        skip_fake_ip_restore: false,
        protocol: ProtocolType::new("test"),
        auth: None,
        source_addr: None,
        client_session_id: None,
        transparent_target: false,
        transparent_original_target: None,
        transparent_host_source: None,
    }
}
pub(super) async fn echo(listener: TcpListener, cipher: String, port: u16, payloads: Vec<Vec<u8>>) {
    let (stream, _) = listener.accept().await.unwrap();
    echo_stream(stream, cipher, port, payloads).await;
}
pub(super) async fn echo_stream(
    stream: tokio::net::TcpStream,
    cipher: String,
    port: u16,
    payloads: Vec<Vec<u8>>,
) {
    echo_io(
        zero_platform_tokio::TcpRelayStream::from_client(zero_platform_tokio::TokioSocket::new(
            stream,
        )),
        cipher,
        port,
        payloads,
    )
    .await;
}
pub(super) async fn echo_io(
    stream: zero_platform_tokio::TcpRelayStream,
    cipher: String,
    port: u16,
    payloads: Vec<Vec<u8>>,
) {
    echo_io_with_close(stream, cipher, port, payloads, true).await
}
pub(super) async fn echo_io_with_close(
    stream: zero_platform_tokio::TcpRelayStream,
    cipher: String,
    port: u16,
    payloads: Vec<Vec<u8>>,
    require_peer_eof: bool,
) {
    let profile = vmess::inbound::VmessInboundProfile::from_config_users([(
        ID.into(),
        cipher,
        None::<String>,
        None,
        None,
        None,
        None,
        None,
    )])
    .unwrap();
    let (facts, mut stream) = profile
        .accept_request_stream(vmess::inbound::VmessInbound, stream)
        .await
        .unwrap();
    assert_eq!(facts.destination.port, port);
    for expected in payloads {
        let len = stream.read_u16().await.unwrap() as usize;
        let mut body = vec![0; len];
        stream.read_exact(&mut body).await.unwrap();
        assert_eq!(&body[..5], &[1, 203, 0, 113, 9]);
        assert_eq!(&body[5..7], &port.to_be_bytes());
        assert_eq!(&body[7..], expected);
        // Split framing deliberately across separate encrypted chunks.
        stream
            .write_all(&(len as u16).to_be_bytes()[..1])
            .await
            .unwrap();
        stream.flush().await.unwrap();
        stream
            .write_all(&(len as u16).to_be_bytes()[1..])
            .await
            .unwrap();
        stream.flush().await.unwrap();
        stream.write_all(&body).await.unwrap();
        stream.flush().await.unwrap();
    }
    let mut byte = [0];
    let close = stream.read(&mut byte).await;
    if require_peer_eof {
        assert_eq!(close.unwrap(), 0);
        stream.shutdown().await.unwrap();
    } else {
        // Cancellation may surface as a real H2 reset or EOF. The client test
        // proves closure with its original provider, never this peer result.
        if let Ok(length) = close {
            assert_eq!(length, 0);
        }
    }
}

pub(super) fn assert_standard_datagram(proxy: &Proxy) {
    let config = proxy.engine().runtime_snapshot().config().clone();
    let (resolved, _plan) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node".into()))
        .unwrap();
    let session = Session::new(
        7,
        Address::Ipv4([203, 0, 113, 9]),
        53,
        Network::Udp,
        ProtocolType::new("test"),
    );
    let prepared = proxy
        .protocols
        .prepare_udp_outbound(
            UdpAdapterContext::new(&config, proxy.udp_runtime_services()),
            &session,
            &resolved,
            b"probe",
            RouteMode::Flow,
        )
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(matches!(
        prepared,
        PreparedUdpOutbound::Single(PreparedUdpLeafCandidate::DatagramFlow(_))
    ));
}
