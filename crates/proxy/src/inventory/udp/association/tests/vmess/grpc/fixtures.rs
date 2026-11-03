use super::*;
pub(super) struct Server {
    release: tokio::sync::watch::Sender<bool>,
    accepted: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub(super) fn accepted(&self) -> usize {
        self.accepted.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub(super) async fn finish(self) {
        self.release.send_replace(true);
        self.task.await.unwrap();
    }
}
pub(super) async fn start(
    tls: bool,
    multi_mode: bool,
    cipher: &str,
    target: &str,
    payloads: Vec<Vec<u8>>,
) -> (Proxy, Server) {
    start_count(tls, multi_mode, cipher, target, payloads, 1).await
}
pub(super) async fn start_count(
    tls: bool,
    multi_mode: bool,
    cipher: &str,
    target: &str,
    payloads: Vec<Vec<u8>>,
    count: usize,
) -> (Proxy, Server) {
    start_count_ca(tls, multi_mode, cipher, target, payloads, count, false).await
}
pub(super) async fn start_count_ca(
    tls: bool,
    multi_mode: bool,
    cipher: &str,
    target: &str,
    payloads: Vec<Vec<u8>>,
    count: usize,
    trusted_ca: bool,
) -> (Proxy, Server) {
    start_config(
        tls,
        multi_mode,
        cipher,
        target,
        payloads,
        count,
        trusted_ca,
        |_| {},
    )
    .await
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn start_config(
    tls: bool,
    multi_mode: bool,
    cipher: &str,
    target: &str,
    payloads: Vec<Vec<u8>>,
    count: usize,
    trusted_ca: bool,
    configure: impl FnOnce(&mut zero_config::RuntimeConfig),
) -> (Proxy, Server) {
    start_config_mode(
        tls,
        multi_mode,
        cipher,
        target,
        payloads,
        count,
        trusted_ca,
        false,
        Box::new(configure),
    )
    .await
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn start_config_mode(
    tls: bool,
    multi_mode: bool,
    cipher: &str,
    target: &str,
    payloads: Vec<Vec<u8>>,
    count: usize,
    trusted_ca: bool,
    native: bool,
    configure: Box<dyn FnOnce(&mut zero_config::RuntimeConfig) + '_>,
) -> (Proxy, Server) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let profile = zero_transport::profile::OwnedGrpcProfile {
        service_names: vec!["actual.udp/Tun".into()],
        multi_mode,
        ..Default::default()
    };
    let material = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = material.path().join("cert.pem");
    let key_path = material.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let acceptor = zero_transport::tls::build_tls_acceptor(
        &zero_transport::profile::OwnedServerTlsProfile {
            options: Default::default(),
            cert_path: cert_path.display().to_string(),
            key_path: key_path.display().to_string(),
            alpn: vec!["h2".into()],
            server_fingerprint: None,
        },
        None,
    )
    .unwrap();
    let mut protocol = serde_json::json!({"type":"vmess","server":"127.0.0.1","port":port,"id":ID,"cipher":cipher,"grpc":{"service_names":["actual.udp/Tun"],"multi_mode":multi_mode}});
    if tls {
        protocol["tls"] =
            serde_json::json!({"server_name":"localhost","insecure":!trusted_ca,"alpn":["h2"]});
        if trusted_ca {
            protocol["tls"]["ca_cert_path"] = "cert.pem".into();
        }
    }
    let proxy = proxy_with_configure_mode(
        protocol,
        serde_json::json!({"final":{"type":"route","outbound":target}}),
        true,
        Some(material.path().to_path_buf()),
        native,
        configure,
    )
    .await;
    if trusted_ca {
        std::fs::remove_file(&cert_path).unwrap();
    }
    let (release, rx) = tokio::sync::watch::channel(false);
    let cipher = cipher.to_owned();
    let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = accepted.clone();
    let task = tokio::spawn(async move {
        let _material = material;
        let mut tasks = tokio::task::JoinSet::new();
        let mut accept_stop = rx.clone();
        for _ in 0..count {
            let (stream, _) = tokio::select! {biased;_=accept_stop.changed()=>break,value=listener.accept()=>value.unwrap()};
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (acceptor, profile, cipher, payloads, mut rx) = (
                acceptor.clone(),
                profile.clone(),
                cipher.clone(),
                payloads.clone(),
                rx.clone(),
            );
            tasks.spawn(async move {
                let stream = zero_platform_tokio::TokioSocket::new(stream);
                let stream = if tls {
                    zero_platform_tokio::TcpRelayStream::new(
                        zero_transport::tls::accept_tls_handshake(&acceptor, stream)
                            .await
                            .unwrap(),
                    )
                } else {
                    zero_platform_tokio::TcpRelayStream::from_client(stream)
                };
                let incoming =
                    zero_transport::grpc::accept_grpc_connection(stream, &profile).unwrap();
                let stream = incoming.accept().await.unwrap();
                echo_io_with_close(
                    zero_platform_tokio::TcpRelayStream::new(stream),
                    cipher,
                    53,
                    payloads,
                    false,
                )
                .await;
                // Keep the real peer H2 connection alive until the client's original
                // controller has produced its own receipt; peer Drop proves nothing.
                if !*rx.borrow() {
                    rx.wait_for(|done| *done).await.unwrap();
                }
                incoming.close();
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    });
    (
        proxy,
        Server {
            release,
            task,
            accepted,
        },
    )
}
pub(super) async fn response(
    dispatch: &mut crate::runtime::udp_dispatch::UdpDispatch,
) -> (Address, u16, Vec<u8>, Option<u64>) {
    let (_, tasks, _) = dispatch.poll_sockets();
    timeout(Duration::from_secs(3), tasks.join_next_with_id())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap()
}
pub(super) fn original(
    proxy: &Proxy,
) -> zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration {
    let execution = proxy.tcp_runtime_services().execution();
    let slot = execution
        .components
        .as_ref()
        .unwrap()
        .configured("node")
        .unwrap();
    let ordinal = slot
        .recipe
        .spec()
        .resources
        .iter()
        .position(|resource| {
            matches!(resource,
        zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeStream { .. })
        })
        .unwrap();
    execution
        .components
        .as_ref()
        .unwrap()
        .configured("node")
        .unwrap()
        .original_carrier_owner(ordinal)
        .unwrap()
}
pub(super) fn assert_no_legacy_udp(proxy: &Proxy) {
    let config = proxy.engine().runtime_snapshot().config().clone();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("node".into()))
        .unwrap();
    let zero_engine::ResolvedOutbound::Single(leaf) = resolved else {
        panic!("single")
    };
    let claim = proxy.protocols.claim_outbound_leaf(&config, leaf).unwrap();
    assert!(
        claim.tcp_connect_for_route(RouteMode::Flow).is_ok(),
        "TCP compatibility remains executable"
    );
    assert!(claim.into_udp_flow_for_route(RouteMode::Flow).is_err());
}

pub(super) async fn exercise_datagram(proxy: Proxy, server: Server, payloads: Vec<Vec<u8>>) {
    assert_standard_datagram(&proxy);
    assert_no_legacy_udp(&proxy);
    proxy
        .protocols
        .drive_component_supervision(async {
            let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
            let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
            let mut ids = vec![];
            for payload in &payloads {
                ids.push(dispatch.dispatch(input(53, payload)).await.unwrap());
                let packet = response(&mut dispatch).await;
                assert_eq!(packet.2, *payload);
                assert_eq!(packet.3, Some(ids[0]));
                crate::runtime::udp_delivery::record_chain_udp_response_parts(
                    &proxy.udp_runtime_services(),
                    &dispatch,
                    packet.0,
                    packet.1,
                    packet.2,
                    packet.3,
                );
            }
            assert!(ids.iter().all(|id| *id == ids[0]));
            let active = proxy.engine().active_sessions();
            let active = active.iter().find(|s| s.id == ids[0]).unwrap();
            assert_eq!(
                active.outbound_tx_bytes,
                37004 + 3 * 9,
                "gRPC/H2/TLS bytes must not enter Flow usage"
            );
            assert_eq!(active.outbound_rx_bytes, 37004);
            assert_eq!(active.path.remote.as_ref().unwrap().host, "127.0.0.1");
            let original = original(&proxy);
            let observed = original.observe().unwrap();
            assert_eq!(
                observed.slots().len(),
                1,
                "same flow reuses its one real carrier"
            );
            let native = observed.slots()[0].original().clone();
            let driver = observed.slots()[0].driver().unwrap().1.clone();
            assert!(native.closed().is_none());
            let retired = proxy.tcp_runtime_services().execution().retired_datagrams();
            let completed = dispatch.finish_all();
            assert_eq!(completed[0].upstream.as_ref().unwrap().0, "127.0.0.1");
            let closed = retired
                .wait_closed(Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            assert!(closed.is_complete(), "{closed:?}");
            assert!(driver.is_joined());
            assert!(
                native.matches_closed(&native.closed().expect("actual original native receipt"))
            );
            assert!(
                original.closed().is_none(),
                "per-flow closure is not whole-owner shutdown"
            );
            server.finish().await;
            proxy
                .protocols
                .shutdown_components(Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
            assert!(original.closed().unwrap().matches(&original));
        })
        .await;
}
