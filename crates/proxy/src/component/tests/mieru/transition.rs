//! The one actual Mieru pool rejects cross-executor reload before stopping old TCP.
use super::*;
use zero_core::{Address, Network, ProtocolType, Session};
fn unsupported(mut config: zero_config::RuntimeConfig) -> zero_config::RuntimeConfig {
    config.outbounds.push(serde_json::from_value(serde_json::json!({"tag":"legacy-grammar","protocol":{
        "type":"vmess","server":"127.0.0.1","port":1,"id":"11111111-2222-3333-4444-555555555555","grpc":{"service_names":["unused/Tun"]}
    }})).unwrap());
    config.outbound_groups.push(serde_json::from_value(serde_json::json!({"tag":"unused-legacy","type":"relay","proxies":["a","legacy-grammar"]})).unwrap());
    config
}
async fn open(proxy: &Proxy) -> zero_platform_tokio::TcpRelayStream {
    let session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 1]),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    let services = proxy.tcp_runtime_services().with_service_invocation(
        crate::component::direct_bridge::ServiceInvocation {
            deadline: deadline(),
            purpose: zero_core::request::ServicePurpose::DiagnosticProbe,
        },
    );
    let target = services.snapshot().plan().target_id("a").unwrap();
    let (resolved, _) = services
        .engine()
        .resolve_target_id_in_snapshot(services.snapshot(), target)
        .unwrap();
    crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        zero_engine::RouteMode::Flow,
        crate::runtime::tcp_dispatch::TcpDispatchIntent::DiagnosticProbe,
    )
    .await
    .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error))
    .into_relay_stream()
    .unwrap()
}
async fn echo(io: &mut zero_platform_tokio::TcpRelayStream) {
    io.write_all(b"live").await.unwrap();
    io.flush().await.unwrap();
    let mut data = [0; 4];
    io.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"live");
}
async fn start(config: zero_config::RuntimeConfig) -> (Proxy, crate::RunningProxy, ProxyHandle) {
    let proxy = Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    (proxy, running, handle)
}
#[tokio::test]
async fn ordinary_mieru_legacy_to_native_reload_preserves_actual_tcp_and_refuses_new_originals() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let a = Server::new(false).await;
        let b = Server::new(false).await;
        let c = Server::new(false).await;
        let native = config([a.address, b.address, c.address]);
        let initial = unsupported(native.clone());
        let (proxy, running, handle) = start(initial.clone()).await;
        assert!(proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .configured("a")
            .is_none());
        let mut old = open(&proxy).await;
        echo(&mut old).await;
        assert_eq!(a.accepts.load(Ordering::SeqCst), 1);
        let revision = proxy.config_revision();
        let error = handle
            .apply_runtime_config_and_wait(native.clone(), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(error.contains("Mieru legacy originals"), "{error}");
        assert_eq!(proxy.config_revision(), revision);
        assert_eq!(
            proxy.engine().runtime_snapshot().config().as_ref(),
            &initial
        );
        echo(&mut old).await;
        assert_eq!(a.accepts.load(Ordering::SeqCst), 1);
        assert_eq!(
            b.accepts.load(Ordering::SeqCst) + c.accepts.load(Ordering::SeqCst),
            0
        );
        assert!(proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .configured("a")
            .is_none());
        drop(old);
        assert!(
            handle
                .apply_runtime_config_and_wait(native, Duration::from_secs(5))
                .await
                .unwrap_err()
                .contains("Mieru legacy originals"),
            "Drop has no legacy driver/native proof"
        );
        let mut retained = open(&proxy).await;
        echo(&mut retained).await;
        drop(retained);
        running.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn ordinary_mieru_native_to_legacy_reload_rejects_before_revoking_actual_old_pool_or_tcp() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let a = Server::new(false).await;
        let b = Server::new(false).await;
        let c = Server::new(false).await;
        let initial = config([a.address, b.address, c.address]);
        let (proxy, running, handle) = start(initial.clone()).await;
        let mut old = open(&proxy).await;
        echo(&mut old).await;
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let slot = plan.configured("a").unwrap();
        let registration = plan
            .original_carrier_owner(slot.dependency_instances()[0], 0)
            .unwrap();
        let observed = registration.observe().unwrap();
        assert_eq!(observed.retained(), 1);
        let native = observed.slots()[0].original().clone();
        let driver = observed.slots()[0].driver().unwrap().1.clone();
        let error = handle
            .apply_runtime_config_and_wait(unsupported(initial.clone()), Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(error.contains("Mieru native owner"), "{error}");
        assert_eq!(
            proxy.engine().runtime_snapshot().config().as_ref(),
            &initial
        );
        assert_eq!(
            slot.instance_id(),
            proxy
                .tcp_runtime_services()
                .components
                .as_ref()
                .unwrap()
                .configured("a")
                .unwrap()
                .instance_id()
        );
        assert!(!driver.is_joined());
        assert!(native.closed().is_none());
        echo(&mut old).await;
        assert_eq!(
            a.accepts.load(Ordering::SeqCst),
            1,
            "same actual pool remains executable"
        );
        drop(old);
        running.shutdown().await.unwrap();
        assert!(driver.is_joined());
        assert!(native.matches_closed(&native.closed().unwrap()));
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .unwrap();
}
