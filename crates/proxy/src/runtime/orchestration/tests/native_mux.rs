//! Actual standard Stream -> one protocol pool -> original carrier -> host Scope.
use super::tcp_transport::fixtures::*;
use crate::{
    inventory::{PreparedTcpCandidateExecution, PreparedTcpOutbound},
    runtime::tcp_dispatch::{dispatch_tcp_outbound, TcpDispatchIntent},
    Proxy,
};
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Instant},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{RouteDecision, RouteMode};
mod cancelled;
mod cleanup;
#[cfg(feature = "upstream-association-runtime")]
mod datagram;
mod replacement;
mod server;
use server::Server;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn config(protocol: &str, port: u16) -> zero_config::RuntimeConfig {
    let mut outbound = credentials(protocol);
    outbound["server"] = json!("127.0.0.1");
    outbound["port"] = json!(port);
    outbound["mux_concurrency"] = json!(2);
    outbound["ws"] = json!({"path":"/mux"});
    if protocol == "trojan" {
        outbound["sni"] = json!("localhost");
        outbound["insecure"] = json!(true);
    } else {
        outbound["tls"] = json!({"server_name":"localhost","insecure":true});
    }
    zero_config::RuntimeConfig::parse(
        &json!({"outbounds":[{"tag":"a","protocol":outbound}],
        "route":{"final":{"type":"route","outbound":"a"}}})
        .to_string(),
    )
    .unwrap()
}
async fn start_components(proxy: &Proxy) -> crate::RunningProxy {
    let running = proxy.spawn();
    crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
    .apply_runtime_config_and_wait(
        proxy.engine().config().as_ref().clone(),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    running
}
async fn connect(proxy: &Proxy, port: u16) -> Result<crate::transport::TcpRelayStream, String> {
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        port,
        Network::Tcp,
        ProtocolType::new("native-mux-test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("a".into()))
        .unwrap();
    let prepared = services
        .prepare_tcp_outbound(&resolved, RouteMode::Flow)
        .map_err(|e| e.error.to_string())?;
    assert!(
        matches!(prepared,PreparedTcpOutbound::Single(ref candidate) if matches!(candidate.execution,PreparedTcpCandidateExecution::Native(_)))
    );
    dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        RouteMode::Flow,
        TcpDispatchIntent::Traffic,
    )
    .await
    .map_err(|e| format!("{}: {}", e.stage, e.error))?
    .into_relay_stream()
    .map_err(|e| e.to_string())
}
async fn echo(stream: &mut crate::transport::TcpRelayStream) {
    stream.write_all(b"real-mux").await.unwrap();
    let mut bytes = [0; 8];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"real-mux");
}
fn registration(
    proxy: &Proxy,
) -> zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration {
    let services = proxy.tcp_runtime_services();
    let plan = services.components.as_ref().unwrap();
    let slot = plan.configured("a").unwrap();
    assert_eq!(slot.dependency_instances().len(), 1);
    plan.original_carrier_owner(slot.dependency_instances()[0], 0)
        .unwrap()
}
#[test]
fn ordinary_proxy_keeps_unqualified_native_mux_registration_closed() {
    for protocol in ["vless", "vmess", "trojan"] {
        let proxy = Proxy::new(config(protocol, 1)).unwrap();
        assert!(proxy
            .tcp_runtime_services()
            .components
            .as_ref()
            .unwrap()
            .configured("a")
            .is_none());
    }
}
#[tokio::test]
async fn native_mux_actual_proxy_reuses_one_original_and_confirms_tasks_users_and_native_close() {
    timeout(Duration::from_secs(30), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let server = Server::start(protocol).await;
            let config = config(protocol, server.fixture.front.port());
            let proxy = Proxy::from_engine_with_background(
                zero_engine::Engine::new(config.clone()).unwrap(),
                true,
            )
            .unwrap();
            let running = start_components(&proxy).await;
            let registration = registration(&proxy);
            let mut first = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut first).await;
            let observation = registration.observe().unwrap();
            assert_eq!(observation.slots().len(), 1);
            let original = observation.slots()[0].original().clone();
            let driver = observation.slots()[0].driver().unwrap().1.clone();
            assert!(!driver.is_joined());
            proxy.protocols.on_config_reloaded(&config);
            let mut second = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut second).await;
            assert_eq!(
                server.fixture.connections(),
                1,
                "{protocol}: compatibility eviction must not retire host pool"
            );
            assert!(
                connect(&proxy, server.fixture.echo.port()).await.is_err(),
                "a saturated logical pool cannot create a second unconfirmed physical owner"
            );
            assert_eq!(registration.observe().unwrap().slots().len(), 1);
            assert_eq!(server.fixture.connections(), 1);
            drop(first);
            let mut third = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut third).await;
            assert_eq!(server.fixture.connections(), 1);
            drop(third);
            drop(second);
            proxy
                .protocols
                .shutdown_components(deadline())
                .await
                .unwrap();
            assert!(driver.is_joined());
            let proof = original.closed().unwrap();
            assert!(original.matches_closed(&proof) && proof.had_native_acquisition());
            assert!(driver.matches_joined(&driver.try_joined().unwrap()));
            assert!(registration.closed().unwrap().matches(&registration));
            assert!(registration.users_closed().unwrap().matches(&registration));
            running.shutdown().await.unwrap();
            server.close().await;
        }
    })
    .await
    .unwrap();
}
