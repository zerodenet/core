pub(super) mod fixtures;
use crate::{protocol_registry::OutboundAdapterContext, Proxy};
use fixtures::*;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
    time::{timeout, Duration},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{ResolvedOutbound, RouteDecision, RouteMode};

#[tokio::test]
async fn materialized_transport_leaves_keep_relative_roots_and_isolate_real_mux_owners() {
    let trace = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let _trace = tracing::subscriber::set_default(trace);
    timeout(Duration::from_secs(20), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let material = material();
            let mut inbound = inbound_credentials(protocol);
            inbound["tls"] = json!({"cert_path":"ca.pem", "key_path":"key.pem"});
            inbound["ws"] = json!({"path":"/mux"});
            let config = config_at(
                json!({"inbounds":[{"tag":"incoming",
                "listen":{"address":"127.0.0.1","port":1},"protocol":inbound}],
                "route":{"final":{"type":"direct"}}}),
                material.path(),
            );
            let upstream = Proxy::new(config.clone()).unwrap();
            start_components(&upstream).await;
            let mut input = config.inbounds[0].clone();
            input.listen.port = 0;
            // Keep the actual bound listener until the real runtime takes it.
            let bound = upstream
                .protocols
                .bind_inbound(&input, config.source_dir())
                .await
                .unwrap();
            let address = match &bound {
                crate::protocol_registry::BoundInbound::Tcp(listener) => {
                    listener.local_addr().unwrap()
                }
                _ => panic!("actual TCP listener"),
            };
            let mut operation = upstream
                .protocols
                .prepare_inbound_listener(input, config.source_dir())
                .unwrap();
            let (shutdown, rx) = watch::channel(false);
            let installed = upstream
                .inbound_owners
                .install_listener(
                    "incoming",
                    &upstream.protocols,
                    operation.internal_tasks(),
                    shutdown.clone(),
                )
                .unwrap();
            operation.install_resources(&installed).unwrap();
            let owner = installed.owner();
            let runtime = crate::runtime::route_runtime::InboundListenerRuntime::new(
                crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
                    upstream.tcp_runtime_services(),
                ),
                "incoming".into(),
                Some(owner.task_access()),
            );
            owner
                .spawn_driver(operation.execute(runtime, bound, rx))
                .unwrap();
            owner.seal_listener_installation();
            let inbound_worker = tokio::spawn(async move { owner.observe_listener().await });
            let fixture = Fixture::start(address).await;
            let mut outbound = credentials(protocol);
            outbound["server"] = json!("127.0.0.1");
            outbound["port"] = json!(fixture.front.port());
            outbound["mux_concurrency"] = json!(8);
            outbound["ws"] = json!({"path":"/mux"});
            if protocol == "trojan" {
                outbound["sni"] = json!("localhost");
                outbound["insecure"] = json!(true);
            } else {
                outbound["tls"] = json!({"server_name":"localhost","ca_cert_path":"ca.pem"});
            }
            let base = json!({"outbounds":[{"tag":"a","protocol":outbound.clone()},
                {"tag":"b","protocol":outbound.clone()}],
                "route":{"final":{"type":"route","outbound":"a"}}});
            let config = config_at(base.clone(), material.path());
            let outer = Proxy::new(config.clone()).unwrap();
            start_components(&outer).await;
            round_trip(&outer, &config, "a", fixture.echo.port()).await;
            round_trip(&outer, &config, "a", fixture.echo.port()).await;
            assert_eq!(
                fixture.connections(),
                1,
                "{protocol}: same actual MUX owner must reuse its carrier"
            );

            // Same owner, endpoint and credentials, but incompatible private
            // transport material. An old cached MUX must not bypass this TLS/WS
            // handshake. Both failed handshakes reach the actual peer.
            let mut incompatible = base.clone();
            if protocol == "trojan" {
                incompatible["outbounds"][0]["protocol"]["ws"]["path"] = json!("/rejected");
            } else {
                incompatible["outbounds"][0]["protocol"]["tls"]["ca_cert_path"] =
                    json!("untrusted.pem");
            }
            let incompatible = config_at(incompatible, material.path());
            assert!(
                connect(&outer, &incompatible, "a", fixture.echo.port())
                    .await
                    .is_err(),
                "{protocol}: cached MUX bypassed changed transport authentication"
            );
            assert_eq!(fixture.connections(), 2);
            round_trip(&outer, &config, "b", fixture.echo.port()).await;
            round_trip(&outer, &config, "a", fixture.echo.port()).await;
            assert_eq!(
                fixture.connections(),
                3,
                "{protocol}: outbound owners stay distinct; original owner stays reusable"
            );
            outer
                .protocols
                .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
            drop(outer);
            shutdown.send(true).unwrap();
            inbound_worker.await.unwrap().unwrap();
            upstream
                .inbound_owners
                .shutdown(
                    "incoming",
                    tokio::time::Instant::now() + Duration::from_secs(2),
                )
                .await
                .unwrap();
            fixture.finish().await;
            upstream
                .protocols
                .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
        }
    })
    .await
    .unwrap();
}

async fn start_components(proxy: &Proxy) {
    let config = proxy.engine().config();
    proxy
        .protocols
        .start_components(
            config.clone(),
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
}

async fn connect(
    proxy: &Proxy,
    config: &zero_config::RuntimeConfig,
    tag: &str,
    port: u16,
) -> Result<crate::transport::TcpRelayStream, zero_engine::EngineError> {
    // Resolve against the supplied immutable planning snapshot while using the
    // one actual adapter assembly and pool. No replacement is committed here.
    let engine = zero_engine::Engine::new(config.clone()).unwrap();
    let (resolved, _) = engine
        .resolve_route_decision(RouteDecision::Route(tag.into()))
        .unwrap();
    let ResolvedOutbound::Single(leaf) = resolved else {
        panic!("actual leaf")
    };
    let claimed = proxy.protocols.claim_outbound_leaf(config, leaf).unwrap();
    let operation = claimed
        .prepare_tcp_connect_for_route(
            OutboundAdapterContext::new(config).source_dir(),
            RouteMode::Flow,
        )
        .map_err(|failure| failure.error)?;
    let mut session = Session::new(
        0,
        Address::Ipv4([127, 0, 0, 1]),
        port,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    operation
        .execute(proxy.tcp_runtime_services().execution(), &session)
        .await
        .map_err(|failure| failure.error)?
        .into_relay_stream()
}
async fn round_trip(proxy: &Proxy, config: &zero_config::RuntimeConfig, tag: &str, port: u16) {
    let mut stream = connect(proxy, config, tag, port).await.unwrap();
    stream.write_all(b"ping").await.unwrap();
    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"ping");
    stream.shutdown().await.unwrap();
}

#[test]
fn cold_tcp_errors_stay_deferred_and_preserve_connect_and_relay_diagnostics() {
    for protocol in ["vless", "vmess", "trojan"] {
        let mut options = credentials(protocol);
        options["server"] = json!("127.0.0.1");
        options["port"] = json!(443);
        options["mux_concurrency"] = json!(8);
        let mut config = zero_config::RuntimeConfig::parse(
            &json!({"outbounds":[{"tag":"a","protocol":options}],
            "route":{"final":{"type":"route","outbound":"a"}}})
            .to_string(),
        )
        .unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        use zero_config::OutboundProtocolConfig::*;
        match &mut config.outbounds[0].protocol {
            Vless {
                mux_response_backlog_frames,
                ..
            }
            | Vmess {
                mux_response_backlog_frames,
                ..
            }
            | Trojan {
                mux_response_backlog_frames,
                ..
            } => *mux_response_backlog_frames = Some(0),
            _ => unreachable!(),
        }
        let leaf = zero_engine::ResolvedLeafOutbound::Proxy {
            identity: zero_engine::OutboundIdentity::from_config_index(0),
        };
        let claimed = proxy.protocols.claim_outbound_leaf(&config, leaf).unwrap();
        assert!(claimed.clone().into_claimed().has_udp_flow_capability());
        let connect = claimed
            .prepare_tcp_connect_for_route(None, RouteMode::Flow)
            .err()
            .expect("deferred invalid TCP config");
        assert_eq!(
            connect.stage,
            match protocol {
                "vless" => "connect_upstream_vless",
                "vmess" => "connect_upstream_vmess",
                _ => "connect_upstream_trojan",
            }
        );
        assert_eq!(
            connect.upstream_endpoint.as_deref(),
            Some(&("127.0.0.1".to_owned(), 443))
        );
        assert!(connect
            .error
            .to_string()
            .contains(&format!("invalid {protocol} tcp config:")));
        let relay = claimed
            .prepare_tcp_relay_hop(None)
            .err()
            .expect("deferred invalid relay config");
        assert!(relay
            .to_string()
            .contains(&format!("invalid {protocol} tcp relay config:")));
    }
}
