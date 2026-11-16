pub(super) mod fixtures;
use crate::Proxy;
use fixtures::*;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{timeout, Duration},
};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::{RouteDecision, RouteMode};

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
            let listener = crate::runtime::test_listener::start(&upstream, input).await;
            let address = listener.address;
            let shutdown = listener.stop;
            let inbound_worker = listener.worker;

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
            let running = outer.spawn();
            let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(outer.engine().clone()), outer.clone());
            handle.apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5)).await.unwrap();
            round_trip(&outer, &config, "a", fixture.echo.port()).await;
            round_trip(&outer, &config, "a", fixture.echo.port()).await;
            assert_eq!(
                fixture.connections(),
                1,
                "{protocol}: same actual MUX owner must reuse its carrier"
            );

            // The same configured owner changes through its real Host apply.
            // No old pool may bypass changed transport authentication.
            let mut incompatible = base.clone();
            if protocol == "trojan" {
                incompatible["outbounds"][0]["protocol"]["ws"]["path"] = json!("/rejected");
            } else {
                incompatible["outbounds"][0]["protocol"]["tls"]["ca_cert_path"] =
                    json!("untrusted.pem");
            }
            let incompatible = config_at(incompatible, material.path());
            handle.apply_runtime_config_and_wait(incompatible.clone(), Duration::from_secs(5)).await.unwrap();
            assert!(
                connect(&outer, &incompatible, "a", fixture.echo.port())
                    .await
                    .is_err(),
                "{protocol}: cached MUX bypassed changed transport authentication"
            );
            assert_eq!(fixture.connections(), 2);
            handle.apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5)).await.unwrap();
            round_trip(&outer, &config, "b", fixture.echo.port()).await;
            round_trip(&outer, &config, "a", fixture.echo.port()).await;
            assert_eq!(
                fixture.connections(),
                4,
                "{protocol}: distinct owners and the restored actual generation retain independent pools"
            );
            outer
                .protocols
                .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
                .await
                .unwrap();
            running.shutdown().await.unwrap();
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
    _config: &zero_config::RuntimeConfig,
    tag: &str,
    port: u16,
) -> Result<crate::transport::TcpRelayStream, zero_engine::EngineError> {
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route(tag.into()))
        .unwrap();
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
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        RouteMode::Flow,
        crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic,
    )
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
fn invalid_tcp_options_fail_the_actual_factory_without_reconstructing_a_private_executor() {
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
        assert!(crate::register::protocol_registry()
            .claim_outbound_leaf(&config, leaf)
            .is_err());
        let error = match Proxy::new(config) {
            Err(source) => source,
            Ok(_) => panic!("invalid native factory accepted"),
        };
        assert!(
            error
                .to_string()
                .contains("MUX response backlog frames must be within 1..=4096"),
            "{protocol}: {error}"
        );
    }
}
