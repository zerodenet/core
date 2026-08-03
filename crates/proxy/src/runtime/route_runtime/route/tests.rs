use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::RouteDecision;

use super::super::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices};

#[tokio::test]
async fn existing_connection_keeps_old_snapshot_and_new_connection_captures_new_snapshot() {
    let proxy = crate::runtime::Proxy::new(config_with_final("direct")).expect("build proxy");
    let factory = InboundRouteRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        "test-inbound".to_owned(),
    );
    let old_connection = factory.for_connection(None);

    proxy
        .engine()
        .reload_runtime_config(config_with_final("reject"))
        .expect("reload config");
    let new_connection = factory.for_connection(None);
    let session = Session::new(
        1,
        Address::Domain("example.com".to_owned()),
        443,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );

    assert_eq!(
        old_connection.tcp_runtime.route_decision(&session).await,
        RouteDecision::Direct
    );
    assert_eq!(
        new_connection.tcp_runtime.route_decision(&session).await,
        RouteDecision::Reject
    );
}

fn config_with_final(action: &str) -> RuntimeConfig {
    RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": [],
            "outbounds": [],
            "route": { "rules": [], "final": { "type": action } }
        })
        .to_string(),
    )
    .expect("parse config")
}
