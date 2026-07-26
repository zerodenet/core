use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::RouteDecision;

use super::UdpIngressRuntime;

#[tokio::test]
async fn unmatched_domain_is_rechecked_against_resolved_ip_rules() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [
                    {
                        "condition": {
                            "type": "ip",
                            "values": ["127.0.0.0/8", "::1/128"]
                        },
                        "action": { "type": "direct" }
                    }
                ],
                "final": { "type": "reject" }
            }
        }"#,
    )
    .expect("parse routing config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let session = Session::new(
        1,
        Address::Domain("localhost".to_owned()),
        53,
        Network::Udp,
        ProtocolType::UNKNOWN,
    );

    assert_eq!(
        runtime.route_decision(&session).await,
        RouteDecision::Direct
    );
}
