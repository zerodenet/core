use zero_config::RuntimeConfig;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::RouteDecision;

use super::route_trace_for_session;

#[tokio::test]
async fn domain_trace_rechecks_resolved_ip_rules() {
    let config = RuntimeConfig::parse(
        r#"{
            "outbounds": [
                { "tag": "proxy", "protocol": { "type": "direct" } }
            ],
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
                "final": { "type": "route", "outbound": "proxy" }
            }
        }"#,
    )
    .expect("parse routing config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let session = Session::new(
        1,
        Address::Domain("localhost".to_owned()),
        80,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );

    let trace = route_trace_for_session(&proxy.tcp_runtime_services(), &session).await;

    assert_eq!(trace.decision, RouteDecision::Direct);
    let matched = trace.matched_rule.expect("resolved IP rule matched");
    assert_eq!(matched.index, 0);
    assert_eq!(matched.condition, "ip: 127.0.0.0/8, ::1/128");
}

#[tokio::test]
async fn resolved_ip_bypass_overrides_an_already_matched_domain_rule() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "bypass": [{"type":"ip","values":["127.0.0.0/8","::1/128"]}],
                "rules": [{
                    "condition":{"type":"domain","values":["localhost"]},
                    "action":{"type":"reject"}
                }],
                "final":{"type":"reject"}
            }
        }"#,
    )
    .expect("parse bypass config");
    let proxy = crate::runtime::Proxy::new(config).expect("build proxy");
    let session = Session::new(
        1,
        Address::Domain("localhost".into()),
        80,
        Network::Tcp,
        ProtocolType::UNKNOWN,
    );
    let services = proxy.tcp_runtime_services();
    let before_resolution = services
        .engine()
        .route_trace_in_snapshot_with_inbound_and_resolved_ips(
            services.snapshot(),
            &session.target,
            None,
            None,
            &[],
        );
    assert_eq!(before_resolution.decision, RouteDecision::Reject);
    assert!(before_resolution.matched_rule.is_some());
    let trace = route_trace_for_session(&services, &session).await;
    assert_eq!(trace.decision, RouteDecision::Direct);
    assert!(trace.matched_rule.is_none());
}

/// A DNS transport with no socket or resolver access. A regression records an
/// attempt and returns an error immediately instead of touching the network.
#[derive(Debug, Default)]
struct UnavailableDns(std::sync::atomic::AtomicUsize);

impl zero_dns::DnsOutboundConnector for UnavailableDns {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async { Err(std::io::Error::other("test DNS is unavailable")) })
    }
}

#[tokio::test]
async fn matched_domain_bypass_never_queries_default_dns_even_when_it_is_unavailable() {
    use std::sync::{atomic::Ordering, Arc};
    for mode in [
        serde_json::json!({"type":"rule"}),
        serde_json::json!({"type":"global","outbound":"dns-out"}),
    ] {
        let config = RuntimeConfig::parse(&serde_json::json!({
            "mode":mode,
            "runtime":{"dns":{
                "servers":{
                    "bootstrap":{"type":"udp","host":"127.0.0.1"},
                    "default":{"type":"udp","host":"127.0.0.1","detour":"dns-out"}
                },
                "default_server":"default",
                "policy":{"node_server":"bootstrap"}
            }},
            "outbounds":[{"tag":"dns-out","protocol":{"type":"direct"}}],
            "route":{
                "bypass":[
                    {"type":"domain","values":["router.example"]},
                    {"type":"ip","values":["192.168.0.0/16"]}
                ],
                "rules":[{"condition":{"type":"ip","values":["0.0.0.0/0"]},"action":{"type":"reject"}}],
                "final":{"type":"reject"}
            }
        }).to_string()).unwrap();
        let proxy = crate::runtime::Proxy::new(config).unwrap();
        let services = proxy.tcp_runtime_services();
        let dns = Arc::new(UnavailableDns::default());
        services.resolver().set_outbound_connector(dns.clone());
        for network in [Network::Tcp, Network::Udp] {
            let session = Session::new(
                1,
                Address::Domain("router.example".into()),
                80,
                network,
                ProtocolType::UNKNOWN,
            );
            let trace = route_trace_for_session(&services, &session).await;
            assert_eq!(trace.decision, RouteDecision::Direct);
            assert_eq!(dns.0.load(Ordering::Relaxed), 0);
        }
    }
}
