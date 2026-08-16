use zero_api::{QueryRequest, QueryResponse, QueryService, TunStatusQuery};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;

use crate::runtime::{Proxy, TunInfo};

use super::super::ProxyHandle;

#[test]
fn running_tun_status_exposes_route_health_and_error() {
    let config = RuntimeConfig::parse(
        r#"{
            "route":{"rules":[],"final":{"type":"direct"}}
        }"#,
    )
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");
    *proxy.tun_info.lock().unwrap() = Some(TunInfo {
        id: 1,
        name: "zero-test".to_owned(),
        addr: "10.66.0.1/24".to_owned(),
        addresses: vec!["10.66.0.1/24".to_owned()],
        mtu: 1500,
        tag: "tun-in".to_owned(),
        auto_route: true,
        dual_stack: false,
        strict_route: true,
        dns_hijack: true,
        healthy: false,
        last_error: Some("route reconciliation failed".to_owned()),
        egress_interface: Some("physical0".to_owned()),
        egress_interface_v4: Some("physical0".to_owned()),
        egress_interface_v6: None,
        route_exclusions: Vec::new(),
        managed_config: None,
    });
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy);

    let response = handle
        .query(QueryRequest::TunStatus(TunStatusQuery))
        .expect("query TUN status");
    let QueryResponse::TunStatus(status) = response else {
        panic!("expected TUN status response");
    };
    assert!(status.running);
    assert!(!status.healthy);
    assert_eq!(
        status.last_error.as_deref(),
        Some("route reconciliation failed")
    );
    assert_eq!(status.egress_interface_v4.as_deref(), Some("physical0"));
}
