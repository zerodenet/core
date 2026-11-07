//! Real metadata owners/locks/counters, not an actual TUN native qualification.
use super::*;
use crate::runtime::{Proxy, TunInfo};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use zero_api::{QueryRequest, QueryService, TunStatusQuery};
use zero_engine::EngineHandle;
fn fixture() -> (ProxyHandle, Arc<AtomicU64>) {
    let proxy=Proxy::new(zero_config::RuntimeConfig::parse(r#"{
  "runtime":{"dns":{"servers":{"local":{"type":"system"}},"default_server":"local","answer":{"type":"fake_ip","cidr":"198.18.0.0/15","ttl_seconds":60}}},
  "route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
    proxy.egress_interface.replace_for(
        false,
        Some(zero_platform_tokio::EgressInterface::new("physical0", 7).unwrap()),
    );
    proxy
        .egress_interface
        .mark_unavailable_for(true, "same-雪 \" unavailable");
    proxy.egress_interface.record_ipv6_to_ipv4_fallback();
    let counter = Arc::new(AtomicU64::new(7));
    *proxy.tun_info.lock().unwrap() = Some(TunInfo {
        id: 1,
        name: "zero-雪".into(),
        addr: "10.66.0.1/24".into(),
        addresses: vec!["10.66.0.1/24".into(), "fd00::1/64".into()],
        mtu: 1500,
        tag: "same-tun".into(),
        auto_route: true,
        include_cidrs: vec!["10.0.0.0/8".parse().unwrap(), "fd00::/8".parse().unwrap()],
        exclude_cidrs: vec!["127.0.0.0/8".parse().unwrap()],
        dual_stack: true,
        strict_route: true,
        dns_hijack: true,
        dns_hijacked_queries: counter.clone(),
        healthy: false,
        last_error: Some("original-雪 failure".into()),
        egress_interface: Some("physical0".into()),
        egress_interface_v4: Some("physical0".into()),
        egress_interface_v6: None,
        route_exclusions: vec![],
        managed_config: None,
    });
    (
        ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy),
        counter,
    )
}
#[test]
fn same_tun_original_guard_and_scalar_capture_keep_exact_existing_wire_without_dto_copies() {
    let (handle, counter) = fixture();
    let request = QueryRequest::TunStatus(TunStatusQuery);
    let expected = serde_json::to_value(handle.query(request.clone()).unwrap()).unwrap();
    let view = handle.control_proxy_query(&request).unwrap();
    assert!(
        matches!(
            handle.proxy.tun_info.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ),
        "actual same metadata owner is borrowed"
    );
    let first = serde_json::to_value(&view).unwrap();
    assert_eq!(first, expected);
    assert_eq!(
        serde_json::to_value(view.body()).unwrap(),
        first["tun_status"]
    );
    counter.fetch_add(10, Ordering::Relaxed);
    assert_eq!(
        serde_json::to_value(&view).unwrap(),
        first,
        "same query captures the actual mutable counter once"
    );
    drop(view);
    assert!(handle.proxy.tun_info.try_lock().is_ok());
    let next = handle.control_proxy_query(&request).unwrap();
    assert_eq!(
        serde_json::to_value(next).unwrap()["tun_status"]["dns_hijacked_queries"],
        17
    );
}
#[test]
fn absent_tun_borrows_original_last_error_and_preserves_absent_tun_fakeip_default() {
    let (handle, _) = fixture();
    handle.proxy.tun_info.lock().unwrap().take();
    *handle.proxy.tun_last_error.lock().unwrap() = Some("last actual route failure-雪".into());
    assert!(handle.proxy.resolver.fake_ip_enabled());
    let request = QueryRequest::TunStatus(TunStatusQuery);
    let expected = serde_json::to_value(handle.query(request.clone()).unwrap()).unwrap();
    let view = handle.control_proxy_query(&request).unwrap();
    assert!(matches!(
        handle.proxy.tun_last_error.try_lock(),
        Err(std::sync::TryLockError::WouldBlock)
    ));
    let actual = serde_json::to_value(&view).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual["tun_status"]["fake_ip_enabled"], false);
    drop(view);
    assert!(handle.proxy.tun_last_error.try_lock().is_ok());
}
