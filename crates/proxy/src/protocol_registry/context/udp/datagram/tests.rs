use super::*;
#[tokio::test]
async fn completion_observer_does_not_retain_the_actual_host_or_retirement_owner() {
    let proxy = crate::runtime::Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    let host = proxy.current_service_source();
    let services = UdpRuntimeServices::new(proxy.tcp_runtime_services());
    let retirement = std::sync::Arc::downgrade(&services.tcp.execution().retired_datagrams());
    let observer = services.association_observer();
    assert!(observer.host.ptr_eq(&host));
    drop(services);
    drop(proxy);
    assert!(host.upgrade().is_none());
    assert!(
        retirement.upgrade().is_none(),
        "actual completion observer contains no strong path back to RetiredDatagrams"
    );
}
