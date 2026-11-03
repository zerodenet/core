use super::*;
use crate::runtime::Proxy;
use zero_config::RuntimeConfig;

#[tokio::test]
async fn proxy_and_inventory_share_one_component_host_and_one_configuration_owner() {
    let proxy =
        Proxy::new(RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap())
            .unwrap();
    let handle = proxy.clone();
    assert!(Arc::ptr_eq(&proxy.host, &proxy.protocols.runtime_host()));
    assert!(Arc::ptr_eq(&proxy.host, &handle.host));
    assert!(Arc::ptr_eq(proxy.host.apply(), handle.host.apply()));
    assert!(Arc::ptr_eq(
        proxy.host.apply_lock(),
        handle.host.apply_lock()
    ));
    let weak = proxy.protocols.downgrade();
    assert!(Arc::ptr_eq(
        &weak.upgrade().unwrap().runtime_host(),
        &proxy.host
    ));
    drop(handle);
    drop(proxy);
    assert!(
        weak.upgrade().is_none(),
        "weak dispatch cannot retain the host, factories or pools"
    );
}

#[tokio::test]
async fn component_host_debug_does_not_expose_a_pending_configuration() {
    let proxy = Proxy::new(RuntimeConfig::parse(r#"{"outbounds":[{"tag":"host-private-configuration","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap()).unwrap();
    let snapshot = proxy.engine().runtime_snapshot();
    let (sender, _receiver) = oneshot::channel();
    *proxy.host.reload_ack().lock().unwrap() = Some(PendingReloadAck {
        expected: snapshot.clone(),
        previous: snapshot,
        persist: false,
        compensating: false,
        sender,
    });
    let debug = format!("{:?}", proxy.protocols);
    assert!(!debug.contains("host-private-configuration"));
    assert!(!debug.contains("PendingReloadAck"));
}
