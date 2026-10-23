//! Cold ownership tests create no socket, pool driver or protocol task. They
//! establish reference lifetimes, never physical I/O-close evidence.
use super::*;
use crate::runtime::{dns_outbound::ProxyDnsOutboundConnector, Proxy};
use zero_config::RuntimeConfig;

fn config(label: &str) -> RuntimeConfig {
    RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds":[{"tag":label,"protocol":{"type":"direct"}}],
            "route":{"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap()
}

#[test]
fn retained_dns_and_callback_cannot_keep_the_engine_config_or_host_source_alive() {
    let proxy = Proxy::new(config("first")).unwrap();
    let config = Arc::downgrade(&proxy.engine().config());
    let source = proxy.current_service_source();
    let callback = ProxyDnsOutboundConnector::new(source.clone());
    let resolver = proxy.resolver.clone(); // Keep the actual installed callback alive too.
    assert!(source.upgrade().unwrap().runtime_services().is_ok());
    drop(proxy);
    assert!(source.upgrade().is_none());
    assert!(
        config.upgrade().is_none(),
        "resolver callback must not retain Engine's current config"
    );
    drop(callback);
    assert!(config.upgrade().is_none());
    drop(resolver);
}

#[test]
fn source_lives_with_actual_proxy_clones_and_uses_the_current_snapshot_on_each_call() {
    let proxy = Proxy::new(config("old")).unwrap();
    let retained = proxy.clone();
    let source = proxy.current_service_source();
    drop(proxy);
    {
        let call = source
            .upgrade()
            .expect("the cloned host still owns the source");
        let services = call.runtime_services().unwrap();
        assert!(services.snapshot().plan().target_id("old").is_some());
    }
    retained
        .engine()
        .reload_runtime_config(config("new"))
        .unwrap();
    {
        let call = source
            .upgrade()
            .expect("the same host survives publication");
        let services = call.runtime_services().unwrap();
        assert!(services.snapshot().plan().target_id("new").is_some());
        assert!(services.snapshot().plan().target_id("old").is_none());
    }
    drop(retained);
    assert!(
        source.upgrade().is_none(),
        "no extra keeper or permanent source map exists"
    );
}

#[test]
fn a_short_source_borrow_cannot_resurrect_dns_inventory_or_datagram_owners() {
    let proxy = Proxy::new(config("short")).unwrap();
    let call = proxy.current_service_source().upgrade().unwrap();
    let resolver = Arc::downgrade(&proxy.resolver);
    let protocols = proxy.protocols.downgrade();
    let retired = Arc::downgrade(&proxy.retired_datagrams);
    drop(proxy);
    assert!(resolver.upgrade().is_none());
    assert!(protocols.upgrade().is_none());
    assert!(retired.upgrade().is_none());
    assert_eq!(
        call.runtime_services().err().unwrap().kind(),
        io::ErrorKind::NotConnected
    );
}

#[test]
fn source_itself_never_pins_the_historical_snapshot_of_a_finished_call() {
    let proxy = Proxy::new(config("before")).unwrap();
    let call = proxy.current_service_source().upgrade().unwrap();
    let snapshot = proxy.engine().runtime_snapshot();
    let history = Arc::downgrade(&snapshot);
    let old_config = Arc::downgrade(snapshot.config());
    drop(snapshot);
    proxy
        .engine()
        .reload_runtime_config(config("after"))
        .unwrap();
    // Proxy's original construction config and inventory cold plans are host
    // fields, so drop the actual host before testing this source in isolation.
    drop(proxy);
    assert!(history.upgrade().is_none());
    assert!(old_config.upgrade().is_none());
    assert!(call
        .engine()
        .runtime_snapshot()
        .plan()
        .target_id("after")
        .is_some());
}
