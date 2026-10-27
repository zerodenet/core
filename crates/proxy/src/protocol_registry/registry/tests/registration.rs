use std::sync::Arc;

use crate::adapters::direct::DirectAdapter;
use crate::protocol_registry::ProtocolRegistry;

#[cfg(feature = "http")]
#[test]
fn inbound_only_registration_has_no_outbound_executor() {
    use zero_config::InboundProtocolConfig;

    let adapter = Arc::new(crate::adapters::http::HttpConnectAdapter);
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    registry.register_core_capability(adapter, None);

    let inbound = registry
        .find_inbound(&InboundProtocolConfig::HttpConnect)
        .expect("actual HTTP listener is registered");
    assert_eq!(Arc::as_ptr(&inbound) as *const (), expected);
    assert!(registry.entries[0].outbound.is_none());
    assert!(!registry.entries[0].support.has_outbound());
}

#[test]
#[should_panic(expected = "protocol capability `direct` registered more than once")]
fn duplicate_protocol_registration_is_rejected_at_the_registry_boundary() {
    let mut registry = ProtocolRegistry::default();
    registry.register_core_capability(
        Arc::new(DirectAdapter::default()),
        Some(DirectAdapter::claim_outbound_leaf_impl),
    );
    registry.register_core_capability(
        Arc::new(DirectAdapter::default()),
        Some(DirectAdapter::claim_outbound_leaf_impl),
    );
}

#[test]
fn registered_behavior_views_retain_one_adapter_instance() {
    let adapter = Arc::new(DirectAdapter::default());
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    registry.register_core_capability(adapter, Some(DirectAdapter::claim_outbound_leaf_impl));

    let entry = registry
        .entries
        .first()
        .expect("registered adapter should create one entry");
    let pointers = [
        Arc::as_ptr(&entry.support) as *const (),
        Arc::as_ptr(&entry.inbound) as *const (),
    ];

    assert!(pointers.into_iter().all(|pointer| pointer == expected));
}

#[cfg(feature = "hysteria2")]
#[test]
fn managed_handler_provider_shares_the_registered_adapter_instance() {
    let adapter = Arc::new(crate::adapters::hysteria2::Hysteria2Adapter::default());
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    registry.register_managed_capability(
        adapter,
        crate::adapters::hysteria2::Hysteria2Adapter::claim_outbound_leaf_impl,
    );

    let entry = registry
        .entries
        .first()
        .expect("registered adapter should create one entry");
    let provider = entry
        .managed_udp_handlers
        .as_ref()
        .expect("managed UDP handler provider view");

    assert_eq!(Arc::as_ptr(provider) as *const (), expected);
    assert_eq!(Arc::as_ptr(&entry.support) as *const (), expected);
}

#[cfg(feature = "socks5")]
#[test]
fn standard_datagram_adapter_requires_no_upstream_handler_registration() {
    let adapter = Arc::new(crate::adapters::socks5::Socks5Adapter);
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    registry.register_core_capability(
        adapter,
        Some(crate::adapters::socks5::Socks5Adapter::claim_outbound_leaf_impl),
    );
    let entry = registry.entries.first().expect("registered adapter entry");
    assert!(entry.outbound.is_some());
    assert_eq!(Arc::as_ptr(&entry.support) as *const (), expected);
}
