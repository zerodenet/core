use std::sync::Arc;

use crate::adapters::direct::DirectAdapter;
use crate::protocol_registry::ProtocolRegistry;

#[cfg(feature = "http")]
#[test]
fn inbound_only_registration_has_no_outbound_executor() {
    let adapter = Arc::new(crate::adapters::http::HttpConnectAdapter);
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    use zero_traits::ProtocolMetadata;
    registry
        .inbounds
        .register(
            adapter.descriptor(),
            http::component::InboundFactory,
            crate::adapters::http::component_options,
        )
        .unwrap();
    registry.register_standard_protocol(adapter, None);
    assert!(registry.inbounds.has_factory("http"));
    assert_eq!(
        Arc::as_ptr(&registry.entries[0].support) as *const (),
        expected
    );
    assert!(registry.entries[0].outbound.is_none());
    assert!(!registry.entries[0].support.has_outbound());
}

#[test]
#[should_panic(expected = "protocol capability `direct` registered more than once")]
fn duplicate_protocol_registration_is_rejected_at_the_registry_boundary() {
    let mut registry = direct_registry();
    registry.register_standard_protocol(
        Arc::new(DirectAdapter::default()),
        Some(DirectAdapter::claim_outbound_leaf_impl),
    );
    registry.register_standard_protocol(
        Arc::new(DirectAdapter::default()),
        Some(DirectAdapter::claim_outbound_leaf_impl),
    );
}

#[test]
fn registered_behavior_views_retain_one_adapter_instance() {
    let adapter = Arc::new(DirectAdapter::default());
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = direct_registry();
    registry.register_standard_protocol(adapter, Some(DirectAdapter::claim_outbound_leaf_impl));

    let entry = registry
        .entries
        .first()
        .expect("registered adapter should create one entry");
    assert_eq!(Arc::as_ptr(&entry.support) as *const (), expected);
    assert!(entry.outbound.is_some());
    assert_eq!(
        Arc::strong_count(&registry.entries[0].support),
        2,
        "support and the real outbound projection share the same adapter"
    );
}

#[cfg(feature = "socks5")]
#[test]
fn standard_datagram_adapter_requires_no_upstream_handler_registration() {
    let adapter = Arc::new(crate::adapters::socks5::Socks5Adapter);
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    use zero_traits::ProtocolMetadata;
    registry
        .inbounds
        .register(
            adapter.descriptor(),
            socks5::component::listener::InboundFactory,
            crate::adapters::socks5::inbound::component_options,
        )
        .unwrap();
    registry.register_standard_protocol(
        adapter,
        Some(crate::adapters::socks5::Socks5Adapter::claim_outbound_leaf_impl),
    );
    let entry = registry.entries.first().expect("registered adapter entry");
    assert!(entry.outbound.is_some());
    assert_eq!(Arc::as_ptr(&entry.support) as *const (), expected);
}

fn direct_registry() -> ProtocolRegistry {
    use zero_traits::ProtocolMetadata;
    let mut registry = ProtocolRegistry::default();
    registry
        .inbounds
        .register(
            DirectAdapter::default().descriptor(),
            crate::adapters::direct::inbound::Factory,
            crate::adapters::direct::inbound::options,
        )
        .unwrap();
    registry
}
