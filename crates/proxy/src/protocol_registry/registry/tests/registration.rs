use std::sync::Arc;

use crate::adapters::direct::DirectAdapter;
use crate::protocol_registry::ProtocolRegistry;

#[test]
#[should_panic(expected = "protocol capability `direct` registered more than once")]
fn duplicate_protocol_registration_is_rejected_at_the_registry_boundary() {
    let mut registry = ProtocolRegistry::default();
    registry.register_capability(
        Arc::new(DirectAdapter),
        DirectAdapter::claim_outbound_leaf_impl,
    );
    registry.register_capability(
        Arc::new(DirectAdapter),
        DirectAdapter::claim_outbound_leaf_impl,
    );
}

#[test]
fn focused_capability_views_share_one_adapter_instance() {
    let adapter = Arc::new(DirectAdapter);
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    registry.register_capability(adapter, DirectAdapter::claim_outbound_leaf_impl);

    let entry = registry
        .entries
        .first()
        .expect("registered adapter should create one entry");
    let pointers = [
        Arc::as_ptr(&entry.support) as *const (),
        Arc::as_ptr(&entry.inbound) as *const (),
        Arc::as_ptr(&entry.tcp) as *const (),
        Arc::as_ptr(entry.udp.as_ref().expect("UDP view")) as *const (),
        Arc::as_ptr(entry.packet_path.as_ref().expect("packet-path view")) as *const (),
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
    assert_eq!(
        Arc::as_ptr(entry.udp.as_ref().expect("UDP view")) as *const (),
        expected
    );
}

#[cfg(feature = "socks5")]
#[test]
fn upstream_handler_provider_shares_the_registered_adapter_instance() {
    let adapter = Arc::new(crate::adapters::socks5::Socks5Adapter);
    let expected = Arc::as_ptr(&adapter) as *const ();
    let mut registry = ProtocolRegistry::default();
    registry.register_upstream_capability(
        adapter,
        crate::adapters::socks5::Socks5Adapter::claim_outbound_leaf_impl,
    );

    let entry = registry.entries.first().expect("registered adapter entry");
    let provider = entry
        .upstream_udp_handler
        .as_ref()
        .expect("upstream UDP handler provider view");
    assert_eq!(Arc::as_ptr(provider) as *const (), expected);
    assert_eq!(
        Arc::as_ptr(entry.udp.as_ref().expect("UDP view")) as *const (),
        expected
    );
}
