//! Keep Packet translation separate from routing and WireGuard cryptography.

use std::{fs, path::PathBuf};

#[test]
fn packet_address_translation_stays_in_the_stack_and_uses_neutral_runtime_io() {
    let proxy = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let stack = proxy.parent().unwrap().join("stack/src");
    let adapter =
        fs::read_to_string(proxy.join("src/runtime/raw_ip/device/returns/translated.rs")).unwrap();
    let compact: String = adapter
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    assert!(compact.contains("owner.response_batch("));
    for forbidden in [
        "wireguard::",
        "gotatun::",
        "parse_icmp_echo",
        "message[",
        "checksum(",
        "translated[",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "neutral Packet I/O must not own {forbidden}"
        );
    }
    let packet_operation =
        fs::read_to_string(proxy.join("src/runtime/raw_ip/device/standard_packet.rs")).unwrap();
    assert!(
        !packet_operation.contains("packet["),
        "IP field offsets belong to the pure stack parser"
    );
    let state = fs::read_to_string(stack.join("echo_translation.rs")).unwrap();
    assert!(state.contains("restore_echo_response"));
    for forbidden in [
        "wireguard::",
        "gotatun::",
        "zero_proxy",
        "zero_router",
        "tokio::",
        "mpsc::",
    ] {
        assert!(
            !state.contains(forbidden),
            "Packet correlation must not depend on {forbidden}"
        );
    }
    let route = fs::read_to_string(proxy.join("src/inventory/packet.rs")).unwrap();
    assert!(route.contains("plan.has_packet_provider(slot.role_id())"));
    assert!(route.contains("plan.has_device_carrier_provider(slot.role_id())"));
    for retired in [
        "src/runtime/network_graph.rs",
        "src/runtime/raw_ip/packet.rs",
        "src/adapters/wireguard/packet.rs",
        "src/runtime/packet_route/host/policy.rs",
    ] {
        assert!(
            !proxy.join(retired).exists(),
            "retired private Packet path: {retired}"
        );
    }
    let source =
        fs::read_to_string(proxy.join("src/runtime/inbound_operation/raw_ip/route.rs")).unwrap();
    assert!(source.contains("packet_roles"));
    assert!(!source.contains("operation.forward"));
    let claim = fs::read_to_string(proxy.join("src/protocol_registry/capability.rs")).unwrap();
    assert!(!claim.contains("PacketRouteOperations"));
    let protocol = fs::read_to_string(proxy.join("../../protocols/wireguard/Cargo.toml")).unwrap();
    assert!(!protocol.contains("zero-stack"));
    assert!(!protocol.contains("zero-proxy"));
}
