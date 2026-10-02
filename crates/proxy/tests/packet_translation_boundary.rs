//! Keep Packet translation separate from routing and WireGuard cryptography.

use std::{fs, path::PathBuf};

#[test]
fn packet_address_translation_stays_in_the_stack_and_uses_neutral_runtime_io() {
    let proxy = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let stack = proxy.parent().unwrap().join("stack/src");
    let adapter =
        fs::read_to_string(proxy.join("src/runtime/raw_ip/device/translation.rs")).unwrap();
    let compact: String = adapter
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    assert!(compact.contains("self.returns.translate("));
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
    let packet_operation = fs::read_to_string(proxy.join("src/runtime/raw_ip/packet.rs")).unwrap();
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
    let graph = fs::read_to_string(proxy.join("src/runtime/network_graph.rs")).unwrap();
    assert!(
        !graph.contains("Icmp"),
        "ICMP remains Packet, not a new L4 graph plane"
    );
    let protocol = fs::read_to_string(proxy.join("../../protocols/wireguard/Cargo.toml")).unwrap();
    assert!(!protocol.contains("zero-stack"));
    assert!(!protocol.contains("zero-proxy"));
}
