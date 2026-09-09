use super::{proxy_src, read_module, workspace_root};

#[test]
fn native_multiplexer_contracts_are_runtime_neutral_and_protocol_implemented() {
    let root = workspace_root();
    let core = read_module(&root.join("crates/core/src/inbound.rs"));
    for name in [
        "trait InboundStreamMultiplexer",
        "trait InboundDatagramMultiplexer",
    ] {
        assert!(core.contains(name), "core must own {name}");
    }
    for forbidden in ["quinn::", "tokio::", "hysteria2::", "mieru::", "u16"] {
        let contracts = super::read(&root.join("crates/core/src/inbound/multiplex.rs"));
        // Comments may explain why native streams do not use frame IDs.
        let code = contracts
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            !code.contains(forbidden),
            "native MUX contract must not contain {forbidden}"
        );
    }
    let protocol = read_module(&root.join("protocols/hysteria2/src/transport/inbound.rs"));
    assert!(protocol.contains("impl InboundStreamMultiplexer"));
    assert!(protocol.contains("impl InboundDatagramMultiplexer"));
    let adapter = read_module(&proxy_src().join("adapters/hysteria2.rs"));
    assert!(!adapter.contains("impl InboundStreamMultiplexer"));
    assert!(!adapter.contains("impl InboundDatagramMultiplexer"));
}

#[test]
fn native_multiplexer_runtime_owns_tasks_without_protocol_or_carrier_state() {
    let runtime = read_module(&proxy_src().join("runtime/inbound_operation/multiplex.rs"));
    assert!(runtime.contains("InboundDatagramMultiplexer"));
    assert!(runtime.contains("JoinSet"));
    assert!(runtime.contains("abort_all"));
    for forbidden in [
        "quinn::",
        "hysteria2::",
        "mieru::",
        "parse_udp",
        "SessionMetadata",
    ] {
        assert!(
            !runtime.contains(forbidden),
            "runtime must not interpret {forbidden}"
        );
    }
}

#[test]
fn mieru_classifies_routes_and_owns_its_wire_state() {
    let protocol = read_module(&workspace_root().join("protocols/mieru/src/inbound.rs"));
    assert!(protocol.contains("InboundStreamRoute for MieruInboundAcceptedSession"));
    let adapter = read_module(&proxy_src().join("adapters/mieru.rs"));
    for forbidden in [
        "MieruInboundAcceptedSession::",
        "MieruCipher",
        "SessionMetadata",
        "tokio::spawn",
        "JoinSet",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "adapter must not own {forbidden}"
        );
    }
    let runtime =
        read_module(&proxy_src().join("runtime/inbound_operation/context/stream_route.rs"));
    assert!(runtime.contains("InboundStreamRoute"));
    assert!(!runtime.contains("mieru::"));
}
