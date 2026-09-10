use super::{proxy_src, read_module, workspace_root};

#[test]
fn native_multiplexer_contracts_are_runtime_neutral_and_protocol_implemented() {
    let root = workspace_root();
    let core = read_module(&root.join("crates/core/src/inbound.rs"));
    for name in [
        "trait InboundStreamMultiplexer",
        "trait InboundDatagramMultiplexer",
        "trait InboundRouteMultiplexer",
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

#[test]
fn route_multiplexer_keeps_handshake_tasks_in_runtime_and_wire_dispatch_in_protocol() {
    let runtime = read_module(&proxy_src().join("runtime/inbound_operation/context/multiplex.rs"));
    for required in [
        "InboundRouteMultiplexer",
        "JoinSet",
        "abort_all",
        "timeout",
        "register_principal_cancellation",
    ] {
        assert!(
            runtime.contains(required),
            "missing lifecycle requirement: {required}"
        );
    }
    for forbidden in ["mieru::", "MieruCipher", "session_id", "parse_segment"] {
        assert!(!runtime.contains(forbidden));
    }
    let protocol = read_module(&workspace_root().join("protocols/mieru/src/inbound/multiplex.rs"));
    assert!(protocol.contains("impl InboundRouteMultiplexer"));
    assert!(protocol.contains("parse_segment"));
    let adapter = read_module(&proxy_src().join("adapters/mieru.rs"));
    for forbidden in [
        "impl InboundRouteMultiplexer",
        "tokio::spawn",
        "SessionMetadata",
    ] {
        assert!(!adapter.contains(forbidden));
    }
}

#[test]
fn datagram_peer_runtime_does_not_own_mieru_reliability_or_pool_state() {
    let runtime = read_module(&proxy_src().join("runtime/inbound_operation/peer_route.rs"));
    assert!(runtime.contains("JoinSet"));
    assert!(runtime.contains("tasks.shutdown"));
    for forbidden in [
        "mieru::",
        "MieruCipher",
        "SessionMetadata",
        "unack_sequence",
        "PoolKey",
    ] {
        assert!(!runtime.contains(forbidden));
    }
    let adapter = read_module(&proxy_src().join("adapters/mieru.rs"));
    for forbidden in [
        "MieruCipher",
        "ReliableSession",
        "SessionMetadata",
        "tokio::spawn",
    ] {
        assert!(!adapter.contains(forbidden));
    }
}
