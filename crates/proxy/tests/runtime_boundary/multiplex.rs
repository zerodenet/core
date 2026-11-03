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
fn standard_hysteria2_requests_use_sealed_quic_and_actual_caller_scope() {
    assert!(!proxy_src()
        .join("runtime/inbound_operation/multiplex.rs")
        .exists());
    let root = workspace_root();
    let source = super::read(&root.join("protocols/hysteria2/src/component/connection.rs"));
    for required in [
        "OwnedConnection",
        "impl RequestMultiplexer",
        "self.auth.as_ref()",
        "facts.request",
        "read_tcp_connect_header",
    ] {
        assert!(
            source.contains(required),
            "standard connection must use {required}"
        );
    }
    let web = super::read(&root.join("protocols/hysteria2/src/component/web.rs"));
    for required in [
        "caller.spawn",
        "owned_h3::Connection",
        "resources",
        "serve_scoped",
    ] {
        assert!(web.contains(required));
    }
    for forbidden in [
        "tokio::spawn",
        "JoinSet",
        "quinn::Connection",
        "Session::new",
    ] {
        assert!(!source.contains(forbidden));
        assert!(!web.contains(forbidden));
    }
    let packets = super::read(&root.join("protocols/hysteria2/src/component/datagram.rs"));
    for required in [
        "impl DatagramReply",
        "client_session_id",
        "decode_dispatch_parts",
        "build_udp_fragments",
        "wire_bytes",
    ] {
        assert!(packets.contains(required));
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
    let runtime = read_module(&proxy_src().join("runtime/route_runtime/route/source.rs"));
    assert!(runtime.contains("PendingInbound"));
    assert!(!runtime.contains("mieru::"));
}

#[test]
fn route_multiplexer_keeps_handshake_tasks_in_runtime_and_wire_dispatch_in_protocol() {
    let runtime = read_module(&proxy_src().join("runtime/route_runtime/route/source.rs"));
    for required in [
        "RequestMultiplexer",
        "TaskClass",
        "close(",
        "deadline",
        "register_principal_cancellation",
    ] {
        assert!(
            runtime.contains(required),
            "missing lifecycle requirement: {required}"
        );
    }
    for forbidden in ["mieru::", "MieruCipher", "parse_segment"] {
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
    assert!(!proxy_src()
        .join("runtime/inbound_operation/peer_route.rs")
        .exists());
    let runtime =
        read_module(&workspace_root().join("crates/platform/tokio/src/contract/inbound/udp.rs"));
    assert!(runtime.contains("DatagramPeer"));
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
