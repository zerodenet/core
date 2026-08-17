//! Architectural boundary tests for the proxy data plane.
//!
//! These tests deliberately lock responsibilities and dependency direction,
//! not a particular module layout. Protocol adapters may be folded or split
//! without changing these assertions.

use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("proxy crate must live under crates/")
        .to_path_buf()
}

fn proxy_src() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    collect_rust_sources(root, &mut sources);
    sources
}

fn collect_rust_sources(root: &Path, sources: &mut Vec<PathBuf>) {
    for entry in
        fs::read_dir(root).unwrap_or_else(|error| panic!("read {}: {error}", root.display()))
    {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            collect_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}

fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .replace("\r\n", "\n")
}

fn read_module(path: &Path) -> String {
    let mut combined = String::new();
    if path.is_file() {
        combined.push_str(&read(path));
        combined.push('\n');
    }
    let module_dir = if path.extension().is_some_and(|extension| extension == "rs") {
        path.with_extension("")
    } else {
        path.to_path_buf()
    };
    if module_dir.is_dir() {
        for source in rust_sources(&module_dir) {
            combined.push_str(&read(&source));
            combined.push('\n');
        }
    }
    combined
}

fn adapter_feature_names() -> Vec<String> {
    let adapters = read(&proxy_src().join("adapters/mod.rs"));
    let mut features = adapters
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("#[cfg(feature = \"")
                .and_then(|feature| feature.strip_suffix("\")]"))
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    features.sort_unstable();
    features.dedup();
    features
}

fn adapter_feature_tokens() -> Vec<String> {
    adapter_feature_names()
        .into_iter()
        .map(|feature| format!("feature = \"{feature}\""))
        .collect()
}

fn external_protocol_feature_names() -> Vec<String> {
    adapter_feature_names()
        .into_iter()
        .filter(|feature| !matches!(feature.as_str(), "http" | "mixed"))
        .collect()
}

fn protocol_crate_feature_names() -> Vec<String> {
    let mut features = fs::read_dir(workspace_root().join("protocols"))
        .expect("read protocol crates")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if !path.is_dir() {
                return None;
            }
            path.file_name()?.to_str().map(str::to_owned)
        })
        .collect::<Vec<_>>();
    features.push("mixed".to_owned());
    features.sort_unstable();
    features.dedup();
    features
}

fn assert_sources_exclude(root: &Path, forbidden: &[&str]) {
    for path in rust_sources(root) {
        let source = read(&path);
        for token in forbidden {
            assert!(
                !source.contains(token),
                "{} must not contain boundary token `{token}`",
                path.display()
            );
        }
    }
}

fn assert_non_test_sources_exclude(root: &Path, forbidden: &[&str]) {
    for path in rust_sources(root) {
        if path
            .components()
            .any(|component| component.as_os_str() == std::ffi::OsStr::new("tests"))
        {
            continue;
        }
        let source = read(&path);
        for token in forbidden {
            assert!(
                !source.contains(token),
                "{} must not contain boundary token `{token}`",
                path.display()
            );
        }
    }
}

#[test]
fn adapters_prepare_protocol_parts_but_do_not_own_runtime_execution() {
    assert_sources_exclude(
        &proxy_src().join("adapters"),
        &[
            "crate::runtime::Proxy",
            "use crate::runtime::Proxy",
            "tokio::spawn",
            "JoinSet",
            "listener_loop",
            "run_tcp_listener_loop",
            "run_logged_tcp_listener_loop",
            "run_logged_quic_listener_loop",
            "serve_inbound",
            "ProtocolTransportBridgeAdapter",
            "mod transport_bridge;",
        ],
    );
}

#[test]
fn adapters_do_not_recover_engine_leaves_or_construct_runtime_facts() {
    assert_sources_exclude(
        &proxy_src().join("adapters"),
        &[
            "ResolvedLeafOutbound",
            "OutboundLeafRuntime",
            "config.outbounds",
            ".outbounds.get(",
            "find_outbound_leaf(",
            "resolve_outbound(",
            "struct VlessOutboundProjection",
            "struct VmessOutboundProjection",
            "struct TrojanOutboundProjection",
        ],
    );
}

#[test]
fn transport_does_not_own_proxy_execution_contracts() {
    assert_sources_exclude(
        &workspace_root().join("crates/transport/src"),
        &[
            "PreparedTransportLeaf",
            "PreparedTcpConnectOperation",
            "PreparedTcpRelayOperation",
            "PreparedUdpFlowOperation",
            "PreparedUdpRelayOperation",
            "ManagedDatagramStartPlan",
            "ManagedStreamPacketBridgePlan",
            "ProtocolManagedDatagramUdpResumeConnectionOps",
            "ProtocolManagedDatagramSocketUdpResumeConnectionOps",
            "ProtocolManagedTupleUdpFlowResumeConnectionOps",
            "ProtocolManagedPacketUdpFlowResumeConnectionOps",
            "ManagedTupleUdpConnectionOps",
            "ManagedPacketUdpConnectionOps",
            "ManagedDatagramConnectionOps",
            "TCP_CONNECT_STAGE",
            "UDP_DIRECT_STAGE",
            "MISMATCH_MESSAGE",
        ],
    );
    assert!(!workspace_root()
        .join("crates/transport/src/inbound_quic.rs")
        .exists());
    assert!(!workspace_root()
        .join("crates/transport/src/transport_plan.rs")
        .exists());
    assert!(!workspace_root()
        .join("crates/transport/src/mux_stack.rs")
        .exists());
    assert!(!workspace_root()
        .join("crates/transport/src/client_hello.rs")
        .exists());

    let quic_runtime = read(&proxy_src().join("runtime/inbound_operation/quic.rs"));
    assert!(quic_runtime.contains("trait AuthenticatedQuicInboundProfile"));
    assert!(quic_runtime.contains("trait AuthenticatedQuicInboundConnection"));

    let hysteria2_adapter = read(&proxy_src().join("adapters/hysteria2/inbound.rs"));
    assert!(hysteria2_adapter.contains("impl AuthenticatedQuicInboundProfile"));
    assert!(hysteria2_adapter.contains("impl AuthenticatedQuicInboundConnection"));
}

#[test]
fn engine_diagnostics_do_not_execute_network_io() {
    let engine = read(&workspace_root().join("crates/engine/src/runtime/diagnostics.rs"));
    for forbidden in [
        "TcpStream::connect_timeout",
        ".to_socket_addrs()",
        "config.outbounds",
        ".protocol.endpoint()",
    ] {
        assert!(
            !engine.contains(forbidden),
            "engine diagnostics must not execute network or project protocol endpoints via `{forbidden}`"
        );
    }

    let proxy = read(&proxy_src().join("runtime/handle/command/diagnostics.rs"));
    assert!(proxy.contains("execute_diagnostics_probe_target"));
    assert!(proxy.contains("execute_diagnostics_dns_lookup"));
    assert!(proxy.contains("execute_diagnostics_trace_route"));
    assert!(proxy.contains(".claim_outbound_leaf("));
    assert!(!proxy.contains("config.outbounds"));
}

#[test]
fn tun_anti_loop_does_not_project_or_resolve_proxy_endpoints() {
    let tun = read_module(&proxy_src().join("inbound/tun.rs"));
    for forbidden in [
        "resolve_tun_route_exclusions",
        ".protocol.endpoint()",
        ".config().outbounds",
        "config.outbounds",
    ] {
        assert!(
            !tun.contains(forbidden),
            "TUN anti-loop must not depend on proxy endpoint enumeration via `{forbidden}`"
        );
    }
    assert!(tun.contains("configured_dns_endpoint_addresses"));
    assert!(tun.contains("install_with_egress"));
}

#[test]
fn outbound_companion_sockets_use_the_shared_egress_factory() {
    let workspace = workspace_root();
    let sources = [
        workspace.join("crates/transport/src/udp_packet_path.rs"),
        workspace.join("protocols/hysteria2/src/transport/connection.rs"),
        workspace.join("protocols/shadowsocks/src/transport/udp_socket.rs"),
    ];
    for path in sources {
        let source = read(&path);
        assert!(
            source.contains("OutboundDatagramSocketFactory")
                || source.contains("socket_factory.bind_std"),
            "{} must receive the runtime-owned datagram socket factory",
            path.display()
        );
        for forbidden in ["std::net::UdpSocket::bind", "tokio::net::UdpSocket::bind"] {
            assert!(
                !source.contains(forbidden),
                "{} must not create an unbound outbound socket via `{forbidden}`",
                path.display()
            );
        }
    }

    let vless = read(&workspace.join("protocols/vless/src/transport/outbound/direct.rs"));
    assert!(vless.contains("OutboundDatagramSocketFactory"));
    assert!(vless.contains("TokioSocket::connect_addr_on"));
    assert!(!vless.contains("TokioSocket::connect_addr(peer)"));
}

#[test]
fn inbound_route_contracts_implementations_and_execution_have_distinct_owners() {
    let core = read(&workspace_root().join("crates/core/src/inbound.rs"));
    for contract in [
        "pub trait InboundFallbackCapture",
        "pub trait InboundFallbackReplay",
        "pub enum InboundRouteAccept",
    ] {
        assert!(core.contains(contract));
    }

    let vless_inbound = read(&workspace_root().join("protocols/vless/src/inbound.rs"));
    let vless_mux = read(&workspace_root().join("protocols/vless/src/mux.rs"));
    assert!(vless_inbound.contains("impl<S> zero_core::InboundFallbackReplay"));
    assert!(vless_mux.contains("impl InboundMuxTcpRelay"));
    assert!(vless_mux.contains("impl InboundMuxUdpRelay"));

    let fallback_runtime = read(&proxy_src().join("runtime/inbound_fallback.rs"));
    let route_runtime = read_module(&proxy_src().join("runtime/inbound_route.rs"));
    assert!(fallback_runtime.contains("relay_recorded_fallback_replay"));
    assert!(route_runtime.contains("InboundRouteAccept"));

    assert_sources_exclude(
        &workspace_root().join("crates/transport/src"),
        &[
            "InboundFallbackReplay",
            "InboundRouteAccept",
            "InboundMuxTcpRelay",
            "InboundMuxUdpRelay",
        ],
    );
    assert_sources_exclude(
        &proxy_src().join("adapters"),
        &[
            "impl InboundFallbackReplay",
            "impl InboundMuxTcpRelay",
            "impl InboundMuxUdpRelay",
        ],
    );
}

#[test]
fn generic_proxy_sources_gate_capabilities_not_protocol_lists() {
    let protocol_feature_tokens = adapter_feature_tokens();
    let protocol_feature_refs = protocol_feature_tokens
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    for root in [
        proxy_src().join("runtime"),
        proxy_src().join("protocol_registry"),
        proxy_src().join("transport"),
    ] {
        assert_non_test_sources_exclude(&root, &protocol_feature_refs);
    }
    let logging = read(&proxy_src().join("logging.rs"));
    for token in &protocol_feature_tokens {
        assert!(!logging.contains(token.as_str()));
    }

    let main = read(&workspace_root().join("src/main.rs"));
    for token in &protocol_feature_tokens {
        assert!(!main.contains(token.as_str()));
    }
    assert!(main.contains("zero_proxy::compiled_protocol_features()"));
}

#[test]
fn concrete_protocol_feature_gates_exist_only_in_adapters_and_registration() {
    let protocol_feature_tokens = adapter_feature_tokens();
    let proxy = proxy_src();
    for path in rust_sources(&proxy) {
        if path
            .components()
            .any(|component| component.as_os_str() == std::ffi::OsStr::new("tests"))
        {
            continue;
        }
        let source = read(&path);
        if !protocol_feature_tokens
            .iter()
            .any(|token| source.contains(token.as_str()))
        {
            continue;
        }
        let relative = path.strip_prefix(&proxy).expect("proxy-relative path");
        assert!(
            relative == Path::new("register.rs") || relative.starts_with("adapters"),
            "{} must gate generic code by capabilities instead of concrete protocols",
            path.display()
        );
    }
}

#[test]
fn protocol_crates_adapters_manifests_and_registration_share_one_feature_inventory() {
    let adapter_features = adapter_feature_names();
    assert_eq!(adapter_features, protocol_crate_feature_names());

    let adapters = read(&proxy_src().join("adapters/mod.rs"));
    let manifest = read(&workspace_root().join("crates/proxy/Cargo.toml"));
    let root_manifest = read(&workspace_root().join("Cargo.toml"));
    let full_start = root_manifest
        .find("full = [")
        .expect("root manifest full feature");
    let full_feature = &root_manifest[full_start..];
    let full_feature = &full_feature[..full_feature
        .find(']')
        .expect("root manifest full feature must close")];
    let register = read(&proxy_src().join("register.rs"));
    for feature in adapter_features {
        let gate = format!("feature = \"{feature}\"");
        assert_eq!(
            adapters.matches(&gate).count(),
            1,
            "adapters/mod.rs must declare `{feature}` exactly once"
        );
        assert_eq!(
            register.matches(&gate).count(),
            1,
            "register.rs must register `{feature}` exactly once"
        );
        assert!(
            manifest
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{feature} ="))),
            "proxy manifest must declare adapter feature `{feature}`"
        );
        let root_forward = root_manifest
            .lines()
            .find(|line| line.trim_start().starts_with(&format!("{feature} =")))
            .unwrap_or_else(|| panic!("root manifest must forward protocol feature `{feature}`"));
        assert!(
            root_forward.contains(&format!("zero-proxy/{feature}")),
            "root feature `{feature}` must forward to zero-proxy"
        );
        assert!(
            full_feature.contains(&format!("\"{feature}\"")),
            "root `full` feature must include protocol `{feature}`"
        );
        assert!(
            register.contains(&format!("feature = \"{feature}\"")),
            "register.rs must own the compiled entry for `{feature}`"
        );
    }
}

#[test]
fn registration_uses_protocol_features_only_for_individual_entries() {
    let register = read(&proxy_src().join("register.rs"));
    let protocol_feature_tokens = adapter_feature_tokens();
    let mut remaining = register.as_str();
    while let Some(start) = remaining.find("#[cfg(any(") {
        let block = &remaining[start..];
        let end = block.find("))]").expect("cfg(any) block must close");
        let block = &block[..end];
        for token in &protocol_feature_tokens {
            assert!(
                !block.contains(token.as_str()),
                "registration capability aggregation must not enumerate `{token}`"
            );
        }
        remaining = &remaining[start + end + 3..];
    }
}

#[test]
fn production_protocol_registry_is_assembled_only_in_register() {
    let proxy = proxy_src();
    for path in rust_sources(&proxy) {
        let relative = path.strip_prefix(&proxy).expect("proxy-relative path");
        if relative == Path::new("register.rs")
            || relative == Path::new("protocol_registry/registry/build.rs")
            || relative
                .components()
                .any(|component| component.as_os_str() == "tests")
            || relative.file_name().is_some_and(|name| name == "tests.rs")
        {
            continue;
        }

        let source = read(&path);
        for forbidden in [
            "ProtocolRegistry::default()",
            ".register_core_capability(",
            ".register_capability(",
            ".register_upstream_capability(",
            ".register_managed_capability(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} must not assemble protocol capabilities outside register.rs",
                path.display()
            );
        }
    }
}

#[test]
fn engine_resolved_proxy_leaf_carries_only_opaque_identity() {
    let resolve = read(&workspace_root().join("crates/engine/src/plan/resolve.rs"));
    let plan = read(&workspace_root().join("crates/engine/src/plan/mod.rs"));
    assert!(resolve.contains("Proxy { identity: OutboundIdentity }"));
    assert!(!resolve.contains("pub fn protocol_name(&self)"));
    assert!(!resolve.contains("pub fn proxy_endpoint(&self)"));
    assert!(!resolve.contains("outbound_index: usize,"));
    assert!(!resolve.contains("protocol: &'static str,"));
    assert!(!resolve.contains("endpoint: Option<(&'a str, u16)>,"));
    assert!(!plan.contains("protocol: &'static str,"));
    assert!(!plan.contains("endpoint: Option<OutboundEndpoint>,"));
}

#[test]
fn protocol_identity_is_open_and_engine_does_not_enumerate_protocols() {
    let session = read(&workspace_root().join("crates/core/src/session.rs"));
    assert!(session.contains("pub struct ProtocolType(&'static str);"));
    assert!(session.contains("pub const fn new(name: &'static str) -> Self"));
    assert!(session.contains("pub const fn as_str(self) -> &'static str"));
    assert!(!session.contains("pub enum ProtocolType"));
    let session_lower = session.to_ascii_lowercase();
    for protocol in adapter_feature_names() {
        assert!(!session_lower.contains(protocol.as_str()));
    }
    assert_sources_exclude(
        &workspace_root().join("crates/engine/src"),
        &["ProtocolType::"],
    );
}

#[test]
fn protocol_metadata_is_owned_by_registered_capabilities_not_a_secondary_catalog() {
    assert!(!proxy_src().join("protocol_catalog.rs").exists());

    let direct = read(&proxy_src().join("adapters/direct.rs"));
    let mixed = read(&proxy_src().join("adapters/mixed.rs"));
    let registry_metadata = read(&proxy_src().join("protocol_registry/registry/metadata.rs"));

    assert!(direct.contains("impl ProtocolMetadata for DirectAdapter"));
    assert!(mixed.contains("impl ProtocolMetadata for MixedAdapter"));
    assert!(registry_metadata.contains("fn block_descriptor()"));
    assert!(!registry_metadata.contains("match protocol"));
}

#[test]
fn protocol_session_identities_are_assigned_by_protocol_crates_not_proxy_runtime() {
    for path in rust_sources(&proxy_src()) {
        if path
            .components()
            .any(|component| component.as_os_str() == std::ffi::OsStr::new("tests"))
            || path
                .file_name()
                .is_some_and(|name| name == std::ffi::OsStr::new("tests.rs"))
        {
            continue;
        }
        assert!(
            !read(&path).contains("ProtocolType::new"),
            "{} must preserve protocol-owned session identity",
            path.display()
        );
    }
}

#[test]
fn runtime_operations_are_protocol_neutral() {
    let operation_files = [
        "runtime/inbound_operation.rs",
        "runtime/tcp_dispatch/operation.rs",
        "runtime/udp_dispatch/operation.rs",
        "runtime/udp_dispatch/packet_path_operation.rs",
    ];
    let protocol_names = external_protocol_feature_names();
    for relative in operation_files {
        let path = proxy_src().join(relative);
        let source = read_module(&path).to_ascii_lowercase();
        for protocol in &protocol_names {
            assert!(
                !source.contains(protocol.as_str()),
                "{} must execute capabilities without concrete `{protocol}` types",
                path.display()
            );
        }
    }
}

#[test]
fn tcp_runtime_dispatch_does_not_retain_engine_leaf_types() {
    assert_sources_exclude(
        &proxy_src().join("runtime/tcp_dispatch"),
        &["ResolvedLeafOutbound"],
    );
}

#[test]
fn tcp_prepared_operations_are_gated_by_tcp_capabilities_not_udp_runtime() {
    let operations = read(&proxy_src().join("runtime/tcp_dispatch/operation.rs"));
    assert!(operations.contains("feature = \"tcp-tunnel-runtime\""));
    assert!(operations.contains("feature = \"tcp-session-runtime\""));
    assert!(operations.contains("feature = \"tcp-transport-session-runtime\""));
    assert!(!operations.contains("feature = \"udp-runtime\""));

    let transport_leaf = read(&proxy_src().join("protocol_registry/transport_leaf/mod.rs"));
    assert!(transport_leaf.contains("feature = \"tcp-tunnel-runtime\""));
    assert!(transport_leaf.contains("feature = \"tcp-session-runtime\""));
    let tcp_claim = read(&proxy_src().join("protocol_registry/transport_leaf/tcp.rs"));
    assert!(!tcp_claim.contains("feature = \"udp-runtime\""));
    let prepared = read(&proxy_src().join("runtime/transport_leaf.rs"));
    assert!(!prepared.contains("feature = \"udp-runtime\""));
}

#[test]
fn proxy_runtime_does_not_retain_engine_leaf_types() {
    assert_sources_exclude(&proxy_src().join("runtime"), &["ResolvedLeafOutbound"]);
}

#[test]
fn udp_runtime_dispatch_does_not_retain_engine_leaf_types() {
    assert_sources_exclude(
        &proxy_src().join("runtime/udp_dispatch"),
        &["ResolvedLeafOutbound"],
    );
}

#[test]
fn udp_managed_bridge_runtime_does_not_retain_engine_leaf_types() {
    assert_sources_exclude(
        &proxy_src().join("runtime/udp_flow/managed/bridge"),
        &["ResolvedLeafOutbound"],
    );
}

#[test]
fn udp_packet_path_chain_runtime_does_not_retain_engine_leaf_types() {
    assert_sources_exclude(
        &proxy_src().join("runtime/udp_flow/packet_path_chain"),
        &["ResolvedLeafOutbound"],
    );
}

#[test]
fn transport_integrations_do_not_parse_top_level_inbound_protocol_enum() {
    assert_sources_exclude(
        &workspace_root().join("crates/transport/src"),
        &["InboundProtocolConfig"],
    );
}

#[test]
fn adapter_inbound_helpers_do_not_parse_top_level_inbound_protocol_enum() {
    for relative in [
        "adapters/vless/listener.rs",
        "adapters/vmess/listener.rs",
        "adapters/trojan/listener.rs",
        "adapters/hysteria2/inbound.rs",
        "adapters/socks5/inbound.rs",
        "adapters/shadowsocks/inbound.rs",
        "adapters/mieru/inbound.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        for forbidden in [
            "InboundProtocolConfig",
            "match &inbound.protocol",
            "match inbound.protocol",
            "InboundConfig",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} must receive typed protocol-owned inbound values instead of `{forbidden}`"
            );
        }
    }
}

#[test]
fn generic_runtime_does_not_dispatch_protocol_config_variants() {
    assert_sources_exclude(
        &proxy_src().join("runtime"),
        &[
            "match InboundProtocolConfig",
            "match &inbound.protocol",
            "match ResolvedLeafOutbound",
            "match leaf {\n        ResolvedLeafOutbound::",
            "ProtocolAdapter",
        ],
    );
}

#[test]
fn listener_and_task_lifecycle_are_runtime_owned() {
    let inbound_operation = read_module(&proxy_src().join("runtime/inbound_operation.rs"));
    let listener_loop = read(&proxy_src().join("runtime/listener_loop.rs"));
    let inventory_inbound = read(&proxy_src().join("inventory/inbound.rs"));
    let runtime_listeners = read(&proxy_src().join("runtime/listeners/inbound.rs"));
    assert!(inbound_operation.contains("PreparedInboundListenerOperation"));
    assert!(inbound_operation.contains("InboundConnectionContext"));
    assert!(inbound_operation.contains("JoinSet") || listener_loop.contains("JoinSet"));
    assert!(
        inbound_operation.contains("tokio::spawn") || listener_loop.contains("tokio::spawn"),
        "runtime must own connection task fan-out"
    );
    assert!(inventory_inbound.contains("prepare_inbound_listener("));
    assert!(!inventory_inbound.contains("check_inbound_enabled("));
    assert!(!runtime_listeners.contains("check_inbound_enabled("));
    assert!(!inventory_inbound.contains("JoinSet"));
    assert!(!inventory_inbound.contains("listeners.spawn("));
    assert!(!inventory_inbound.contains("operation.execute("));
    assert!(runtime_listeners.contains("listeners.spawn("));
    assert!(runtime_listeners.contains(".execute(listener_runtime, bound, shutdown_rx)"));
}

#[test]
fn capability_surface_is_split_and_context_is_narrow() {
    let capability = read(&proxy_src().join("protocol_registry/capability.rs"));
    let context = read_module(&proxy_src().join("protocol_registry/context.rs"));
    for capability_name in [
        "ProtocolSupportCapability",
        "InboundListenerCapability",
        "TcpOutboundCapability",
        "UdpFlowCapability",
        "UdpPacketPathCapability",
    ] {
        assert!(capability.contains(&format!("trait {capability_name}")));
    }
    assert!(!capability.contains("trait ProtocolAdapter"));
    assert!(!capability.contains("proxy: &Proxy"));
    assert!(!capability.contains("fn claim_tcp_outbound_leaf"));
    assert!(!capability.contains("fn claim_udp_flow_leaf"));
    assert!(!capability.contains("fn claim_udp_packet_path_leaf"));
    assert!(capability.contains("BoundInbound"));
    assert!(!capability.contains("fn runtime(&self) -> OutboundLeafRuntime;"));
    assert!(capability.contains("pub(crate) tcp_path: TcpPathCategory,"));
    assert!(!capability.contains("OutboundLeafRuntime"));
    assert!(!capability.contains("fn claims_outbound_leaf("));
    assert!(!capability.contains("fn outbound_leaf_runtime("));
    assert!(context.contains(
        "pub(crate) struct OutboundAdapterContext<'a> {\n    config: &'a RuntimeConfig,"
    ));
    assert!(
        !context.contains("pub(crate) struct OutboundAdapterContext<'a> {\n    proxy: &'a Proxy,")
    );
    assert!(context.contains("pub(crate) struct UdpAdapterContext<'a> {\n    config: &'a RuntimeConfig,\n    services: UdpRuntimeServices,"));
    assert!(!context.contains("pub(crate) struct UdpAdapterContext<'a> {\n    proxy: &'a Proxy,"));
    let tcp_leaf = read(&proxy_src().join("inventory/tcp/leaf.rs"));
    let tcp_operation = read_module(&proxy_src().join("runtime/tcp_dispatch/operation.rs"));
    let udp_operation = read_module(&proxy_src().join("runtime/udp_dispatch/operation.rs"));
    assert!(tcp_leaf.contains("OutboundAdapterContext"));
    assert!(tcp_operation.contains("TcpRuntimeServices"));
    assert!(udp_operation.contains("UdpAdapterContext"));
}

#[test]
fn transport_leaf_operations_are_generic() {
    let tcp = read_module(&proxy_src().join("runtime/tcp_dispatch/operation.rs"));
    let udp = read_module(&proxy_src().join("runtime/udp_dispatch/operation.rs"));
    let udp_claim = read(&proxy_src().join("protocol_registry/transport_leaf/udp.rs"));
    assert!(tcp.contains("TransportLeafTcpConnectOperation"));
    assert!(tcp.contains("TransportLeafTcpRelayOperation"));
    assert!(tcp.contains("PreparedTransportLeaf"));
    assert!(udp.contains("prepare_transport_udp_direct"));
    assert!(udp.contains("prepare_transport_udp_relay_two_stream"));
    assert!(!udp_claim.contains("impl<TLeaf> PreparedUdpFlowOperation"));
    assert!(!udp_claim.contains("async fn execute_"));
    assert!(!udp_claim.contains(".await"));
}

#[test]
fn transport_bridge_helpers_drop_raw_leaf_resolution_paths() {
    let tcp = read(&proxy_src().join("protocol_registry/transport_leaf/tcp.rs"));
    let udp = read(&proxy_src().join("protocol_registry/transport_leaf/udp.rs"));
    let combined = format!("{tcp}\n{udp}");
    assert!(!combined.contains("prepare_last_transport_bridge_leaf"));
    assert!(!combined.contains("prepare_transport_bridge_leaf"));
    assert!(!combined.contains("trait ProtocolTransportLeafResolver {"));
    assert!(!combined.contains("trait ProtocolTransportLeafResolver<'a>"));
    assert!(!combined.contains("ResolvedLeafOutbound"));
    assert!(!tcp.contains("ResolvedLeafOutbound"));
    assert!(!udp.contains("ResolvedLeafOutbound"));
    assert_sources_exclude(
        &proxy_src().join("protocol_registry/transport_leaf"),
        &["ResolvedLeafOutbound"],
    );
}

#[test]
fn tcp_prepared_operations_do_not_borrow_inventory_or_runtime_services() {
    let tcp_leaf = read(&proxy_src().join("inventory/tcp/leaf.rs"));
    let context = read_module(&proxy_src().join("protocol_registry/context.rs"));
    assert!(tcp_leaf.contains("fn prepare_claimed_tcp_candidate<'a>(\n        &self,"));
    assert!(tcp_leaf.contains("fn prepare_claimed_tcp_relay_hop<'a>(\n        &self,"));
    assert!(context.contains("pub(crate) fn prepare_tcp_outbound<'a>(\n        &'a self,"));
    assert!(!context.contains("pub(crate) fn prepare_tcp_candidate<'a>(\n        &self,"));
    assert!(!context.contains("pub(crate) fn prepare_tcp_relay_chain<'a>(\n        &self,"));
    assert!(!context.contains("pub(crate) fn prepare_tcp_relay_hop<'a>(\n        &self,"));
    assert!(!context.contains("ResolvedLeafOutbound"));
}

#[test]
fn udp_prepared_operations_do_not_borrow_adapters_or_bridges() {
    let capability = read(&proxy_src().join("protocol_registry/capability.rs"));
    let udp = read(&proxy_src().join("runtime/udp_dispatch/operation/transport.rs"));
    assert!(capability.contains("trait ClaimedUdpFlowLeaf<'a>"));
    assert!(capability.contains("fn prepare_udp_flow("));
    assert!(capability.contains("fn prepare_udp_relay("));
    assert!(!capability.contains("RelayCarrier"));
    assert!(!capability.contains("fn prepare_owned_udp_relay_final_hop("));
    assert!(!capability.contains("fn prepare_owned_udp_relay_two_stream("));
    assert!(
        !udp.contains("bridge: &'a TBridge"),
        "prepared UDP transport operations must own bridge state instead of borrowing it"
    );
}

#[test]
fn protocol_crates_do_not_depend_on_proxy_or_config_union_crates() {
    let protocols = workspace_root().join("protocols");
    for entry in fs::read_dir(&protocols).expect("read protocols") {
        let crate_dir = entry.expect("protocol crate").path();
        if !crate_dir.is_dir() {
            continue;
        }
        let manifest = crate_dir.join("Cargo.toml");
        if !manifest.exists() {
            continue;
        }
        let source = read(&manifest);
        for forbidden in ["zero-proxy", "zero-config"] {
            assert!(
                !source.contains(forbidden),
                "{} must not depend on `{forbidden}`",
                manifest.display()
            );
        }
    }
}

#[test]
fn protocol_registration_is_the_only_compiled_adapter_collection() {
    let register = read(&proxy_src().join("register.rs"));
    let inventory = rust_sources(&proxy_src().join("inventory"))
        .into_iter()
        .map(|path| read(&path))
        .collect::<String>();
    assert!(register.contains("ProtocolRegistry"));
    for protocol in ["VlessAdapter", "VmessAdapter", "TrojanAdapter"] {
        assert!(register.contains(protocol));
        assert!(
            !inventory.contains(protocol),
            "inventory must dispatch capabilities without concrete `{protocol}` access"
        );
    }
}

#[test]
fn runtime_owns_post_accept_route_execution() {
    let inbound_route = proxy_src().join("runtime/inbound_route");
    let combined = rust_sources(&inbound_route)
        .into_iter()
        .map(|path| read(&path))
        .collect::<String>();
    assert!(combined.contains("dispatch_protocol_stream_route"));
    assert!(combined.contains("dispatch_protocol_mux_route"));
    assert!(combined.contains("recorded"));
    assert!(combined.contains("InboundRouteRuntime"));
    assert!(combined.contains("MuxSubstreamRuntime"));
    assert!(!combined.contains("run_udp: move |proxy: Proxy"));
    assert!(!combined.contains("run_mux: move |proxy: Proxy"));
}

#[test]
fn adapter_runtime_service_access_does_not_expose_proxy() {
    let context = read_module(&proxy_src().join("protocol_registry/context.rs"));
    assert!(context.contains("struct TcpRuntimeServices"));
    assert!(context.contains("struct UdpRuntimeServices"));
    assert!(context.contains("fn runtime_services"));
    assert!(!context.contains("use crate::runtime::Proxy"));
    assert!(!context.contains("from_proxy("));
    assert!(!context.contains("proxy: Proxy"));
    let adapters = rust_sources(&proxy_src().join("adapters"))
        .into_iter()
        .map(|path| read(&path))
        .collect::<String>();
    assert!(!adapters.contains("ctx.proxy()"));
    assert!(!adapters.contains("use crate::runtime::Proxy"));
    assert!(!adapters.contains("proxy: Proxy"));
    assert!(!adapters.contains("|proxy: Proxy"));
}

#[test]
fn protocol_adapter_roots_do_not_construct_transport_plans_or_request_bundles_inline() {
    for relative in [
        "adapters/vless.rs",
        "adapters/vmess.rs",
        "adapters/trojan.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        for forbidden in [
            "PreparedVlessOutboundRequestBundle",
            "PreparedVmessOutboundRequestBundle",
            "PreparedTrojanOutboundRequestBundle",
            "VlessOutboundLeaf::from_profile_refs",
            "VmessOutboundLeaf::from_profile_refs",
            "OwnedVlessOutboundTransportPlan::from_profile_refs",
            "OwnedVmessOutboundTransportPlan::from_profile_refs",
            "OwnedTrojanOutboundTlsPlan::from_parts",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} must delegate protocol-private leaf construction instead of building `{forbidden}` inline"
            );
        }
    }
}

#[test]
fn transport_bridge_adapters_hold_protocol_runtime_handles_instead_of_protocol_pools() {
    for (relative, runtime_handle, forbidden) in [
        (
            "adapters/vless.rs",
            "VlessTransportRuntime",
            "mux_pool::MuxConnectionPool",
        ),
        (
            "adapters/vmess.rs",
            "VmessTransportRuntime",
            "VmessMuxConnectionPool",
        ),
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains(runtime_handle),
            "{relative} should hold protocol-owned runtime handle `{runtime_handle}`"
        );
        assert!(
            !source.contains(forbidden),
            "{relative} must not retain protocol-owned pool type `{forbidden}`"
        );
        assert!(
            !source.contains(".evict_all()"),
            "{relative} must delegate reload eviction through its protocol runtime handle"
        );
    }
}

#[test]
fn protocol_adapter_listener_modules_do_not_construct_transport_listener_requests_inline() {
    for relative in [
        "adapters/vless/listener.rs",
        "adapters/vmess/listener.rs",
        "adapters/trojan/listener.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        for forbidden in [
            "VlessInboundListenerRequest::from_profile_refs",
            "VmessInboundListenerRequest::from_profile_refs",
            "TrojanInboundListenerRequest::new",
            "build_required_tls_acceptor",
            "build_optional_tls_acceptor",
            "OwnedVlessInboundTransportPlan::from_profile_refs",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} must delegate protocol-owned inbound listener construction instead of building `{forbidden}` inline"
            );
        }
    }
}

#[test]
fn protocol_transport_roots_do_not_reexport_internal_outbound_plan_types() {
    for (relative, forbidden) in [
        (
            "protocols/vless/src/transport.rs",
            "pub use outbound::OwnedVlessOutboundTransportPlan;",
        ),
        (
            "protocols/vmess/src/transport.rs",
            "pub use outbound::OwnedVmessOutboundTransportPlan;",
        ),
        (
            "protocols/trojan/src/transport.rs",
            "pub use outbound::OwnedTrojanOutboundTlsPlan;",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains(forbidden),
            "{relative} must keep transport plan internals behind leaf-owned entrypoints instead of re-exporting `{forbidden}`"
        );
    }
}

#[test]
fn protocol_transport_roots_do_not_reexport_internal_owned_config_intermediates() {
    for (relative, forbidden) in [
        (
            "protocols/vless/src/transport.rs",
            "OwnedVlessInboundListenerConfig",
        ),
        (
            "protocols/vless/src/transport.rs",
            "OwnedVlessOutboundLeafConfig",
        ),
        (
            "protocols/vmess/src/transport.rs",
            "OwnedVmessInboundListenerConfig",
        ),
        (
            "protocols/vmess/src/transport.rs",
            "OwnedVmessOutboundLeafConfig",
        ),
        (
            "protocols/trojan/src/transport.rs",
            "OwnedTrojanInboundListenerConfig",
        ),
        (
            "protocols/trojan/src/transport.rs",
            "OwnedTrojanOutboundLeafConfig",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains(forbidden),
            "{relative} must keep `{forbidden}` internal instead of re-exporting protocol-owned config intermediates"
        );
    }
}

#[test]
fn protocol_transport_roots_do_not_reexport_legacy_inbound_helper_functions() {
    for (relative, forbidden) in [
        (
            "protocols/hysteria2/src/transport.rs",
            "inbound_profile_from_options",
        ),
        (
            "protocols/hysteria2/src/transport.rs",
            "inbound_profile_from_password",
        ),
        (
            "protocols/hysteria2/src/transport.rs",
            "inbound_tcp_acceptor",
        ),
        (
            "protocols/hysteria2/src/transport.rs",
            "accept_and_dispatch_authenticated_hysteria2_quic_session",
        ),
        (
            "protocols/mieru/src/transport.rs",
            "inbound_listener_request_from_users",
        ),
        (
            "protocols/shadowsocks/src/transport.rs",
            "inbound_listener_parts_from_options",
        ),
        (
            "protocols/shadowsocks/src/transport.rs",
            "inbound_listener_parts_from_cipher_password",
        ),
        (
            "protocols/socks5/src/transport.rs",
            "inbound_acceptor_from_users",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains(forbidden),
            "{relative} must expose typed inbound surfaces instead of legacy helper `{forbidden}`"
        );
    }
}

#[test]
fn protocol_transport_roots_do_not_reexport_legacy_udp_config_helpers() {
    for (relative, forbidden) in [
        (
            "protocols/hysteria2/src/transport.rs",
            "udp_flow_resume_from_config",
        ),
        (
            "protocols/hysteria2/src/transport.rs",
            "udp_packet_path_carrier_descriptor_from_config",
        ),
        (
            "protocols/hysteria2/src/transport.rs",
            "udp_packet_path_carrier_build_from_config",
        ),
        (
            "protocols/mieru/src/transport.rs",
            "udp_flow_resume_from_config",
        ),
        (
            "protocols/shadowsocks/src/transport.rs",
            "udp_flow_resume_from_config",
        ),
        (
            "protocols/shadowsocks/src/transport.rs",
            "udp_packet_path_carrier_descriptor_from_config",
        ),
        (
            "protocols/shadowsocks/src/transport.rs",
            "udp_packet_path_carrier_codec_from_config",
        ),
        (
            "protocols/shadowsocks/src/transport.rs",
            "udp_packet_path_datagram_source_build_from_config",
        ),
        (
            "protocols/socks5/src/transport.rs",
            "udp_association_target_from_config",
        ),
        (
            "protocols/socks5/src/transport.rs",
            "udp_packet_path_carrier_descriptor_from_config",
        ),
        (
            "protocols/socks5/src/transport.rs",
            "udp_packet_path_carrier_build_from_config",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains(forbidden),
            "{relative} must expose typed UDP surfaces instead of legacy helper `{forbidden}`"
        );
    }
}

#[test]
fn adapters_do_not_hold_protocol_owned_config_intermediates() {
    for (relative, forbidden) in [
        ("adapters/vless.rs", "OwnedVlessOutboundLeafConfig"),
        (
            "adapters/vless/listener.rs",
            "OwnedVlessInboundListenerConfig",
        ),
        ("adapters/vmess.rs", "OwnedVmessOutboundLeafConfig"),
        (
            "adapters/vmess/listener.rs",
            "OwnedVmessInboundListenerConfig",
        ),
        ("adapters/trojan.rs", "OwnedTrojanOutboundLeafConfig"),
        (
            "adapters/trojan/listener.rs",
            "OwnedTrojanInboundListenerConfig",
        ),
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains(forbidden),
            "{relative} must construct final protocol-owned leaves/requests without retaining `{forbidden}`"
        );
    }
}

#[test]
fn simpler_protocol_surfaces_do_not_expose_owned_inbound_intermediate_names() {
    for (relative, forbidden) in [
        (
            "protocols/shadowsocks/src/transport.rs",
            "OwnedShadowsocksInbound",
        ),
        ("protocols/mieru/src/transport.rs", "OwnedMieruInbound"),
        (
            "protocols/hysteria2/src/transport.rs",
            "OwnedHysteria2Inbound",
        ),
        ("protocols/socks5/src/transport.rs", "OwnedSocks5Inbound"),
        ("adapters/shadowsocks/inbound.rs", "OwnedShadowsocksInbound"),
        ("adapters/mieru/inbound.rs", "OwnedMieruInbound"),
        ("adapters/hysteria2.rs", "OwnedHysteria2Inbound"),
        ("adapters/socks5/inbound.rs", "OwnedSocks5Inbound"),
        ("adapters/mixed/inbound.rs", "OwnedSocks5Inbound"),
    ] {
        let source = if relative.starts_with("protocols/") {
            read(&workspace_root().join(relative))
        } else {
            read(&proxy_src().join(relative))
        };
        assert!(
            !source.contains(forbidden),
            "{relative} must not expose transitional inbound surface `{forbidden}`"
        );
    }
}

#[test]
fn vless_public_boundary_profiles_do_not_use_owned_prefixes() {
    for (relative, forbidden) in [
        (
            "protocols/vless/src/transport.rs",
            "OwnedVlessInboundBindPlan",
        ),
        (
            "protocols/vless/src/transport.rs",
            "OwnedVlessQuicBindProfile",
        ),
        (
            "protocols/vless/src/transport.rs",
            "OwnedVlessQuicClientProfile",
        ),
        (
            "protocols/vless/src/transport.rs",
            "OwnedVlessRealityClientProfile",
        ),
        ("adapters/vless.rs", "OwnedVlessInboundBindPlan"),
        ("adapters/vless.rs", "OwnedVlessQuicBindProfile"),
        ("adapters/vless.rs", "OwnedVlessQuicClientProfile"),
        ("adapters/vless.rs", "OwnedVlessRealityClientProfile"),
    ] {
        let source = if relative.starts_with("protocols/") {
            read(&workspace_root().join(relative))
        } else {
            read(&proxy_src().join(relative))
        };
        assert!(
            !source.contains(forbidden),
            "{relative} must not expose stable VLESS boundary type `{forbidden}`"
        );
    }
}

#[test]
fn vless_adapter_uses_protocol_option_refs_instead_of_private_profile_constructors() {
    let adapter = read(&proxy_src().join("adapters/vless.rs"));
    for forbidden in [
        "VlessQuicBindProfile",
        "VlessQuicClientProfile",
        "VlessRealityClientProfile",
        "fn outbound_reality_profile(",
        "fn quic_client_profile(",
        "fn quic_bind_profile(",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "adapters/vless.rs must not construct protocol-private VLESS profile `{forbidden}` inline"
        );
    }
    for required in [
        "VlessInboundBindPlan",
        "VlessOutboundBuildOptionsRef",
        "VlessOutboundOptionsRef",
        "VlessQuicBindOptionsRef",
        "VlessQuicClientOptionsRef",
        "VlessRealityClientOptionsRef",
        "VlessInboundBindPlan::from_options_refs",
        "VlessOutboundLeaf::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/vless.rs should project through protocol-owned VLESS option/runtime surface `{required}`"
        );
    }

    let transport = read(&workspace_root().join("protocols/vless/src/transport.rs"));
    for forbidden in [
        "VlessQuicBindProfile",
        "VlessQuicClientProfile",
        "VlessRealityClientProfile",
    ] {
        assert!(
            !transport.contains(forbidden),
            "protocols/vless/src/transport.rs must not re-export protocol-private VLESS profile surface `{forbidden}`"
        );
    }
}

#[test]
fn vless_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/vless.rs"));
    for required in [
        "VlessInboundOptionsRef",
        "VlessInboundUserRef",
        "VlessRealityServerOptionsRef",
        "VlessInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/vless.rs should project through protocol-owned VLESS inbound option surface `{required}`"
        );
    }

    let listener = read(&proxy_src().join("adapters/vless/listener.rs"));
    for forbidden in [
        "VlessInboundProfile::from_config_users",
        "VlessRealityServerProfile::from_config_server",
        "VlessInboundListenerRequest::from_config_refs",
        "VlessInboundListenerRequest::from_profile_refs",
        "VlessTransportRuntime",
        "VlessInboundOptionsRef",
        "VlessInboundUserRef",
        "VlessRealityServerOptionsRef",
        "VlessInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            !listener.contains(forbidden),
            "adapters/vless/listener.rs must consume prepared VLESS listener requests instead of `{forbidden}`"
        );
    }
}

#[test]
fn vmess_adapter_uses_protocol_outbound_option_refs() {
    let adapter = read(&proxy_src().join("adapters/vmess.rs"));
    for required in [
        "VmessOutboundBuildOptionsRef",
        "VmessOutboundOptionsRef",
        "VmessTransportRuntime",
        "VmessOutboundLeaf::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/vmess.rs should project through protocol-owned VMess outbound option surface `{required}`"
        );
    }
}

#[test]
fn vmess_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/vmess.rs"));
    for required in [
        "VmessInboundOptionsRef",
        "VmessInboundUserRef",
        "VmessInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/vmess.rs should project through protocol-owned VMess inbound option surface `{required}`"
        );
    }

    let listener = read(&proxy_src().join("adapters/vmess/listener.rs"));
    for forbidden in [
        "VmessInboundProfile::from_config_users",
        "VmessInboundListenerRequest::from_config_refs",
        "VmessInboundListenerRequest::from_profile_refs",
        "VmessTransportRuntime",
        "VmessInboundOptionsRef",
        "VmessInboundUserRef",
        "VmessInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            !listener.contains(forbidden),
            "adapters/vmess/listener.rs must consume prepared VMess listener requests instead of `{forbidden}`"
        );
    }
}

#[test]
fn heavy_protocol_transport_roots_expose_named_inbound_option_surfaces() {
    for (relative, required, forbidden) in [
        (
            "protocols/vless/src/transport.rs",
            "VlessInboundUserRef",
            "BorrowedVlessInboundUserConfigParts",
        ),
        (
            "protocols/vless/src/transport.rs",
            "VlessInboundOptionsRef",
            "BorrowedVlessInboundUserConfigParts",
        ),
        (
            "protocols/vmess/src/transport.rs",
            "VmessInboundUserRef",
            "BorrowedVmessInboundUserConfigParts",
        ),
        (
            "protocols/vmess/src/transport.rs",
            "VmessInboundOptionsRef",
            "BorrowedVmessInboundUserConfigParts",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            source.contains(required),
            "{relative} should expose named inbound option surface `{required}`"
        );
        assert!(
            !source.contains(forbidden),
            "{relative} must not re-export borrowed tuple inbound surface `{forbidden}`"
        );
    }
}

#[test]
fn heavy_protocol_inbound_modules_do_not_keep_borrowed_tuple_surfaces() {
    for (relative, forbidden) in [
        (
            "protocols/vless/src/inbound.rs",
            "BorrowedVlessInboundUserConfigParts",
        ),
        (
            "protocols/vmess/src/inbound.rs",
            "BorrowedVmessInboundUserConfigParts",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains(forbidden),
            "{relative} must not keep transitional borrowed tuple surface `{forbidden}`"
        );
    }
}

#[test]
fn heavy_protocol_transport_roots_expose_named_outbound_build_surfaces() {
    for (relative, required, forbidden) in [
        (
            "protocols/vless/src/transport.rs",
            "VlessOutboundBuildOptionsRef",
            None,
        ),
        (
            "protocols/vmess/src/transport.rs",
            "VmessOutboundBuildOptionsRef",
            None,
        ),
        (
            "protocols/trojan/src/transport.rs",
            "TrojanOutboundBuildOptionsRef",
            Some("TrojanTransportRuntime"),
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            source.contains(required),
            "{relative} should expose named outbound build surface `{required}`"
        );
        if let Some(forbidden) = forbidden {
            assert!(
                !source.contains(forbidden),
                "{relative} must not re-export transitional runtime wrapper `{forbidden}`"
            );
        }
    }
}

#[test]
fn trojan_adapter_uses_protocol_option_refs_directly() {
    let adapter = read(&proxy_src().join("adapters/trojan.rs"));
    for forbidden in [
        "TrojanOutboundLeaf::from_config_refs",
        "TrojanTransportRuntime",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "adapters/trojan.rs must not rely on legacy Trojan outbound surface `{forbidden}`"
        );
    }
    for required in [
        "TrojanOutboundBuildOptionsRef",
        "TrojanOutboundOptionsRef",
        "TrojanOutboundLeaf::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/trojan.rs should project directly through protocol-owned Trojan option surface `{required}`"
        );
    }
}

#[test]
fn carrier_rich_protocol_transport_runtimes_only_keep_reloadable_state() {
    for relative in [
        "protocols/vless/src/transport/runtime.rs",
        "protocols/vmess/src/transport/runtime.rs",
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            source.contains("evict_all()"),
            "{relative} should keep reload-driven pool eviction state"
        );
        for forbidden in [
            "build_outbound_leaf(",
            "from_profile_refs(",
            "from_options_refs(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} must not keep constructor passthrough `{forbidden}`"
            );
        }
    }
}

#[test]
fn heavy_protocol_transport_files_do_not_expose_public_config_constructors() {
    for (relative, required) in [
        (
            "protocols/vless/src/transport/inbound.rs",
            "pub(in crate::transport) fn from_profile_refs",
        ),
        (
            "protocols/vless/src/transport/leaf.rs",
            "pub(in crate::transport) fn from_profile_refs",
        ),
        (
            "protocols/vmess/src/transport/inbound.rs",
            "pub(in crate::transport) fn from_profile_refs",
        ),
        (
            "protocols/vmess/src/transport/leaf.rs",
            "pub(in crate::transport) fn from_profile_refs",
        ),
        (
            "protocols/trojan/src/transport/inbound.rs",
            "pub fn from_options_refs",
        ),
        (
            "protocols/trojan/src/transport/leaf.rs",
            "pub fn from_options_refs",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            source.contains(required),
            "{relative} should expose narrowed transport-owned constructor `{required}`"
        );
        assert!(
            !source.contains("pub fn from_config_refs"),
            "{relative} must not expose legacy public config constructor `pub fn from_config_refs`"
        );
    }
}

#[test]
fn carrier_rich_outbound_transport_leaves_expose_public_option_ref_constructors() {
    for (relative, required) in [
        (
            "protocols/vless/src/transport/leaf.rs",
            "pub fn from_options_refs",
        ),
        (
            "protocols/vmess/src/transport/leaf.rs",
            "pub fn from_options_refs",
        ),
        (
            "protocols/trojan/src/transport/leaf.rs",
            "pub fn from_options_refs",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            source.contains(required),
            "{relative} should expose typed public outbound option constructor `{required}`"
        );
    }
}

#[test]
fn heavy_protocol_inbound_transport_requests_expose_public_option_ref_constructors() {
    for (relative, required) in [
        (
            "protocols/vless/src/transport/inbound.rs",
            "pub fn from_options_refs",
        ),
        (
            "protocols/vmess/src/transport/inbound.rs",
            "pub fn from_options_refs",
        ),
        (
            "protocols/trojan/src/transport/inbound.rs",
            "pub fn from_options_refs",
        ),
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            source.contains(required),
            "{relative} should expose typed public option constructor `{required}`"
        );
    }
}

#[test]
fn hysteria2_adapter_uses_protocol_bind_option_refs() {
    let adapter = read(&proxy_src().join("adapters/hysteria2.rs"));
    let forbidden = "Hysteria2InboundBindPlan::from_paths";
    assert!(
        !adapter.contains(forbidden),
        "adapters/hysteria2.rs must not build hysteria2 bind plans via `{forbidden}`"
    );
    for required in [
        "Hysteria2InboundBindOptionsRef",
        "Hysteria2InboundBindPlan::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/hysteria2.rs should project through protocol-owned hysteria2 bind option surface `{required}`"
        );
    }
}

#[test]
fn hysteria2_adapter_uses_protocol_outbound_option_refs() {
    let adapter = read(&proxy_src().join("adapters/hysteria2.rs"));
    let forbidden = "Hysteria2TransportLeaf::new(";
    assert!(
        !adapter.contains(forbidden),
        "adapters/hysteria2.rs must not construct hysteria2 transport leaves via `{forbidden}`"
    );
    for required in [
        "Hysteria2OutboundOptionsRef",
        "Hysteria2TransportLeaf::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/hysteria2.rs should project outbound through protocol-owned hysteria2 option surface `{required}`"
        );
    }
}

#[test]
fn hysteria2_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/hysteria2.rs"));
    for required in [
        "Hysteria2InboundOptionsRef",
        "Hysteria2AuthenticatedInboundProfile::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/hysteria2.rs should project through protocol-owned hysteria2 inbound option surface `{required}`"
        );
    }

    let listener = read(&proxy_src().join("adapters/hysteria2/inbound.rs"));
    let forbidden = "inbound_profile_from_options";
    assert!(
        !listener.contains(forbidden),
        "adapters/hysteria2/inbound.rs must not construct hysteria2 inbound profile via `{forbidden}`"
    );
    for forbidden in [
        "Hysteria2InboundOptionsRef",
        "Hysteria2AuthenticatedInboundProfile::from_options_refs",
    ] {
        assert!(
            !listener.contains(forbidden),
            "adapters/hysteria2/inbound.rs must consume prepared hysteria2 profiles instead of `{forbidden}`"
        );
    }
}

#[test]
fn hysteria2_protocol_inbound_surface_stops_at_authenticated_quic_connections() {
    let inbound = read(&workspace_root().join("protocols/hysteria2/src/inbound.rs"));
    for forbidden in [
        "dispatch_session_with_handlers",
        "accept_and_dispatch_authenticated_quic_session",
        "JoinSet",
        "tokio::select!",
    ] {
        assert!(
            !inbound.contains(forbidden),
            "protocols/hysteria2/src/inbound.rs must not own QUIC task orchestration via `{forbidden}`"
        );
    }
    for required in [
        "pub async fn accept_authenticated_quic_session",
        "pub async fn accept_next_tcp_stream",
        "pub fn accept_udp_session(&self)",
    ] {
        assert!(
            inbound.contains(required),
            "protocols/hysteria2/src/inbound.rs should stop at authenticated QUIC connection surfaces like `{required}`"
        );
    }
}

#[test]
fn shadowsocks_adapter_accepts_protocol_stream_before_runtime_handoff() {
    let listener = read(&proxy_src().join("adapters/shadowsocks/inbound.rs"));
    assert!(
        listener.contains(".accept_stream("),
        "adapters/shadowsocks/inbound.rs should accept a protocol-owned stream surface before runtime handoff"
    );
    assert!(
        !listener.contains("accept_and_dispatch_stream"),
        "adapters/shadowsocks/inbound.rs must not use legacy accept-and-dispatch helpers"
    );

    let inbound = read(&workspace_root().join("protocols/shadowsocks/src/inbound.rs"));
    assert!(
        !inbound.contains("accept_and_dispatch_stream"),
        "protocols/shadowsocks/src/inbound.rs must stop at accept_stream instead of owning runtime handoff helpers"
    );
}

#[test]
fn mieru_adapter_accepts_protocol_session_before_runtime_handoff() {
    let listener = read(&proxy_src().join("adapters/mieru/inbound.rs"));
    assert!(
        listener.contains(".accept_client("),
        "adapters/mieru/inbound.rs should accept a protocol-owned session surface before runtime handoff"
    );
    assert!(
        listener.contains("MieruInboundAcceptedSession::Tcp"),
        "adapters/mieru/inbound.rs should explicitly branch on accepted mieru TCP sessions"
    );
    assert!(
        listener.contains("MieruInboundAcceptedSession::Udp"),
        "adapters/mieru/inbound.rs should explicitly branch on accepted mieru UDP sessions"
    );

    let inbound = read(&workspace_root().join("protocols/mieru/src/inbound.rs"));
    assert!(
        !inbound.contains("pub async fn dispatch<"),
        "protocols/mieru/src/inbound.rs must stop at accepted session surfaces instead of owning adapter dispatch helpers"
    );
}

#[test]
fn vmess_transport_accepts_client_owned_surface_without_route_wrappers() {
    let transport = read(&workspace_root().join("protocols/vmess/src/transport/inbound.rs"));
    assert!(
        transport.contains(".accept_client_owned("),
        "protocols/vmess/src/transport/inbound.rs should consume the owned accepted client surface directly"
    );
    assert!(
        !transport.contains(".accept_route_owned("),
        "protocols/vmess/src/transport/inbound.rs must not round-trip through legacy route wrapper helpers"
    );

    let inbound = read(&workspace_root().join("protocols/vmess/src/inbound.rs"));
    assert!(
        !inbound.contains("pub async fn accept_route_owned"),
        "protocols/vmess/src/inbound.rs must not expose legacy owned route wrapper helpers"
    );
    assert!(
        !inbound.contains("pub async fn accept_route_owned_with"),
        "protocols/vmess/src/inbound.rs must not expose callback-based owned route helpers"
    );
}

#[test]
fn trojan_transport_accepts_client_owned_surface_without_route_wrappers() {
    let transport = read(&workspace_root().join("protocols/trojan/src/transport/inbound.rs"));
    assert!(
        transport.contains(".accept_client_owned("),
        "protocols/trojan/src/transport/inbound.rs should consume the owned accepted client surface directly"
    );
    assert!(
        !transport.contains(".accept_route_owned("),
        "protocols/trojan/src/transport/inbound.rs must not round-trip through legacy route wrapper helpers"
    );

    let inbound = read(&workspace_root().join("protocols/trojan/src/inbound.rs"));
    assert!(
        !inbound.contains("pub async fn accept_route_owned"),
        "protocols/trojan/src/inbound.rs must not expose legacy owned route wrapper helpers"
    );
    assert!(
        !inbound.contains("pub async fn accept_route_owned_with"),
        "protocols/trojan/src/inbound.rs must not expose callback-based owned route helpers"
    );
}

#[test]
fn vless_transport_plan_accepts_client_owned_surface_without_callback_wrappers() {
    let plan = read(&workspace_root().join("protocols/vless/src/transport/inbound/plan.rs"));
    assert!(
        plan.contains(".accept_client_owned("),
        "protocols/vless/src/transport/inbound/plan.rs should consume the owned accepted client surface directly"
    );
    assert!(
        !plan.contains(".accept_route_owned_with_sni_or_else("),
        "protocols/vless/src/transport/inbound/plan.rs must not round-trip through callback-based route wrapper helpers"
    );

    let inbound = read(&workspace_root().join("protocols/vless/src/inbound.rs"));
    assert!(
        !inbound.contains("pub async fn accept_route_owned_with"),
        "protocols/vless/src/inbound.rs must not expose callback-based owned route helpers"
    );
}

#[test]
fn socks5_adapter_accepts_protocol_request_before_runtime_handoff() {
    let listener = read(&proxy_src().join("adapters/socks5/inbound/listener.rs"));
    assert!(
        listener.contains(".accept_command(&mut metered)"),
        "adapters/socks5/inbound/listener.rs should accept a protocol-owned request surface before runtime handoff"
    );
    assert!(
        listener.contains("Socks5Request::Connect"),
        "adapters/socks5/inbound/listener.rs should explicitly branch on accepted SOCKS5 CONNECT requests"
    );
    assert!(
        listener.contains("Socks5Request::UdpAssociate"),
        "adapters/socks5/inbound/listener.rs should explicitly branch on accepted SOCKS5 UDP ASSOCIATE requests"
    );
    assert!(
        listener.contains("setup_inbound_udp_association(&mut metered, request)"),
        "adapters/socks5/inbound/listener.rs should keep SOCKS5 UDP associate setup explicit at the adapter/runtime boundary"
    );

    let inbound = read(&workspace_root().join("protocols/socks5/src/inbound.rs"));
    assert!(
        !inbound.contains("pub async fn dispatch_with_handlers"),
        "protocols/socks5/src/inbound.rs must stop at accepted request surfaces instead of owning adapter dispatch helpers"
    );
}

#[test]
fn protocol_mux_servers_dispatch_opened_routes_without_handler_wrappers() {
    for relative in ["protocols/vless/src/mux.rs", "protocols/vmess/src/mux.rs"] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains("dispatch_with_handlers("),
            "{relative} must not keep callback-style opened-route dispatch helpers"
        );
        assert!(
            !source.contains("dispatch_next_opened_route_with_handlers("),
            "{relative} must not keep secondary handler-wrapper dispatch entrypoints"
        );
        assert!(
            source.contains("match route.state"),
            "{relative} should branch explicitly on opened mux routes"
        );
    }
}

#[test]
fn heavy_protocol_inbound_transport_requests_do_not_keep_listener_config_wrappers() {
    for relative in [
        "protocols/vless/src/transport/inbound.rs",
        "protocols/vmess/src/transport/inbound.rs",
        "protocols/trojan/src/transport/inbound.rs",
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains("OwnedVlessInboundListenerConfig")
                && !source.contains("OwnedVmessInboundListenerConfig")
                && !source.contains("OwnedTrojanInboundListenerConfig"),
            "{relative} must build final inbound listener requests directly instead of keeping listener-config wrapper intermediates"
        );
        assert!(
            !source.contains("impl From<Owned"),
            "{relative} must not rely on wrapper-to-request conversion shims"
        );
    }
}

#[test]
fn heavy_protocol_outbound_transport_leaves_do_not_keep_leaf_config_wrappers() {
    for relative in [
        "protocols/vless/src/transport/leaf.rs",
        "protocols/vmess/src/transport/leaf.rs",
        "protocols/trojan/src/transport/leaf.rs",
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains("OwnedVlessOutboundLeafConfig")
                && !source.contains("OwnedVmessOutboundLeafConfig")
                && !source.contains("OwnedTrojanOutboundLeafConfig"),
            "{relative} must construct final outbound leaves directly instead of keeping leaf-config wrapper intermediates"
        );
        assert!(
            !source.contains("impl From<Owned"),
            "{relative} must not rely on wrapper-to-leaf conversion shims"
        );
    }
}

#[test]
fn mieru_adapter_uses_protocol_outbound_option_refs() {
    let adapter = read(&proxy_src().join("adapters/mieru.rs"));
    let forbidden = "MieruTransportLeaf::new(";
    assert!(
        !adapter.contains(forbidden),
        "adapters/mieru.rs must not construct mieru transport leaves via `{forbidden}`"
    );
    for required in [
        "MieruOutboundOptionsRef",
        "MieruTransportLeaf::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/mieru.rs should project outbound through protocol-owned mieru option surface `{required}`"
        );
    }
}

#[test]
fn mieru_adapter_consumes_the_protocol_owned_managed_connector_flow_type() {
    let adapter = read(&proxy_src().join("adapters/mieru/udp.rs"));
    assert!(adapter.contains("::mieru::transport::MieruManagedUdpConnectorFlow"));
    assert!(!adapter.contains("ManagedConnectorFlow<::mieru::udp::MieruUdpConnectorFlow>"));

    let protocol = read(&workspace_root().join("protocols/mieru/src/transport/managed_udp.rs"));
    assert!(protocol.contains("pub type MieruManagedUdpConnectorFlow"));
}

#[test]
fn mieru_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/mieru.rs"));
    for required in [
        "MieruInboundUserRef",
        "MieruInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/mieru.rs should project through protocol-owned mieru inbound option surface `{required}`"
        );
    }

    let listener = read(&proxy_src().join("adapters/mieru/inbound.rs"));
    let forbidden = "inbound_listener_request_from_users";
    assert!(
        !listener.contains(forbidden),
        "adapters/mieru/inbound.rs must not construct mieru inbound listener state via `{forbidden}`"
    );
    for forbidden in [
        "MieruInboundUserRef",
        "MieruInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            !listener.contains(forbidden),
            "adapters/mieru/inbound.rs must consume prepared mieru listener requests instead of `{forbidden}`"
        );
    }
}

#[test]
fn shadowsocks_adapter_uses_protocol_outbound_option_refs() {
    let adapter = read(&proxy_src().join("adapters/shadowsocks.rs"));
    let forbidden = "ShadowsocksTransportLeaf::new(";
    assert!(
        !adapter.contains(forbidden),
        "adapters/shadowsocks.rs must not construct shadowsocks transport leaves via `{forbidden}`"
    );
    for required in [
        "ShadowsocksOutboundOptionsRef",
        "ShadowsocksTransportLeaf::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/shadowsocks.rs should project outbound through protocol-owned shadowsocks option surface `{required}`"
        );
    }
}

#[test]
fn shadowsocks_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/shadowsocks.rs"));
    for required in [
        "ShadowsocksInboundOptionsRef",
        "ShadowsocksInboundBindings::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/shadowsocks.rs should project through protocol-owned shadowsocks inbound option surface `{required}`"
        );
    }

    let listener = read(&proxy_src().join("adapters/shadowsocks/inbound.rs"));
    let forbidden = "inbound_listener_parts_from_options";
    assert!(
        !listener.contains(forbidden),
        "adapters/shadowsocks/inbound.rs must not construct shadowsocks inbound listener state via `{forbidden}`"
    );
    assert!(
        listener.contains("ShadowsocksInboundBindings"),
        "adapters/shadowsocks/inbound.rs should consume prepared shadowsocks bindings"
    );
    for forbidden in [
        "ShadowsocksInboundOptionsRef",
        "ShadowsocksInboundBindings::from_options_refs",
    ] {
        assert!(
            !listener.contains(forbidden),
            "adapters/shadowsocks/inbound.rs must consume prepared shadowsocks bindings instead of `{forbidden}`"
        );
    }
}

#[test]
fn socks5_adapter_uses_protocol_outbound_option_refs() {
    let adapter = read(&proxy_src().join("adapters/socks5.rs"));
    let forbidden = "Socks5TransportLeaf::new(";
    assert!(
        !adapter.contains(forbidden),
        "adapters/socks5.rs must not construct socks5 transport leaves via `{forbidden}`"
    );
    for required in [
        "Socks5OutboundOptionsRef",
        "Socks5TransportLeaf::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/socks5.rs should project outbound through protocol-owned socks5 option surface `{required}`"
        );
    }
}

#[test]
fn socks5_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/socks5.rs"));
    for required in [
        "Socks5InboundUserRef",
        "Socks5InboundAcceptor::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/socks5.rs should project through protocol-owned socks5 inbound option surface `{required}`"
        );
    }

    let listener = read(&proxy_src().join("adapters/socks5/inbound.rs"));
    let forbidden = "inbound_acceptor_from_users";
    assert!(
        !listener.contains(forbidden),
        "adapters/socks5/inbound.rs must not construct socks5 inbound acceptors via `{forbidden}`"
    );
    assert!(
        listener.contains("Socks5InboundAcceptor"),
        "adapters/socks5/inbound.rs should consume a prepared socks5 acceptor"
    );
    for forbidden in [
        "Socks5InboundUserRef",
        "Socks5InboundAcceptor::from_options_refs",
    ] {
        assert!(
            !listener.contains(forbidden),
            "adapters/socks5/inbound.rs must consume prepared socks5 acceptors instead of `{forbidden}`"
        );
    }
}

#[test]
fn mixed_listener_adapter_uses_protocol_socks5_option_refs() {
    let listener = read(&proxy_src().join("adapters/mixed/inbound.rs"));
    let forbidden = "inbound_acceptor_from_users";
    assert!(
        !listener.contains(forbidden),
        "adapters/mixed/inbound.rs must not construct mixed socks5 acceptors via `{forbidden}`"
    );
    for required in [
        "Socks5InboundUserRef",
        "Socks5InboundAcceptor::from_options_refs",
    ] {
        assert!(
            listener.contains(required),
            "adapters/mixed/inbound.rs should project through protocol-owned socks5 option surface `{required}`"
        );
    }
}

#[test]
fn trojan_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/trojan.rs"));
    for required in [
        "TrojanInboundOptionsRef",
        "TrojanInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/trojan.rs should project through protocol-owned Trojan inbound option surface `{required}`"
        );
    }

    let listener = read(&proxy_src().join("adapters/trojan/listener.rs"));
    for forbidden in [
        "TrojanInboundProfile::from_config_password",
        "TrojanInboundListenerRequest::from_config_refs",
        "TrojanInboundListenerRequest::from_profile_refs",
        "TrojanTransportRuntime",
        "TrojanInboundOptionsRef",
        "TrojanInboundListenerRequest::from_options_refs",
    ] {
        assert!(
            !listener.contains(forbidden),
            "adapters/trojan/listener.rs must consume prepared Trojan listener requests instead of `{forbidden}`"
        );
    }
}

#[test]
fn heavy_transport_bridge_adapters_use_protocol_owned_outbound_options_directly() {
    for (relative, removed_projection, config_variant, build_options) in [
        (
            "adapters/vless.rs",
            "VlessOutboundProjection",
            "OutboundProtocolConfig::Vless {",
            "VlessOutboundBuildOptionsRef {",
        ),
        (
            "adapters/vmess.rs",
            "VmessOutboundProjection",
            "OutboundProtocolConfig::Vmess {",
            "VmessOutboundBuildOptionsRef {",
        ),
        (
            "adapters/trojan.rs",
            "TrojanOutboundProjection",
            "OutboundProtocolConfig::Trojan {",
            "TrojanOutboundBuildOptionsRef {",
        ),
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains(&format!("struct {removed_projection}")),
            "{relative} must not duplicate protocol-owned options in `{removed_projection}`"
        );
        assert!(
            source.contains("fn outbound_options<'a>("),
            "{relative} should project config directly into the protocol-owned option surface"
        );
        assert!(
            source.contains("fn claim_outbound_leaf_impl"),
            "{relative} should expose one claim-time outbound helper"
        );
        assert!(
            source.contains("OutboundLeafInput::Proxy { outbound, endpoint }"),
            "{relative} should consume the registry-resolved endpoint"
        );
        assert!(
            !source.contains("fn endpoint(&self)"),
            "{relative} must not repeat endpoint projection after registry claim"
        );
        assert_eq!(
            source.matches(config_variant).count(),
            1,
            "{relative} should match its protocol config variant in one thin option projection"
        );
        assert_eq!(
            source.matches(build_options).count(),
            1,
            "{relative} should construct one outbound build bundle shape and reuse it across TCP and UDP"
        );
    }
}

#[test]
fn inventory_udp_dispatch_keeps_relay_choreography_outside_candidate_root() {
    let dispatch = read(&proxy_src().join("inventory/udp/outbound.rs"));
    assert!(!dispatch.contains("ClaimedResolvedOutbound"));
    assert!(!dispatch.contains("claim_udp_outbound(resolved)?"));
    assert!(dispatch.contains("match resolved"));
    assert!(dispatch.contains("prepare_udp_outbound<'a>("));
    assert!(!dispatch.contains("async fn"));
    assert!(!dispatch.contains(".await"));
    assert!(!dispatch.contains(".execute("));
    assert!(!dispatch.contains("dispatch_tcp_relay_prefix"));
    assert!(!dispatch.contains("prepare_udp_packet_path_pair"));
    assert!(!dispatch.contains("prepare_udp_leaf_candidate("));
    assert!(!dispatch.contains("prepare_udp_relay_chain("));
    assert!(!dispatch.contains("UdpPacketRef"));
    assert!(!dispatch.contains("ResolvedLeafOutbound"));
    assert!(!dispatch.contains("enum UdpCandidate"));
    assert!(!dispatch.contains(".outbound_leaf_runtime("));
    assert!(!dispatch.contains("start_udp_leaf_flow("));
    assert!(!dispatch.contains("start_udp_relay_chain("));
}

#[test]
fn inventory_tcp_dispatch_claims_resolved_outbound_before_prepare() {
    let dispatch = read(&proxy_src().join("inventory/tcp/outbound.rs"));
    assert!(!dispatch.contains("ClaimedResolvedOutbound"));
    assert!(!dispatch.contains("claim_tcp_outbound(resolved)?"));
    assert!(dispatch.contains("match resolved"));
    assert!(dispatch.contains("prepare_tcp_outbound<'a>("));
    assert!(!dispatch.contains("async fn"));
    assert!(!dispatch.contains(".await"));
    assert!(!dispatch.contains(".execute("));
    assert!(!dispatch.contains("prepare_tcp_candidate("));
    assert!(!dispatch.contains("prepare_tcp_relay_chain("));
    assert!(!dispatch.contains("ResolvedLeafOutbound"));
    assert!(!dispatch.contains(".outbound_leaf_runtime("));
}

#[test]
fn inventory_udp_dispatch_uses_adapter_context_instead_of_proxy() {
    let dispatch = read(&proxy_src().join("inventory/udp/outbound.rs"));
    assert!(!dispatch.contains("use crate::runtime::Proxy"));
    assert!(!dispatch.contains("&Proxy"));
}

#[test]
fn inventory_udp_leaf_and_relay_use_adapter_context_instead_of_proxy() {
    for relative in ["inventory/udp/leaf.rs", "inventory/udp/relay.rs"] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly"
        );
        assert!(
            !source.contains("&Proxy"),
            "{relative} must not carry raw Proxy references"
        );
    }
    let udp_leaf = read(&proxy_src().join("inventory/udp/leaf.rs"));
    let udp_relay = read(&proxy_src().join("inventory/udp/relay.rs"));
    assert!(!udp_leaf.contains("pub(crate) fn prepare_udp_leaf_candidate<'a>("));
    assert!(!udp_leaf.contains("pub(crate) async fn start_udp_leaf_flow("));
    assert!(!udp_leaf.contains("find_udp_flow_leaf("));
    assert!(!udp_relay.contains("find_udp_flow_leaf("));
    assert!(!udp_relay.contains("find_udp_packet_path_leaf("));
    assert!(!udp_relay.contains("pub(crate) fn prepare_udp_packet_path_pair<'a>("));
    assert!(!udp_relay.contains("pub(crate) fn udp_relay_needs_two_streams("));
    assert!(!udp_relay.contains("pub(crate) async fn start_udp_relay_two_stream("));
    assert!(!udp_relay.contains("pub(crate) async fn start_udp_relay_final_hop("));
    assert!(!udp_leaf.contains("UdpFlowCapability"));
    assert!(!udp_relay.contains("UdpFlowCapability"));
    assert!(!udp_relay.contains("UdpPacketPathCapability"));
}

#[test]
fn inventory_udp_packet_path_builder_stays_prepared_operation_scoped() {
    let packet_path = read(&proxy_src().join("inventory/udp/packet_path.rs"));
    assert!(!packet_path.contains("ResolvedLeafOutbound"));
    assert!(!packet_path.contains("UdpPacketPathCapability"));
    assert!(packet_path.contains("PreparedUdpPacketPathOperation"));
}

#[test]
fn runtime_tcp_relay_executes_prepared_chain_without_engine_leaf_roundtrip() {
    let inventory_relay = read(&proxy_src().join("inventory/tcp/relay.rs"));
    let runtime_relay = read(&proxy_src().join("runtime/tcp_dispatch/relay.rs"));
    assert!(
        !runtime_relay.contains("apply_tcp_relay_hop("),
        "tcp relay execution should consume the final prepared hop directly"
    );
    assert!(
        !runtime_relay.contains("ResolvedLeafOutbound"),
        "runtime relay execution must not recover raw engine leaf values"
    );
    assert!(!inventory_relay.contains("current_prepared"));
    assert!(!inventory_relay.contains("stage: \"relay_last\""));
    assert!(runtime_relay.contains("current_prepared"));
    assert!(runtime_relay.contains("stage: \"relay_last\""));
}

#[test]
fn inventory_udp_relay_prepares_continuation_without_opening_carriers() {
    let relay = read(&proxy_src().join("inventory/udp/relay.rs"));
    assert!(relay.contains("PreparedUdpRelayChain::FinalHop"));
    assert!(relay.contains("PreparedUdpRelayChain::TwoStream"));
    assert!(relay.contains("prepare_claimed_udp_relay_chain<'a>("));
    assert!(relay.contains("prepare_udp_relay(ctx.source_dir())"));
    assert!(!relay.contains("dispatch_prepared_tcp_relay_carrier("));
    assert!(!relay.contains(".await"));
    assert!(!relay.contains("RelayCarrier"));
}

#[test]
fn inventory_relay_modules_only_prepare_claimed_chains() {
    for relative in ["inventory/tcp/relay.rs", "inventory/udp/relay.rs"] {
        let relay = read(&proxy_src().join(relative));
        assert!(
            !relay.contains("ResolvedLeafOutbound"),
            "{relative} should not carry raw engine leaf types after the inventory claim boundary"
        );
        assert!(
            !relay.contains("claim_outbound_leaf("),
            "{relative} should prepare already claimed leaves instead of reclaiming raw outbounds"
        );
    }
    let tcp_relay = read(&proxy_src().join("inventory/tcp/relay.rs"));
    for forbidden in [
        "dispatch_prepared_tcp_candidate(",
        ".into_relay_stream()",
        "current_prepared.execute(",
        "stage: \"relay_last\"",
    ] {
        assert!(
            !tcp_relay.contains(forbidden),
            "inventory tcp relay must not execute prepared relay chains via `{forbidden}`"
        );
    }
}

#[test]
fn runtime_udp_relay_executes_prepared_chain() {
    let inventory_relay = read(&proxy_src().join("inventory/udp/relay.rs"));
    let runtime_relay = read(&proxy_src().join("runtime/udp_dispatch/relay.rs"));
    assert!(!inventory_relay.contains("impl PreparedUdpRelayChain"));
    assert!(!inventory_relay.contains("send_packet_path_chain("));
    assert!(!inventory_relay.contains("operation.execute("));
    assert!(runtime_relay.contains("impl PreparedUdpRelayChain"));
    assert!(runtime_relay.contains("send_packet_path_chain("));
    assert!(runtime_relay.contains("bind_final_hop(carrier)"));
    assert!(runtime_relay.contains("bind_two_stream(post_carrier, get_carrier)"));
    assert!(runtime_relay.contains("dispatch_prepared_tcp_relay_carrier("));
    assert!(!runtime_relay.contains("ResolvedLeafOutbound"));
    assert!(!runtime_relay.contains("ClaimedOutboundLeaf"));
}

#[test]
fn udp_two_stream_transport_bridge_uses_carrier_only_relay_prefix() {
    let relay = read(&proxy_src().join("inventory/udp/relay.rs"));
    let runtime = read(&proxy_src().join("runtime/udp_dispatch/relay.rs"));
    let udp = read(&proxy_src().join("protocol_registry/transport_leaf/udp.rs"));
    assert!(relay.contains("prepare_claimed_tcp_relay_chain("));
    assert!(!relay.contains("dispatch_prepared_tcp_relay_carrier("));
    assert!(runtime.contains("dispatch_prepared_tcp_relay_carrier(post_prefix)"));
    assert!(runtime.contains("dispatch_prepared_tcp_relay_carrier(get_prefix)"));
    assert!(udp.contains("fn bind_two_stream("));
    assert!(!relay.contains("prepare_tcp_relay_chain(&chain)"));
    assert!(!udp.contains("prepare_claimed_tcp_relay_chain("));
    assert!(!udp.contains("dispatch_prepared_tcp_relay_carrier(post_prepared)"));
    assert!(!udp.contains("dispatch_prepared_tcp_relay_carrier(get_prepared)"));
}

#[test]
fn managed_stream_connectors_use_runtime_services_instead_of_proxy() {
    assert_sources_exclude(
        &proxy_src().join("runtime/udp_flow/managed/stream_manager/connector"),
        &["use crate::runtime::Proxy", "&Proxy", "Option<&Proxy>"],
    );
}

#[test]
fn managed_datagram_connectors_use_runtime_services_instead_of_proxy() {
    assert_sources_exclude(
        &proxy_src().join("runtime/udp_flow/managed/datagram_manager/connector"),
        &["use crate::runtime::Proxy", "&Proxy", "Option<&Proxy>"],
    );
}

#[test]
fn registered_upstream_runtime_uses_runtime_services_instead_of_proxy() {
    assert_sources_exclude(
        &proxy_src().join("runtime/udp_flow/registered/upstream/runtime/association"),
        &["use crate::runtime::Proxy", "&Proxy"],
    );
}

#[test]
fn managed_stream_bridge_requests_use_runtime_services_instead_of_proxy() {
    assert_sources_exclude(
        &proxy_src().join("runtime/udp_flow/managed/bridge/stream_packet"),
        &[
            "use crate::runtime::Proxy",
            "Option<&'a Proxy>",
            "Option<&Proxy>",
        ],
    );
}

#[test]
fn managed_udp_forward_paths_use_runtime_services_instead_of_proxy() {
    for relative in [
        "runtime/udp_flow/managed/state/forward.rs",
        "runtime/udp_flow/managed/stream/forward.rs",
        "runtime/udp_flow/registered/forward.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly"
        );
        assert!(
            !source.contains("&Proxy"),
            "{relative} must not carry raw Proxy references"
        );
    }
}

#[test]
fn tcp_dispatch_operations_use_runtime_services_for_connect_flows() {
    let operation = read_module(&proxy_src().join("runtime/tcp_dispatch/operation.rs"));
    assert!(operation.contains("TcpRuntimeServices"));
    assert!(!operation.contains("ctx.proxy()"));
}

#[test]
fn udp_dispatch_operations_use_runtime_services_for_direct_flows() {
    let operation = read_module(&proxy_src().join("runtime/udp_dispatch/operation.rs"));
    assert!(operation.contains("ctx.runtime_services()"));
    assert!(!operation.contains("ctx.proxy()"));
}

#[test]
fn transport_leaf_udp_execution_uses_runtime_services_instead_of_proxy() {
    let claim = read(&proxy_src().join("protocol_registry/transport_leaf/udp.rs"));
    let udp = read(&proxy_src().join("runtime/udp_dispatch/operation/transport.rs"));
    assert!(!claim.contains(".await"));
    assert!(!claim.contains("impl<TLeaf> PreparedUdpFlowOperation"));
    assert!(!udp.contains("use crate::runtime::Proxy"));
    assert!(!udp.contains("ctx.proxy()"));
    assert!(!udp.contains("&Proxy"));
}

#[test]
fn packet_path_start_surfaces_use_adapter_context_instead_of_proxy() {
    for relative in [
        "runtime/udp_dispatch/packet_path.rs",
        "runtime/udp_flow/packet_path_chain.rs",
        "runtime/udp_flow/packet_path_chain/entry.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly"
        );
    }
}

#[test]
fn udp_socket_helpers_use_runtime_services_instead_of_proxy() {
    for relative in [
        "runtime/udp_socket.rs",
        "runtime/udp_flow/packet_path_chain/carriers/udp_socket_carrier.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly"
        );
        assert!(
            !source.contains("&Proxy"),
            "{relative} must not carry raw Proxy references"
        );
    }

    let carrier = read(
        &proxy_src().join("runtime/udp_flow/packet_path_chain/carriers/udp_socket_carrier.rs"),
    );
    assert!(carrier.contains("UdpNetworkServices"));
}

#[test]
fn udp_delivery_helpers_use_runtime_services_instead_of_proxy() {
    let helpers = read_module(&proxy_src().join("runtime/udp_delivery/helpers.rs"));
    assert!(helpers.contains("UdpRuntimeServices"));
    assert!(!helpers.contains("use crate::runtime::Proxy"));
    assert!(!helpers.contains("&Proxy"));
}

#[test]
fn udp_delivery_helpers_root_stays_facade_only() {
    let helpers_root = read(&proxy_src().join("runtime/udp_delivery/helpers.rs"));
    let helpers = read_module(&proxy_src().join("runtime/udp_delivery/helpers.rs"));
    for module_name in [
        "mod accounting;",
        "mod lifecycle;",
        "mod parts;",
        "mod response;",
    ] {
        assert!(helpers_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct UdpInboundResponseAccounting",
        "pub(crate) fn log_completed_udp_flow",
        "pub(crate) fn record_direct_udp_response_parts",
        "pub(crate) fn record_chain_udp_response_parts",
        "pub(crate) fn record_upstream_udp_response_received",
    ] {
        assert!(
            !helpers_root.contains(forbidden),
            "udp_delivery helpers facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct UdpInboundResponseAccounting",
        "pub(crate) fn log_completed_udp_flow",
        "pub(crate) fn record_direct_udp_response_parts",
        "pub(crate) fn record_chain_udp_response_parts",
        "pub(crate) fn record_upstream_udp_response_received",
    ] {
        assert!(
            helpers.contains(expected),
            "udp_delivery helpers module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_ingress_runtime_collapses_proxy_and_services_for_session_loops() {
    let ingress = read_module(&proxy_src().join("runtime/udp_ingress.rs"));
    assert!(ingress.contains("struct UdpIngressRuntime"));
    assert!(ingress.contains("services: UdpRuntimeServices"));
    assert!(!ingress.contains("use crate::runtime::Proxy"));
    assert!(!ingress.contains("proxy: Proxy"));
    assert!(!ingress.contains("from_proxy("));

    let association = read_module(&proxy_src().join("runtime/udp_association/contract.rs"));
    assert!(association.contains("struct UdpAssociationDatagramRequest"));
    assert!(association.contains("runtime: &'a UdpIngressRuntime"));
    assert!(!association.contains("use crate::runtime::Proxy"));
    assert!(!association.contains("proxy: &Proxy"));
    assert!(!association.contains("services: &UdpRuntimeServices"));

    for relative in [
        "runtime/udp_association/lifecycle.rs",
        "runtime/datagram_udp/lifecycle.rs",
        "runtime/packet_session_udp/lifecycle.rs",
        "runtime/stream_udp.rs",
        "runtime/mux_udp.rs",
    ] {
        let source = read_module(&proxy_src().join(relative));
        assert!(
            source.contains("UdpIngressRuntime"),
            "{relative} must route inbound UDP through the shared ingress runtime context"
        );
        assert!(
            !source.contains("UdpRuntimeServices::from_proxy(proxy)"),
            "{relative} must not rebuild services inline from raw Proxy"
        );
        assert!(
            !source.contains("dispatch_inbound_udp_packet(proxy"),
            "{relative} must not call the raw inbound UDP helper directly"
        );
    }

    for relative in ["runtime/mux_session.rs", "runtime/mux_tcp.rs"] {
        let source = read_module(&proxy_src().join(relative));
        assert!(
            source.contains("MuxSubstreamRuntime"),
            "{relative} must route mux sub-stream tasks through the shared mux runtime context"
        );
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly for mux sub-stream orchestration"
        );
    }

    for relative in [
        "runtime/datagram_udp/lifecycle.rs",
        "runtime/packet_session_udp/lifecycle.rs",
        "runtime/stream_udp.rs",
        "runtime/mux_udp.rs",
        "runtime/mux_session.rs",
        "runtime/mux_tcp.rs",
    ] {
        let source = read_module(&proxy_src().join(relative));
        assert!(
            !source.contains("&Proxy"),
            "{relative} must not borrow raw Proxy references for outer UDP session loops"
        );
    }

    let route_runtime_root = read(&proxy_src().join("runtime/route_runtime.rs"));
    let route_runtime = read_module(&proxy_src().join("runtime/route_runtime.rs"));
    assert!(route_runtime_root.contains("mod listener;"));
    assert!(route_runtime_root.contains("mod route;"));
    assert!(route_runtime_root.contains("mod shared;"));
    assert!(!route_runtime_root.contains("struct InboundRouteRuntime"));
    assert!(!route_runtime_root.contains("struct InboundListenerRuntime"));
    assert!(!route_runtime_root.contains("struct SharedIngressRuntimeServices"));
    assert!(route_runtime.contains("struct InboundRouteRuntime"));
    assert!(route_runtime.contains("struct InboundListenerRuntime"));
    assert!(route_runtime.contains("struct SharedIngressRuntimeServices"));
    assert!(route_runtime.contains("TcpIngressRuntime"));
    assert!(route_runtime.contains("tcp_runtime: TcpIngressRuntime"));
    assert!(!route_runtime.contains("use crate::runtime::Proxy"));
    assert!(!route_runtime.contains("from_proxy("));
    assert!(!route_runtime.contains("fallback_proxy"));
    assert!(!route_runtime.contains("proxy: Proxy"));
    #[cfg(feature = "managed-stream-runtime")]
    assert!(route_runtime.contains("struct MuxSubstreamRuntime"));

    let inbound_operation = read_module(&proxy_src().join("runtime/inbound_operation.rs"));
    assert!(inbound_operation.contains("InboundListenerRuntime"));
    assert!(!inbound_operation.contains("proxy: Proxy"));
    assert!(!inbound_operation.contains("execute(proxy"));

    let inbound_context_root = read(&proxy_src().join("runtime/inbound_operation/context.rs"));
    let inbound_context = read_module(&proxy_src().join("runtime/inbound_operation/context.rs"));
    for module_name in [
        "mod model;",
        "mod no_client;",
        "mod recorded;",
        "mod serve;",
        "mod udp;",
    ] {
        assert!(inbound_context_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn run_udp_association",
        "pub(crate) async fn serve<P>(",
        "pub(crate) async fn dispatch_no_client_stream_route",
        "pub(crate) async fn dispatch_recorded_mux_tcp_route",
    ] {
        assert!(
            !inbound_context_root.contains(forbidden),
            "inbound_operation context facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn run_udp_association",
        "pub(crate) async fn serve<P>(",
        "pub(crate) async fn dispatch_no_client_stream_route",
        "pub(crate) async fn dispatch_recorded_mux_tcp_route",
    ] {
        assert!(
            inbound_context.contains(expected),
            "inbound_operation context module tree must still provide `{expected}`"
        );
    }

    let inventory_inbound = read(&proxy_src().join("inventory/inbound.rs"));
    assert!(inventory_inbound.contains("PreparedInboundListenerOperation"));
    assert!(!inventory_inbound.contains("InboundListenerRuntime"));
    assert!(!inventory_inbound.contains("JoinSet"));
    assert!(!inventory_inbound.contains("operation.execute("));
    assert!(!inventory_inbound.contains("use crate::runtime::Proxy"));
    assert!(!inventory_inbound.contains("execute(proxy.clone()"));

    let listener_inbound = read(&proxy_src().join("runtime/listeners/inbound.rs"));
    assert!(listener_inbound.contains("ProtocolInventory"));
    assert!(listener_inbound.contains("InboundListenerRuntimeFactory"));
    assert!(listener_inbound.contains("listeners.spawn("));
    assert!(listener_inbound.contains(".execute(listener_runtime, bound, shutdown_rx)"));
    assert!(!listener_inbound.contains("use super::super::Proxy"));
    assert!(!listener_inbound.contains("proxy: &Proxy"));
    assert!(!listener_inbound.contains("proxy.config.source_dir()"));

    let tcp_ingress_runtime = read_module(&proxy_src().join("runtime/tcp_ingress/runtime.rs"));
    assert!(tcp_ingress_runtime.contains("struct TcpIngressRuntime"));
    assert!(tcp_ingress_runtime.contains("serve_inbound("));
    assert!(!tcp_ingress_runtime.contains("use crate::runtime::Proxy"));
    assert!(!tcp_ingress_runtime.contains("proxy: Proxy"));

    let tcp_ingress_lifecycle = read_module(&proxy_src().join("runtime/tcp_ingress/lifecycle.rs"));
    assert!(tcp_ingress_lifecycle.contains("TcpIngressRuntime"));
    assert!(!tcp_ingress_lifecycle.contains("use crate::runtime::Proxy"));
    assert!(!tcp_ingress_lifecycle.contains("proxy: &Proxy"));

    let tcp_ingress_contract = read_module(&proxy_src().join("runtime/tcp_ingress/contract.rs"));
    assert!(tcp_ingress_contract.contains("TcpRuntimeServices"));
    assert!(!tcp_ingress_contract.contains("use crate::runtime::Proxy"));
    assert!(!tcp_ingress_contract.contains("proxy: &Proxy"));

    for relative in [
        "runtime/listener_loop/tcp.rs",
        "runtime/listener_loop/quic.rs",
        "runtime/listener_loop/system.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly"
        );
        assert!(
            !source.contains("proxy: &'a Proxy"),
            "{relative} must not retain raw Proxy in listener-loop request models"
        );
        assert!(
            !source.contains("Fn(Proxy"),
            "{relative} must not expose raw Proxy in listener-loop handler contracts"
        );
        assert!(
            !source.contains("FnMut(Proxy"),
            "{relative} must not expose raw Proxy in listener-loop handler contracts"
        );
    }

    for relative in [
        "runtime/inbound_route/stream/dispatch.rs",
        "runtime/inbound_route/mux/dispatch.rs",
        "runtime/mux_session.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("FnOnce(Proxy"),
            "{relative} must not expose raw Proxy in generic route or mux closure contracts"
        );
        assert!(
            !source.contains("FnMut(Proxy"),
            "{relative} must not expose raw Proxy in generic route or mux closure contracts"
        );
    }

    for relative in [
        "runtime/udp_dispatch/dispatch.rs",
        "runtime/udp_dispatch/forward.rs",
        "runtime/udp_dispatch/managed/forward.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly"
        );
        assert!(
            !source.contains("&Proxy"),
            "{relative} must not carry raw Proxy references"
        );
        assert!(
            !source.contains("UdpRuntimeServices::from_proxy(proxy)"),
            "{relative} must not rebuild services inline from raw Proxy"
        );
    }

    let pipe = read_module(&proxy_src().join("runtime/pipe.rs"));
    assert!(pipe.contains("pub(crate) struct UdpPipe<'a> {\n    dispatch: &'a mut UdpDispatch,"));
    assert!(pipe.contains("pub(crate) fn new(dispatch: &'a mut UdpDispatch)"));
    assert!(pipe.contains("Self { dispatch }"));
    assert!(!pipe.contains("UdpDispatch::dispatch(self.dispatch, self.proxy, input)"));
    assert!(pipe.contains("UdpDispatch::dispatch(self.dispatch, input)"));
}

#[test]
fn pipe_root_stays_facade_only() {
    let pipe_root = read(&proxy_src().join("runtime/pipe.rs"));
    let pipe = read_module(&proxy_src().join("runtime/pipe.rs"));
    for module_name in ["mod contract;", "mod tcp;", "mod udp;"] {
        assert!(pipe_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) trait KernelPipe",
        "pub(crate) struct TcpPipe<'a>",
        "pub(crate) struct UdpPipe<'a>",
        "pub(crate) struct UdpPipeInput<'a>",
    ] {
        assert!(
            !pipe_root.contains(forbidden),
            "pipe facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) trait KernelPipe",
        "pub(crate) struct TcpPipe<'a>",
        "pub(crate) struct UdpPipe<'a>",
        "pub(crate) struct UdpPipeInput<'a>",
    ] {
        assert!(
            pipe.contains(expected),
            "pipe module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn listener_loop_quic_root_stays_facade_only() {
    let quic_root = read(&proxy_src().join("runtime/listener_loop/quic.rs"));
    let quic = read_module(&proxy_src().join("runtime/listener_loop/quic.rs"));
    for module_name in ["mod connection;", "mod logged;", "mod stream;"] {
        assert!(quic_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct QuicListenerLoopRequest<H>",
        "pub(crate) struct QuicStreamListenerLoopRequest<H>",
        "pub(crate) struct LoggedQuicStreamListenerRequest<R, D>",
        "pub(crate) async fn run_quic_listener_loop<H, Fut>(",
        "pub(crate) async fn run_logged_quic_stream_listener_loop<R, D, Fut>(",
    ] {
        assert!(
            !quic_root.contains(forbidden),
            "listener_loop quic facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct QuicListenerLoopRequest<H>",
        "pub(crate) struct QuicStreamListenerLoopRequest<H>",
        "pub(crate) struct LoggedQuicStreamListenerRequest<R, D>",
        "pub(crate) async fn run_quic_listener_loop<H, Fut>(",
        "pub(crate) async fn run_logged_quic_stream_listener_loop<R, D, Fut>(",
    ] {
        assert!(
            quic.contains(expected),
            "listener_loop quic module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn listener_loop_tcp_root_stays_facade_only() {
    let tcp_root = read(&proxy_src().join("runtime/listener_loop/tcp.rs"));
    let tcp = read_module(&proxy_src().join("runtime/listener_loop/tcp.rs"));
    for module_name in ["mod connection;", "mod logged;"] {
        assert!(tcp_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct TcpListenerLoopRequest<H>",
        "pub(crate) struct LoggedTcpSocketListenerRequest<R, D>",
        "pub(crate) async fn run_tcp_listener_loop<H, Fut>(",
        "pub(crate) async fn run_logged_tcp_socket_listener_loop<R, D, Fut>(",
    ] {
        assert!(
            !tcp_root.contains(forbidden),
            "listener_loop tcp facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct TcpListenerLoopRequest<H>",
        "pub(crate) struct LoggedTcpSocketListenerRequest<R, D>",
        "pub(crate) async fn run_tcp_listener_loop<H, Fut>(",
        "pub(crate) async fn run_logged_tcp_socket_listener_loop<R, D, Fut>(",
    ] {
        assert!(
            tcp.contains(expected),
            "listener_loop tcp module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_ingress_root_stays_facade_only() {
    let ingress_root = read(&proxy_src().join("runtime/udp_ingress.rs"));
    let ingress = read_module(&proxy_src().join("runtime/udp_ingress.rs"));
    for module_name in ["mod dispatch;", "mod model;", "mod route;", "mod session;"] {
        assert!(ingress_root.contains(module_name));
    }
    for forbidden in [
        "struct UdpIngressRuntime",
        "pub(crate) async fn new_dispatch",
        "pub(crate) async fn route_decision",
        "pub(crate) fn prepare_udp_session",
        "pub(crate) async fn dispatch_inbound_packet",
    ] {
        assert!(
            !ingress_root.contains(forbidden),
            "udp_ingress facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct UdpIngressRuntime",
        "pub(crate) async fn new_dispatch",
        "pub(crate) async fn route_decision",
        "pub(crate) fn prepare_udp_session",
        "pub(crate) async fn dispatch_inbound_packet",
    ] {
        assert!(
            ingress.contains(expected),
            "udp_ingress module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn tcp_ingress_runtime_root_stays_facade_only() {
    let runtime_root = read(&proxy_src().join("runtime/tcp_ingress/runtime.rs"));
    let runtime = read_module(&proxy_src().join("runtime/tcp_ingress/runtime.rs"));
    for module_name in ["mod model;", "mod route;", "mod serve;", "mod session;"] {
        assert!(runtime_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct TcpIngressRuntime",
        "pub(crate) async fn serve<P>(",
        "pub(crate) async fn route_decision",
        "pub(crate) fn prepare_session",
        "pub(crate) async fn open_tcp_upstream",
    ] {
        assert!(
            !runtime_root.contains(forbidden),
            "tcp_ingress runtime facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct TcpIngressRuntime",
        "pub(crate) async fn serve<P>(",
        "pub(crate) async fn route_decision",
        "pub(crate) fn prepare_session",
        "pub(crate) async fn open_tcp_upstream",
    ] {
        assert!(
            runtime.contains(expected),
            "tcp_ingress runtime module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn tcp_ingress_lifecycle_root_stays_facade_only() {
    let lifecycle_root = read(&proxy_src().join("runtime/tcp_ingress/lifecycle.rs"));
    let lifecycle = read_module(&proxy_src().join("runtime/tcp_ingress/lifecycle.rs"));
    for module_name in ["mod rate_limit;", "mod result;", "mod serve;"] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn serve_inbound<",
        "pub(crate) async fn serve_inbound_with_client_response",
        "pub(crate) fn apply_kernel_rate_limits_from_config",
        "tokio::time::timeout(",
        "fn finish_relay_success(",
    ] {
        assert!(
            !lifecycle_root.contains(forbidden),
            "tcp_ingress lifecycle facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn serve_inbound<",
        "pub(crate) fn apply_kernel_rate_limits_from_config",
        "tokio::time::timeout(",
        "fn finish_relay_success(",
        "fn finish_route_or_establish_failure(",
    ] {
        assert!(
            lifecycle.contains(expected),
            "tcp_ingress lifecycle module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn tcp_ingress_contract_root_stays_facade_only() {
    let contract_root = read(&proxy_src().join("runtime/tcp_ingress/contract.rs"));
    let contract = read_module(&proxy_src().join("runtime/tcp_ingress/contract.rs"));
    for module_name in [
        "mod accounting;",
        "mod client_response;",
        "mod no_response;",
        "mod protocol;",
    ] {
        assert!(contract_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) fn record_tcp_upload",
        "pub(crate) trait InboundProtocol",
        "pub(crate) struct ClientResponseInboundProtocol",
        "pub(crate) struct NoClientResponseStreamProtocol",
    ] {
        assert!(
            !contract_root.contains(forbidden),
            "tcp_ingress contract facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "fn record_tcp_upload(",
        "pub(crate) trait InboundProtocol",
        "pub(crate) struct ClientResponseInboundProtocol",
        "pub(crate) struct NoClientResponseStreamProtocol",
        "TcpRuntimeServices",
    ] {
        assert!(
            contract.contains(expected),
            "tcp_ingress contract module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn orchestration_root_stays_facade_only() {
    let orchestration_root = read(&proxy_src().join("runtime/orchestration.rs"));
    let orchestration = read_module(&proxy_src().join("runtime/orchestration.rs"));
    for module_name in ["mod lifecycle;", "mod logging;", "mod state;"] {
        assert!(orchestration_root.contains(module_name));
    }
    for forbidden in [
        "pub(super) async fn run_until<",
        "pub(in crate::runtime) async fn run_until<",
        "struct OrchestrationState",
        "fn log_started(",
        "fn log_stopped(",
        "tokio::select!",
    ] {
        assert!(
            !orchestration_root.contains(forbidden),
            "orchestration facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(in crate::runtime) async fn run_until<",
        "pub(super) struct OrchestrationState",
        "pub(super) fn log_started(",
        "pub(super) fn log_stopped(",
        "tokio::select! {",
    ] {
        assert!(
            orchestration.contains(expected),
            "orchestration module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn inventory_tcp_relay_root_is_not_a_proxy_impl_bucket() {
    let relay = read(&proxy_src().join("inventory/tcp/relay.rs"));
    assert!(!relay.contains("impl Proxy"));
    assert!(!relay.contains("use crate::runtime::Proxy"));
    assert!(!relay.contains("&Proxy"));
}

#[test]
fn inventory_tcp_leaf_stays_adapter_facing() {
    let leaf = read(&proxy_src().join("inventory/tcp/leaf.rs"));
    assert!(!leaf.contains("impl Proxy"));
    assert!(!leaf.contains("use crate::runtime::Proxy"));
    assert!(!leaf.contains("&Proxy"));
    assert!(!leaf.contains("pub(crate) fn prepare_tcp_candidate<'a>("));
    assert!(!leaf.contains("pub(crate) fn prepare_tcp_relay_hop<'a>("));
    assert!(!leaf.contains("find_outbound_leaf("));
    assert!(!leaf.contains("TcpOutboundCapability"));
}

#[test]
fn inventory_tcp_dispatch_root_is_not_a_proxy_impl_bucket() {
    let dispatch = read(&proxy_src().join("inventory/tcp/outbound.rs"));
    assert!(!dispatch.contains("impl Proxy"));
}

#[test]
fn route_runtime_route_root_stays_facade_only() {
    let route_root = read(&proxy_src().join("runtime/route_runtime/route.rs"));
    let route = read_module(&proxy_src().join("runtime/route_runtime/route.rs"));
    for module_name in ["mod access;", "mod model;", "mod serve;"] {
        assert!(route_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct InboundRouteRuntime",
        "pub(crate) struct InboundRouteRuntimeFactory",
        "pub(crate) async fn serve<P>(",
        "pub(crate) fn for_connection(&self, source_addr: Option<SocketAddr>)",
    ] {
        assert!(
            !route_root.contains(forbidden),
            "route runtime route facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct InboundRouteRuntime",
        "pub(crate) struct InboundRouteRuntimeFactory",
        "pub(crate) async fn serve<P>(",
        "pub(crate) fn for_connection(&self, source_addr: Option<SocketAddr>)",
    ] {
        assert!(
            route.contains(expected),
            "route runtime route module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn handle_command_root_stays_facade_only() {
    let command_root = read(&proxy_src().join("runtime/handle/command.rs"));
    let command = read_module(&proxy_src().join("runtime/handle/command.rs"));
    for module_name in [
        "mod diagnostics;",
        "mod dispatch;",
        "mod runtime;",
        "mod tun;",
    ] {
        assert!(command_root.contains(module_name));
    }
    for forbidden in [
        "impl zero_api::CommandService for ProxyHandle",
        "CommandRequest::TunStart",
        "CommandRequest::DiagnosticsProbeOutbound",
        "CommandRequest::DiagnosticsDnsCache",
        "CommandRequest::DiagnosticsFakeipLookup",
    ] {
        assert!(
            !command_root.contains(forbidden),
            "handle command facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "impl zero_api::CommandService for ProxyHandle",
        "CommandRequest::TunStart",
        "CommandRequest::DiagnosticsProbeOutbound",
        "CommandRequest::DiagnosticsDnsCache",
        "CommandRequest::DiagnosticsFakeipLookup",
    ] {
        assert!(
            command.contains(expected),
            "handle command module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_outbound_root_stays_facade_only() {
    let outbound_root = read(&proxy_src().join("runtime/udp_flow/outbound.rs"));
    let outbound = read_module(&proxy_src().join("runtime/udp_flow/outbound.rs"));
    assert!(outbound_root.contains("mod model;"));
    assert!(outbound_root.contains("mod projection;"));
    assert!(!outbound_root.contains("pub(crate) fn tag(&self)"));
    assert!(!outbound_root.contains("fn upstream_endpoint(&self)"));
    assert!(outbound.contains("pub(crate) enum UdpFlowOutbound"));
    assert!(outbound.contains("pub(in crate::runtime::udp_flow) fn completion(&self)"));
}

#[test]
fn udp_flow_outbound_projection_root_stays_facade_only() {
    let projection_root = read(&proxy_src().join("runtime/udp_flow/outbound/projection.rs"));
    let projection = read_module(&proxy_src().join("runtime/udp_flow/outbound/projection.rs"));
    for module_name in ["mod access;", "mod completion;", "mod index;"] {
        assert!(projection_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) fn tag(&self)",
        "pub(crate) fn upstream(&self)",
        "pub(in crate::runtime::udp_flow) fn index_keys(&self)",
        "fn success_outcome(&self)",
        "pub(in crate::runtime::udp_flow) fn completion(&self)",
    ] {
        assert!(
            !projection_root.contains(forbidden),
            "udp_flow outbound projection facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) fn tag(&self)",
        "pub(crate) fn upstream(&self)",
        "pub(in crate::runtime::udp_flow) fn index_keys(&self)",
        "fn success_outcome(&self)",
        "pub(in crate::runtime::udp_flow) fn completion(&self)",
    ] {
        assert!(
            projection.contains(expected),
            "udp_flow outbound projection module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_outbound_projection_access_root_stays_facade_only() {
    let access_root = read(&proxy_src().join("runtime/udp_flow/outbound/projection/access.rs"));
    let access = read_module(&proxy_src().join("runtime/udp_flow/outbound/projection/access.rs"));
    for module_name in ["mod identity;", "mod managed;", "mod upstream;"] {
        assert!(access_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) fn tag(&self) -> &str",
        "pub(crate) fn path_category(&self) -> UdpPathCategory",
        "pub(crate) fn managed_flow(&self) -> Option<ManagedUdpFlowRef>",
        "pub(crate) fn upstream(&self) -> Option<UdpFlowUpstream<'_>>",
    ] {
        assert!(
            !access_root.contains(forbidden),
            "udp_flow outbound projection access facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) fn tag(&self) -> &str",
        "pub(crate) fn path_category(&self) -> UdpPathCategory",
        "pub(crate) fn managed_flow(&self) -> Option<ManagedUdpFlowRef>",
        "pub(crate) fn upstream(&self) -> Option<UdpFlowUpstream<'_>>",
    ] {
        assert!(
            access.contains(expected),
            "udp_flow outbound projection access module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_packet_path_root_stays_facade_only() {
    let packet_path_root = read(&proxy_src().join("runtime/udp_flow/packet_path.rs"));
    let packet_path = read_module(&proxy_src().join("runtime/udp_flow/packet_path.rs"));
    for module_name in [
        "mod carrier;",
        "mod context;",
        "mod datagram;",
        "mod snapshot;",
    ] {
        assert!(packet_path_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) type ChainTask",
        "pub(crate) trait PacketPathCarrier",
        "pub(crate) struct UdpDatagramSource",
        "pub(crate) struct PacketPathFlowBinding",
    ] {
        assert!(
            !packet_path_root.contains(forbidden),
            "packet_path facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) trait PacketPathCarrier",
        "pub(crate) struct UdpDatagramSource",
        "pub(crate) struct PacketPathFlowBinding",
    ] {
        assert!(
            packet_path.contains(expected),
            "packet_path module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_packet_path_datagram_root_stays_facade_only() {
    let datagram_root = read(&proxy_src().join("runtime/udp_flow/packet_path/datagram.rs"));
    let datagram = read_module(&proxy_src().join("runtime/udp_flow/packet_path/datagram.rs"));
    for module_name in ["mod access;", "mod build;", "mod model;"] {
        assert!(datagram_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct UdpDatagramDescriptor",
        "pub(crate) struct UdpDatagramSource",
        "pub(crate) fn udp_datagram_source(",
        "pub(crate) fn target(&self) -> Address",
    ] {
        assert!(
            !datagram_root.contains(forbidden),
            "udp_flow packet_path datagram facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct UdpDatagramDescriptor",
        "pub(crate) struct UdpDatagramSource",
        "pub(crate) fn udp_datagram_source(",
        "pub(crate) fn target(&self) -> Address",
    ] {
        assert!(
            datagram.contains(expected),
            "udp_flow packet_path datagram module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_packet_path_chain_root_stays_facade_only() {
    let chain_root = read(&proxy_src().join("runtime/udp_flow/packet_path_chain.rs"));
    let chain = read_module(&proxy_src().join("runtime/udp_flow/packet_path_chain.rs"));
    for module_name in [
        "mod bridge;",
        "pub(crate) mod carriers;",
        "mod entry;",
        "mod key;",
        "mod model;",
        "mod snapshot;",
        "mod start;",
        "mod state;",
    ] {
        assert!(chain_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct PacketPathManager",
        "pub(crate) fn new(",
        "pub(crate) async fn send(",
        "pub(crate) async fn send_with_snapshot(",
        "async fn ensure_entry(",
        "pub(crate) struct SendWithSnapshotRequest",
    ] {
        assert!(
            !chain_root.contains(forbidden),
            "udp_flow packet_path_chain facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct PacketPathManager",
        "pub(crate) fn new(",
        "pub(crate) async fn send(",
        "pub(crate) async fn send_with_snapshot(",
        "async fn ensure_entry(",
        "pub(crate) struct SendWithSnapshotRequest",
    ] {
        assert!(
            chain.contains(expected),
            "udp_flow packet_path_chain module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_packet_path_chain_bridge_root_stays_facade_only() {
    let bridge_root = read(&proxy_src().join("runtime/udp_flow/packet_path_chain/bridge.rs"));
    let bridge = read_module(&proxy_src().join("runtime/udp_flow/packet_path_chain/bridge.rs"));
    for module_name in ["mod dispatch;", "mod recv;", "mod waiter;"] {
        assert!(bridge_root.contains(module_name));
    }
    for forbidden in [
        "type RecvItem =",
        "pub(super) struct Waiter",
        "pub(super) async fn dispatch_via_entry(",
        "pub(super) async fn recv_loop(",
        "fn remove_waiter(",
    ] {
        assert!(
            !bridge_root.contains(forbidden),
            "udp_flow packet_path_chain bridge facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "type RecvItem =",
        "struct Waiter",
        "async fn dispatch_via_entry(",
        "async fn recv_loop(",
        "fn remove_waiter(",
    ] {
        assert!(
            bridge.contains(expected),
            "udp_flow packet_path_chain bridge module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_dispatch_forward_root_stays_facade_only() {
    let forward_root = read(&proxy_src().join("runtime/udp_dispatch/forward.rs"));
    let forward = read_module(&proxy_src().join("runtime/udp_dispatch/forward.rs"));
    for module_name in ["mod path;", "mod result;"] {
        assert!(forward_root.contains(module_name));
    }
    for forbidden in [
        "pub(in crate::runtime::udp_dispatch) async fn forward_existing(",
        "fn fail_flow_with_msg(",
        "fn record_or_fail(",
    ] {
        assert!(
            !forward_root.contains(forbidden),
            "udp_dispatch forward facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(in crate::runtime::udp_dispatch) async fn forward_existing(",
        "pub(super) fn fail_flow_with_msg(",
        "pub(super) fn record_or_fail(",
    ] {
        assert!(
            forward.contains(expected),
            "udp_dispatch forward module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_dispatch_root_stays_facade_only() {
    let dispatch_root = read(&proxy_src().join("runtime/udp_dispatch/mod.rs"));
    let dispatch = read_module(&proxy_src().join("runtime/udp_dispatch"));
    for module_name in ["mod failure;", "mod model;"] {
        assert!(dispatch_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct UdpDispatch",
        "fn fail_flow(",
        "log_session_failed",
    ] {
        assert!(
            !dispatch_root.contains(forbidden),
            "udp_dispatch facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in ["pub(crate) struct UdpDispatch", "pub(super) fn fail_flow("] {
        assert!(
            dispatch.contains(expected),
            "udp_dispatch module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_dispatch_lifecycle_root_stays_facade_only() {
    let lifecycle_root = read(&proxy_src().join("runtime/udp_dispatch/lifecycle.rs"));
    let lifecycle = read_module(&proxy_src().join("runtime/udp_dispatch/lifecycle.rs"));
    for module_name in ["mod lookup;", "mod poll;", "mod setup;"] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct UpstreamAssociationView<'a>",
        "pub(crate) struct ClosedUpstreamAssociation",
        "pub(crate) async fn new(",
        "pub(crate) fn poll_refs(",
        "pub(crate) fn finish_all(mut self)",
    ] {
        assert!(
            !lifecycle_root.contains(forbidden),
            "udp_dispatch lifecycle facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct UpstreamAssociationView<'a>",
        "pub(crate) struct ClosedUpstreamAssociation",
        "pub(crate) async fn new(",
        "pub(crate) fn poll_refs(",
        "pub(crate) fn finish_all(mut self)",
    ] {
        assert!(
            lifecycle.contains(expected),
            "udp_dispatch lifecycle module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_dispatch_managed_start_root_stays_facade_only() {
    let start_root = read(&proxy_src().join("runtime/udp_dispatch/managed/start.rs"));
    let start = read_module(&proxy_src().join("runtime/udp_dispatch/managed/start.rs"));
    for module_name in [
        "mod context;",
        "mod datagram;",
        "mod send;",
        "mod upstream;",
    ] {
        assert!(start_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) fn flow_start_context(&mut self)",
        "pub(in crate::runtime::udp_dispatch::managed) async fn send_managed_udp(",
        "pub(crate) async fn start_tracked_managed_datagram",
        "pub(crate) async fn start_tracked_upstream",
    ] {
        assert!(
            !start_root.contains(forbidden),
            "udp_dispatch managed start facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) fn flow_start_context(&mut self)",
        "pub(in crate::runtime::udp_dispatch::managed) async fn send_managed_udp(",
        "pub(crate) async fn start_tracked_managed_datagram",
        "pub(crate) async fn start_tracked_upstream",
    ] {
        assert!(
            start.contains(expected),
            "udp_dispatch managed start module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn mux_session_root_stays_facade_only() {
    let mux_session_root = read(&proxy_src().join("runtime/mux_session.rs"));
    let mux_session = read_module(&proxy_src().join("runtime/mux_session.rs"));
    for module_name in ["mod lifecycle;", "mod model;", "mod protocol;"] {
        assert!(mux_session_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct MuxSessionLoop",
        "pub(crate) trait MuxOpenedDispatcher",
        "pub(crate) async fn run_mux_session_loop",
        "pub(crate) fn drain_completed_mux_tasks",
        "pub(crate) async fn run_protocol_mux_session",
    ] {
        assert!(
            !mux_session_root.contains(forbidden),
            "mux_session facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct MuxSessionLoop",
        "pub(crate) trait MuxOpenedDispatcher",
        "pub(crate) async fn run_mux_session_loop",
        "pub(crate) fn drain_completed_mux_tasks",
        "pub(crate) async fn run_protocol_mux_session",
    ] {
        assert!(
            mux_session.contains(expected),
            "mux_session module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn mux_udp_root_stays_facade_only() {
    let mux_udp_root = read(&proxy_src().join("runtime/mux_udp.rs"));
    let mux_udp = read_module(&proxy_src().join("runtime/mux_udp.rs"));
    for module_name in ["mod handler;", "mod relay;", "mod task;"] {
        assert!(mux_udp_root.contains(module_name));
    }
    for forbidden in [
        "struct MuxPacketSessionUdpHandler",
        "pub(crate) async fn run_protocol_mux_udp_relay",
        "pub(crate) async fn run_protocol_mux_udp_task",
        "pub(crate) async fn run_protocol_mux_udp_task_with_accept_log",
    ] {
        assert!(
            !mux_udp_root.contains(forbidden),
            "mux_udp facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(super) struct MuxPacketSessionUdpHandler",
        "pub(crate) async fn run_protocol_mux_udp_relay",
        "pub(crate) async fn run_protocol_mux_udp_task",
        "pub(crate) async fn run_protocol_mux_udp_task_with_accept_log",
    ] {
        assert!(
            mux_udp.contains(expected),
            "mux_udp module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn no_client_mux_root_stays_facade_only() {
    let no_client_root = read(&proxy_src().join("runtime/inbound_route/mux/no_client.rs"));
    let no_client = read_module(&proxy_src().join("runtime/inbound_route/mux/no_client.rs"));
    for module_name in ["mod request;", "mod route;"] {
        assert!(no_client_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn dispatch_no_client_mux_route<",
        "pub(crate) async fn dispatch_no_client_mux_route_with_defaults<",
        "pub(crate) async fn dispatch_no_client_mux_route_request_with_defaults<",
    ] {
        assert!(
            !no_client_root.contains(forbidden),
            "no_client mux facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn dispatch_no_client_mux_route<",
        "pub(crate) async fn dispatch_no_client_mux_route_with_defaults<",
        "pub(crate) async fn dispatch_no_client_mux_route_request_with_defaults<",
    ] {
        assert!(
            no_client.contains(expected),
            "no_client mux module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn recorded_dispatch_root_stays_facade_only() {
    let dispatch_root = read(&proxy_src().join("runtime/inbound_route/recorded/dispatch.rs"));
    let dispatch = read_module(&proxy_src().join("runtime/inbound_route/recorded/dispatch.rs"));
    for module_name in ["mod accept;", "mod route;"] {
        assert!(dispatch_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn dispatch_recorded_protocol_mux_route<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_route_accept_result<",
        "pub(crate) async fn dispatch_optional_recorded_protocol_mux_route_accept_result<",
    ] {
        assert!(
            !dispatch_root.contains(forbidden),
            "recorded dispatch facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn dispatch_recorded_protocol_mux_route<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_route_accept_result<",
        "pub(crate) async fn dispatch_optional_recorded_protocol_mux_route_accept_result<",
    ] {
        assert!(
            dispatch.contains(expected),
            "recorded dispatch module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn recorded_request_root_stays_facade_only() {
    let request_root = read(&proxy_src().join("runtime/inbound_route/recorded/request.rs"));
    let request = read_module(&proxy_src().join("runtime/inbound_route/recorded/request.rs"));
    for module_name in ["mod defaults;", "mod result;"] {
        assert!(request_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn dispatch_recorded_protocol_mux_tcp_request_result<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_stream_request_result<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_tcp_request_with_defaults<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_stream_request_with_defaults<",
    ] {
        assert!(
            !request_root.contains(forbidden),
            "recorded request facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn dispatch_recorded_protocol_mux_tcp_request_result<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_stream_request_result<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_tcp_request_with_defaults<",
        "pub(crate) async fn dispatch_recorded_protocol_mux_stream_request_with_defaults<",
    ] {
        assert!(
            request.contains(expected),
            "recorded request module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn stream_udp_root_stays_facade_only() {
    let stream_udp_root = read(&proxy_src().join("runtime/stream_udp.rs"));
    let stream_udp = read_module(&proxy_src().join("runtime/stream_udp.rs"));
    for module_name in ["mod handler;", "mod recording;", "mod relay;"] {
        assert!(stream_udp_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct StreamUdpRelayRequest",
        "struct StreamPacketSessionUdpHandler",
        "pub(crate) async fn run_mapped_protocol_stream_udp_relay",
        "async fn run_stream_udp_relay",
        "fn record_stream_udp_client_io",
    ] {
        assert!(
            !stream_udp_root.contains(forbidden),
            "stream_udp facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct StreamUdpRelayRequest",
        "pub(super) struct StreamPacketSessionUdpHandler",
        "pub(crate) async fn run_mapped_protocol_stream_udp_relay",
        "async fn run_stream_udp_relay",
        "pub(super) fn record_stream_udp_client_io",
    ] {
        assert!(
            stream_udp.contains(expected),
            "stream_udp module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_sessions_root_stays_facade_only() {
    let sessions_root = read(&proxy_src().join("runtime/udp_flow/sessions.rs"));
    let sessions = read_module(&proxy_src().join("runtime/udp_flow/sessions.rs"));
    for module_name in ["mod index;", "mod lifecycle;", "mod model;"] {
        assert!(sessions_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct UdpSessionFlows",
        "struct UdpFlowKey",
        "fn index_flow(",
        "pub(crate) fn finish_all(&mut self)",
    ] {
        assert!(
            !sessions_root.contains(forbidden),
            "udp_flow sessions facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct UdpSessionFlows",
        "pub(crate) struct CompletedUdpFlow",
        "pub(crate) fn finish_all(&mut self)",
        "pub(crate) fn direct_response_session_id(&self, sender: SocketAddr)",
    ] {
        assert!(
            sessions.contains(expected),
            "udp_flow sessions module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn packet_session_udp_lifecycle_root_stays_facade_only() {
    let lifecycle_root = read(&proxy_src().join("runtime/packet_session_udp/lifecycle.rs"));
    let lifecycle = read_module(&proxy_src().join("runtime/packet_session_udp/lifecycle.rs"));
    for module_name in ["mod failure;", "mod read;", "mod relay;", "mod response;"] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn run_packet_session_udp_relay",
        "async fn process_packet_session_read",
        "async fn handle_runtime_failure",
        "async fn handle_direct_response",
        "tokio::select!",
    ] {
        assert!(
            !lifecycle_root.contains(forbidden),
            "packet_session_udp lifecycle facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn run_packet_session_udp_relay",
        "async fn process_packet_session_read",
        "async fn handle_runtime_failure",
        "async fn handle_direct_response",
        "select! {",
    ] {
        assert!(
            lifecycle.contains(expected),
            "packet_session_udp lifecycle module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn datagram_udp_lifecycle_root_stays_facade_only() {
    let lifecycle_root = read(&proxy_src().join("runtime/datagram_udp/lifecycle.rs"));
    let lifecycle = read_module(&proxy_src().join("runtime/datagram_udp/lifecycle.rs"));
    for module_name in [
        "mod read;",
        "mod relay;",
        "mod response;",
        "mod without_upstream;",
    ] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn run_protocol_datagram_udp_relay",
        "async fn process_datagram_read",
        "type ChainUdpResponseResult",
        "tokio::select!",
    ] {
        assert!(
            !lifecycle_root.contains(forbidden),
            "datagram_udp lifecycle facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn run_protocol_datagram_udp_relay",
        "async fn process_datagram_read",
        "type ChainUdpResponseResult",
        "select! {",
    ] {
        assert!(
            lifecycle.contains(expected),
            "datagram_udp lifecycle module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_association_contract_root_stays_facade_only() {
    let contract_root = read(&proxy_src().join("runtime/udp_association/contract.rs"));
    let contract = read_module(&proxy_src().join("runtime/udp_association/contract.rs"));
    for module_name in [
        "mod dispatch;",
        "mod handler;",
        "mod model;",
        "mod response;",
    ] {
        assert!(contract_root.contains(module_name));
    }
    for forbidden in [
        "enum UdpAssociationDispatchOutcome",
        "struct UdpAssociationDispatchBridge",
        "trait UdpAssociationHandler",
        "async fn write_target_response",
        "async fn send_association_response",
    ] {
        assert!(
            !contract_root.contains(forbidden),
            "udp_association contract facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "enum UdpAssociationDispatchOutcome",
        "struct UdpAssociationDispatchBridge",
        "trait UdpAssociationHandler",
        "async fn write_target_response",
        "async fn send_association_response",
    ] {
        assert!(
            contract.contains(expected),
            "udp_association contract module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_association_lifecycle_root_stays_facade_only() {
    let lifecycle_root = read(&proxy_src().join("runtime/udp_association/lifecycle.rs"));
    let lifecycle = read_module(&proxy_src().join("runtime/udp_association/lifecycle.rs"));
    for module_name in ["mod idle;", "mod relay;", "mod response;"] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn run_udp_association_loop",
        "fn handle_idle_timeout(",
        "async fn handle_upstream_response",
        "async fn handle_chain_result",
        "select! {",
    ] {
        assert!(
            !lifecycle_root.contains(forbidden),
            "udp_association lifecycle facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn run_udp_association_loop",
        "pub(super) fn finish_dispatch(",
        "pub(super) async fn handle_upstream_response",
        "pub(super) async fn handle_chain_result",
        "select! {",
    ] {
        assert!(
            lifecycle.contains(expected),
            "udp_association lifecycle module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_state_root_stays_facade_only() {
    let state_root = read(&proxy_src().join("runtime/udp_flow/state.rs"));
    let state = read_module(&proxy_src().join("runtime/udp_flow/state.rs"));
    for module_name in [
        "mod context;",
        "mod lifecycle;",
        "mod managed;",
        "mod model;",
        "mod packet_path;",
    ] {
        assert!(state_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct UdpFlowState",
        "pub(crate) struct UdpFlowStartContext",
        "pub(crate) async fn start_managed_flow(",
        "pub(crate) async fn send_packet_path_chain(",
    ] {
        assert!(
            !state_root.contains(forbidden),
            "udp_flow state facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct UdpFlowState",
        "pub(crate) struct UdpFlowStartContext",
        "pub(crate) async fn start_managed_flow(",
        "pub(crate) async fn send_packet_path_chain(",
    ] {
        assert!(
            state.contains(expected),
            "udp_flow state module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_root_stays_facade_only() {
    let managed_root = read(&proxy_src().join("runtime/udp_flow/managed/mod.rs"));
    let managed = read_module(&proxy_src().join("runtime/udp_flow/managed"));
    for module_name in [
        "pub(crate) mod bridge;",
        "mod cache;",
        "mod connection;",
        "mod datagram;",
        "pub(crate) mod datagram_manager;",
        "mod flow;",
        "pub(crate) mod model;",
        "pub(crate) mod state;",
        "mod stream;",
        "pub(crate) mod stream_manager;",
    ] {
        assert!(managed_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct ManagedUdpFlowResume",
        "pub(crate) struct ManagedUdpHandlers",
        "pub(crate) struct ManagedUdpState",
        "pub(crate) trait ManagedDatagramFlowHandler",
        "pub(crate) struct ManagedStreamHandlerPair",
    ] {
        assert!(
            !managed_root.contains(forbidden),
            "udp_flow managed facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct ManagedUdpFlowResume",
        "pub(crate) struct ManagedUdpHandlers",
        "pub(crate) struct ManagedUdpState",
        "pub(crate) trait ManagedDatagramFlowHandler",
        "pub(crate) struct ManagedStreamHandlerPair",
    ] {
        assert!(
            managed.contains(expected),
            "udp_flow managed module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_cache_stream_insert_root_stays_facade_only() {
    let insert_root = read(&proxy_src().join("runtime/udp_flow/managed/cache/stream/insert.rs"));
    let insert = read_module(&proxy_src().join("runtime/udp_flow/managed/cache/stream/insert.rs"));
    for module_name in ["mod establish;", "mod pre_sent;", "mod relay;"] {
        assert!(insert_root.contains(module_name));
    }
    for forbidden in [
        "async fn send_or_insert_pre_sent<",
        "pub(crate) async fn send_or_insert_pre_sent_key<",
        "async fn send_or_insert<",
        "pub(crate) async fn send_or_insert_key<",
        "async fn insert_and_send(",
        "pub(crate) async fn insert_and_send_key(",
    ] {
        assert!(
            !insert_root.contains(forbidden),
            "udp_flow managed cache stream insert facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "async fn send_or_insert_pre_sent<",
        "pub(crate) async fn send_or_insert_pre_sent_key<",
        "async fn send_or_insert<",
        "pub(crate) async fn send_or_insert_key<",
        "async fn insert_and_send(",
        "pub(crate) async fn insert_and_send_key(",
    ] {
        assert!(
            insert.contains(expected),
            "udp_flow managed cache stream insert module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_flow_request_root_stays_facade_only() {
    let request_root = read(&proxy_src().join("runtime/udp_flow/managed/flow/request.rs"));
    let request = read_module(&proxy_src().join("runtime/udp_flow/managed/flow/request.rs"));
    for module_name in ["mod datagram;", "mod envelope;", "mod stream;"] {
        assert!(request_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct ManagedDatagramFlow",
        "pub(crate) struct ManagedStreamPacketFlow",
        "pub(crate) struct ManagedRelayStreamFlow",
        "pub(crate) struct ManagedUdpFlowRequest",
        "pub(crate) enum ManagedUdpFlowKind",
    ] {
        assert!(
            !request_root.contains(forbidden),
            "udp_flow managed flow request facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct ManagedDatagramFlow",
        "pub(crate) struct ManagedStreamPacketFlow",
        "pub(crate) struct ManagedRelayStreamFlow",
        "pub(crate) struct ManagedUdpFlowRequest",
        "pub(crate) enum ManagedUdpFlowKind",
    ] {
        assert!(
            request.contains(expected),
            "udp_flow managed flow request module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_model_send_root_stays_facade_only() {
    let send_root = read(&proxy_src().join("runtime/udp_flow/managed/model/send.rs"));
    let send = read_module(&proxy_src().join("runtime/udp_flow/managed/model/send.rs"));
    for module_name in ["mod datagram;", "mod relay;", "mod stream;"] {
        assert!(send_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct ManagedDatagramExistingSend",
        "pub(crate) struct ManagedStreamExistingSend",
        "pub(crate) struct ManagedRelayExistingSend",
        "fn datagram(",
        "fn stream_packet(",
        "fn relay_stream(",
    ] {
        assert!(
            !send_root.contains(forbidden),
            "udp_flow managed model send facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct ManagedDatagramExistingSend",
        "pub(crate) struct ManagedStreamExistingSend",
        "pub(crate) struct ManagedRelayExistingSend",
        "fn datagram(",
        "fn stream_packet(",
        "fn relay_stream(",
    ] {
        assert!(
            send.contains(expected),
            "udp_flow managed model send module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_flow_root_stays_facade_only() {
    let flow_root = read(&proxy_src().join("runtime/udp_flow/managed/flow.rs"));
    let flow = read_module(&proxy_src().join("runtime/udp_flow/managed/flow.rs"));
    for module_name in ["mod request;", "mod resume;"] {
        assert!(flow_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct ManagedUdpFlowRequest",
        "pub(crate) enum ManagedUdpFlowKind",
        "pub(crate) struct ManagedUdpFlowResume",
    ] {
        assert!(
            !flow_root.contains(forbidden),
            "udp_flow managed flow facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct ManagedUdpFlowRequest",
        "pub(crate) enum ManagedUdpFlowKind",
        "pub(crate) struct ManagedUdpFlowResume",
    ] {
        assert!(
            flow.contains(expected),
            "udp_flow managed flow module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_model_root_stays_facade_only() {
    let model_root = read(&proxy_src().join("runtime/udp_flow/managed/model.rs"));
    let model = read_module(&proxy_src().join("runtime/udp_flow/managed/model.rs"));
    for module_name in ["mod handler;", "mod send;"] {
        assert!(model_root.contains(module_name));
    }
    for forbidden in [
        "trait ManagedDatagramFlowHandler",
        "trait ManagedStreamPacketFlowHandler",
        "trait ManagedRelayFlowHandler",
        "pub(crate) struct ManagedStreamHandlerPair",
        "pub(crate) struct ManagedDatagramExistingSend",
    ] {
        assert!(
            !model_root.contains(forbidden),
            "udp_flow managed model facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "trait ManagedDatagramFlowHandler",
        "trait ManagedStreamPacketFlowHandler",
        "trait ManagedRelayFlowHandler",
        "pub(crate) struct ManagedStreamHandlerPair",
        "pub(crate) struct ManagedDatagramExistingSend",
    ] {
        assert!(
            model.contains(expected),
            "udp_flow managed model module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_state_start_root_stays_facade_only() {
    let start_root = read(&proxy_src().join("runtime/udp_flow/managed/state/start.rs"));
    let start = read_module(&proxy_src().join("runtime/udp_flow/managed/state/start.rs"));
    for module_name in ["mod datagram;", "mod dispatch;", "mod stream;"] {
        assert!(start_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn start_flow(",
        "pub(crate) async fn start_datagram_flow(",
        "pub(crate) async fn start_stream_packet_flow(",
        "pub(crate) async fn start_relay_stream_flow(",
    ] {
        assert!(
            !start_root.contains(forbidden),
            "udp_flow managed state start facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn start_flow(",
        "pub(crate) async fn start_datagram_flow(",
        "pub(crate) async fn start_stream_packet_flow(",
        "pub(crate) async fn start_relay_stream_flow(",
    ] {
        assert!(
            start.contains(expected),
            "udp_flow managed state start module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_state_start_dispatch_root_stays_facade_only() {
    let dispatch_root = read(&proxy_src().join("runtime/udp_flow/managed/state/start/dispatch.rs"));
    let dispatch =
        read_module(&proxy_src().join("runtime/udp_flow/managed/state/start/dispatch.rs"));
    for module_name in ["mod datagram;", "mod request;", "mod relay;", "mod stream;"] {
        assert!(dispatch_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn start_flow(",
        "pub(crate) async fn start_datagram_request(",
        "pub(crate) async fn start_stream_packet_request(",
        "pub(crate) async fn start_relay_stream_request(",
    ] {
        assert!(
            !dispatch_root.contains(forbidden),
            "udp_flow managed state start dispatch facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn start_flow(",
        "pub(crate) async fn start_datagram_request(",
        "pub(crate) async fn start_stream_packet_request(",
        "pub(crate) async fn start_relay_stream_request(",
    ] {
        assert!(
            dispatch.contains(expected),
            "udp_flow managed state start dispatch module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_stream_manager_root_stays_facade_only() {
    let manager_root =
        read(&proxy_src().join("runtime/udp_flow/managed/stream_manager/manager.rs"));
    let manager =
        read_module(&proxy_src().join("runtime/udp_flow/managed/stream_manager/manager.rs"));
    for module_name in ["mod mismatch;", "mod model;", "mod relay;", "mod send;"] {
        assert!(manager_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct ManagedStreamFlowManager",
        "pub(crate) struct SharedManagedStreamFlowManager",
        "pub(super) struct ManagedStreamRelayRequest",
        "pub(super) async fn send(",
        "async fn send_relay(",
    ] {
        assert!(
            !manager_root.contains(forbidden),
            "udp_flow managed stream_manager facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct ManagedStreamFlowManager",
        "pub(crate) struct SharedManagedStreamFlowManager",
        "pub(super) struct ManagedStreamRelayRequest",
        "pub(super) async fn send(",
        "async fn send_relay(",
    ] {
        assert!(
            manager.contains(expected),
            "udp_flow managed stream_manager module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_stream_manager_send_root_stays_facade_only() {
    let send_root =
        read(&proxy_src().join("runtime/udp_flow/managed/stream_manager/manager/send.rs"));
    let send =
        read_module(&proxy_src().join("runtime/udp_flow/managed/stream_manager/manager/send.rs"));
    for module_name in ["mod dispatch;", "mod existing;"] {
        assert!(send_root.contains(module_name));
    }
    for forbidden in [
        "pub(super) async fn send(",
        "async fn send_managed_existing(",
        "\"relay upstream is not established\"",
    ] {
        assert!(
            !send_root.contains(forbidden),
            "udp_flow managed stream_manager send facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(super) async fn send(",
        "async fn send_managed_existing(",
        "\"relay upstream is not established\"",
    ] {
        assert!(
            send.contains(expected),
            "udp_flow managed stream_manager send module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_stream_manager_relay_root_stays_facade_only() {
    let relay_root =
        read(&proxy_src().join("runtime/udp_flow/managed/stream_manager/manager/relay.rs"));
    let relay =
        read_module(&proxy_src().join("runtime/udp_flow/managed/stream_manager/manager/relay.rs"));
    for module_name in ["mod dispatch;", "mod handler;"] {
        assert!(relay_root.contains(module_name));
    }
    for forbidden in [
        "async fn send_relay(",
        "async fn send_managed_relay_existing(",
        "fn supports_managed_existing(",
        "fn supports_managed_relay_existing(",
    ] {
        assert!(
            !relay_root.contains(forbidden),
            "udp_flow managed stream_manager relay facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "async fn send_relay(",
        "async fn send_managed_relay_existing(",
        "fn supports_managed_existing(",
        "fn supports_managed_relay_existing(",
    ] {
        assert!(
            relay.contains(expected),
            "udp_flow managed stream_manager relay module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_stream_manager_relay_handler_root_stays_facade_only() {
    let handler_root =
        read(&proxy_src().join("runtime/udp_flow/managed/stream_manager/manager/relay/handler.rs"));
    let handler = read_module(
        &proxy_src().join("runtime/udp_flow/managed/stream_manager/manager/relay/handler.rs"),
    );
    for module_name in ["mod packet;", "mod relay;"] {
        assert!(handler_root.contains(module_name));
    }
    for forbidden in [
        "impl<T> ManagedStreamPacketFlowHandler for SharedManagedStreamFlowManager<T>",
        "fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
        "impl<T> ManagedRelayFlowHandler for SharedManagedStreamFlowManager<T>",
        "fn supports_managed_relay_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
    ] {
        assert!(
            !handler_root.contains(forbidden),
            "udp_flow managed stream_manager relay handler facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "impl<T> ManagedStreamPacketFlowHandler for SharedManagedStreamFlowManager<T>",
        "fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
        "impl<T> ManagedRelayFlowHandler for SharedManagedStreamFlowManager<T>",
        "fn supports_managed_relay_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
    ] {
        assert!(
            handler.contains(expected),
            "udp_flow managed stream_manager relay handler module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_datagram_manager_root_stays_facade_only() {
    let manager_root =
        read(&proxy_src().join("runtime/udp_flow/managed/datagram_manager/manager.rs"));
    let manager =
        read_module(&proxy_src().join("runtime/udp_flow/managed/datagram_manager/manager.rs"));
    for module_name in ["mod flow;", "mod mismatch;", "mod model;", "mod socket;"] {
        assert!(manager_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct ManagedDatagramFlowManager",
        "pub(crate) struct ManagedDatagramSocketFlowManager",
        "async fn send(",
        "async fn send_managed_existing(",
        "fn supports_managed_existing(",
    ] {
        assert!(
            !manager_root.contains(forbidden),
            "udp_flow managed datagram_manager facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct ManagedDatagramFlowManager",
        "pub(crate) struct ManagedDatagramSocketFlowManager",
        "async fn send(",
        "async fn send_managed_existing(",
        "fn supports_managed_existing(",
    ] {
        assert!(
            manager.contains(expected),
            "udp_flow managed datagram_manager module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_datagram_manager_flow_root_stays_facade_only() {
    let flow_root =
        read(&proxy_src().join("runtime/udp_flow/managed/datagram_manager/manager/flow.rs"));
    let flow =
        read_module(&proxy_src().join("runtime/udp_flow/managed/datagram_manager/manager/flow.rs"));
    for module_name in ["mod dispatch;", "mod handler;"] {
        assert!(flow_root.contains(module_name));
    }
    for forbidden in [
        "fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
        "async fn send(",
        "async fn send_managed_existing(",
        "impl<T, C> ManagedDatagramFlowHandler for ManagedDatagramFlowManager<T, C>",
    ] {
        assert!(
            !flow_root.contains(forbidden),
            "udp_flow managed datagram manager flow facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
        "async fn send(",
        "async fn send_managed_existing(",
        "impl<T, C> ManagedDatagramFlowHandler for ManagedDatagramFlowManager<T, C>",
    ] {
        assert!(
            flow.contains(expected),
            "udp_flow managed datagram manager flow module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_datagram_manager_socket_root_stays_facade_only() {
    let socket_root =
        read(&proxy_src().join("runtime/udp_flow/managed/datagram_manager/manager/socket.rs"));
    let socket = read_module(
        &proxy_src().join("runtime/udp_flow/managed/datagram_manager/manager/socket.rs"),
    );
    for module_name in ["mod dispatch;", "mod handler;"] {
        assert!(socket_root.contains(module_name));
    }
    for forbidden in [
        "fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
        "async fn send(",
        "async fn send_managed_existing(",
        "impl<T, C> ManagedDatagramFlowHandler for ManagedDatagramSocketFlowManager<T, C>",
    ] {
        assert!(
            !socket_root.contains(forbidden),
            "udp_flow managed datagram manager socket facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool",
        "async fn send(",
        "async fn send_managed_existing(",
        "impl<T, C> ManagedDatagramFlowHandler for ManagedDatagramSocketFlowManager<T, C>",
    ] {
        assert!(
            socket.contains(expected),
            "udp_flow managed datagram manager socket module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_connection_tuple_root_stays_facade_only() {
    let tuple_root = read(&proxy_src().join("runtime/udp_flow/managed/connection/tuple.rs"));
    let tuple = read_module(&proxy_src().join("runtime/udp_flow/managed/connection/tuple.rs"));
    for module_name in ["mod build;", "mod connection;", "mod flow;", "mod sender;"] {
        assert!(tuple_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) trait ManagedTupleUdpSender",
        "struct ManagedTupleUdpConnection",
        "pub(crate) fn managed_tuple_udp_connection(",
        "pub(crate) trait ManagedTupleUdpFlowConnection",
        "pub(crate) fn managed_tuple_udp_connection_from_flow<",
    ] {
        assert!(
            !tuple_root.contains(forbidden),
            "udp_flow managed connection tuple facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) trait ManagedTupleUdpSender",
        "struct ManagedTupleUdpConnection",
        "fn managed_tuple_udp_connection(",
        "pub(crate) trait ManagedTupleUdpFlowConnection",
        "pub(crate) fn managed_tuple_udp_connection_from_flow<",
    ] {
        assert!(
            tuple.contains(expected),
            "udp_flow managed connection tuple module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_managed_connection_packet_root_stays_facade_only() {
    let packet_root = read(&proxy_src().join("runtime/udp_flow/managed/connection/packet.rs"));
    let packet = read_module(&proxy_src().join("runtime/udp_flow/managed/connection/packet.rs"));
    for module_name in ["mod build;", "mod connection;", "mod flow;", "mod sender;"] {
        assert!(packet_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) trait ManagedPacketUdpSender",
        "struct ManagedPacketUdpConnection",
        "pub(crate) fn managed_packet_udp_connection(",
        "pub(crate) trait ManagedPacketUdpFlowConnection",
        "pub(crate) fn managed_packet_udp_connection_from_flow<",
    ] {
        assert!(
            !packet_root.contains(forbidden),
            "udp_flow managed connection packet facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) trait ManagedPacketUdpSender",
        "struct ManagedPacketUdpConnection",
        "fn managed_packet_udp_connection(",
        "pub(crate) trait ManagedPacketUdpFlowConnection",
        "pub(crate) fn managed_packet_udp_connection_from_flow<",
    ] {
        assert!(
            packet.contains(expected),
            "udp_flow managed connection packet module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_registered_root_stays_facade_only() {
    let registered_root = read(&proxy_src().join("runtime/udp_flow/registered/mod.rs"));
    let registered = read_module(&proxy_src().join("runtime/udp_flow/registered"));
    for module_name in ["mod forward;", "mod state;", "mod upstream;"] {
        assert!(registered_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct RegisteredUdpHandlers",
        "pub(crate) struct RegisteredUdpState",
        "pub(crate) trait UpstreamAssociationHandler",
        "pub(crate) struct UpstreamAssociationSend",
        "fn boxed_registered_upstream_handler<",
        "pub(crate) struct UpstreamUdpHandlers",
    ] {
        assert!(
            !registered_root.contains(forbidden),
            "udp_flow registered facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct RegisteredUdpHandlers",
        "pub(crate) struct RegisteredUdpState",
        "pub(crate) trait UpstreamAssociationHandler",
        "pub(crate) struct UpstreamAssociationSend",
        "fn boxed_registered_upstream_handler<",
        "pub(crate) struct UpstreamUdpHandlers",
    ] {
        assert!(
            registered.contains(expected),
            "udp_flow registered module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_registered_upstream_root_stays_facade_only() {
    let upstream_root = read(&proxy_src().join("runtime/udp_flow/registered/upstream.rs"));
    let upstream = read_module(&proxy_src().join("runtime/udp_flow/registered/upstream"));
    for module_name in ["mod contract;", "mod runtime;", "mod state;"] {
        assert!(upstream_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) trait UpstreamAssociationHandler",
        "pub(crate) struct UpstreamAssociationSend",
        "pub(crate) struct UpstreamUdpHandlers",
        "fn boxed_registered_upstream_handler<",
        "pub(crate) struct UpstreamAssociationRuntime",
        "pub(crate) struct TrackedUpstreamAssociationState",
    ] {
        assert!(
            !upstream_root.contains(forbidden),
            "udp_flow registered upstream facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) trait UpstreamAssociationHandler",
        "pub(crate) struct UpstreamAssociationSend",
        "pub(crate) struct UpstreamUdpHandlers",
        "fn boxed_registered_upstream_handler<",
        "pub(crate) struct UpstreamAssociationRuntime",
        "pub(crate) struct TrackedUpstreamAssociationState",
    ] {
        assert!(
            upstream.contains(expected),
            "udp_flow registered upstream module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_registered_upstream_runtime_handler_root_stays_facade_only() {
    let handler_root =
        read(&proxy_src().join("runtime/udp_flow/registered/upstream/runtime/handler.rs"));
    let handler =
        read_module(&proxy_src().join("runtime/udp_flow/registered/upstream/runtime/handler.rs"));
    for module_name in ["mod build;", "mod dispatch;", "mod model;"] {
        assert!(handler_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) struct RegisteredUpstreamAssociationHandler",
        "pub(crate) fn new(stages: UpstreamAssociationStages) -> Self",
        "async fn send_upstream(",
        "fn close_all_upstreams(&mut self)",
        "fn boxed_registered_upstream_handler<",
    ] {
        assert!(
            !handler_root.contains(forbidden),
            "udp_flow registered upstream runtime handler facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) struct RegisteredUpstreamAssociationHandler",
        "pub(crate) fn new(stages: UpstreamAssociationStages) -> Self",
        "async fn send_upstream(",
        "fn close_all_upstreams(&mut self)",
        "fn boxed_registered_upstream_handler<",
    ] {
        assert!(
            handler.contains(expected),
            "udp_flow registered upstream runtime handler module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_registered_upstream_runtime_control_root_stays_facade_only() {
    let control_root =
        read(&proxy_src().join("runtime/udp_flow/registered/upstream/runtime/control.rs"));
    let control =
        read_module(&proxy_src().join("runtime/udp_flow/registered/upstream/runtime/control.rs"));
    for module_name in ["mod close;", "mod start;"] {
        assert!(control_root.contains(module_name));
    }
    for forbidden in [
        "async fn start_registered_upstream_flow<",
        "fn close_registered_dropped_upstream<",
        "fn close_registered_idle_upstream<",
        "fn registered_target_log_parts<",
    ] {
        assert!(
            !control_root.contains(forbidden),
            "udp_flow registered upstream runtime control facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "async fn start_registered_upstream_flow<",
        "fn close_registered_dropped_upstream<",
        "fn close_registered_idle_upstream<",
        "fn registered_target_log_parts<",
    ] {
        assert!(
            control.contains(expected),
            "udp_flow registered upstream runtime control module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_registered_upstream_runtime_association_lifecycle_root_stays_facade_only() {
    let lifecycle_root = read(
        &proxy_src().join("runtime/udp_flow/registered/upstream/runtime/association/lifecycle.rs"),
    );
    let lifecycle = read_module(
        &proxy_src().join("runtime/udp_flow/registered/upstream/runtime/association/lifecycle.rs"),
    );
    for module_name in ["mod close;", "mod send;"] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn send_packet(",
        "async fn ensure_association(",
        "pub(crate) fn drop_after_send_error(",
        "pub(crate) fn close_idle(",
        "pub(crate) fn close_dropped(",
        "pub(crate) fn close_all_upstreams(",
    ] {
        assert!(
            !lifecycle_root.contains(forbidden),
            "udp_flow registered upstream runtime association lifecycle facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn send_packet(",
        "async fn ensure_association(",
        "pub(crate) fn drop_after_send_error(",
        "pub(crate) fn close_idle(",
        "pub(crate) fn close_dropped(",
        "pub(crate) fn close_all_upstreams(",
    ] {
        assert!(
            lifecycle.contains(expected),
            "udp_flow registered upstream runtime association lifecycle module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_registered_state_start_root_stays_facade_only() {
    let start_root = read(&proxy_src().join("runtime/udp_flow/registered/state/start.rs"));
    let start = read_module(&proxy_src().join("runtime/udp_flow/registered/state/start.rs"));
    for module_name in ["mod error;", "mod managed;", "mod upstream;"] {
        assert!(start_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn start_upstream_udp_flow(",
        "pub(crate) fn handles_upstream_resume(",
        "pub(crate) async fn start_managed_udp_flow(",
        "fn unhandled_managed_flow(",
        "fn upstream_send(",
    ] {
        assert!(
            !start_root.contains(forbidden),
            "udp_flow registered state start facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) async fn start_upstream_udp_flow(",
        "pub(crate) fn handles_upstream_resume(",
        "pub(crate) async fn start_managed_udp_flow(",
        "fn unhandled_managed_flow(",
        "fn upstream_send(",
    ] {
        assert!(
            start.contains(expected),
            "udp_flow registered state start module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn udp_flow_registered_state_lifecycle_root_stays_facade_only() {
    let lifecycle_root = read(&proxy_src().join("runtime/udp_flow/registered/state/lifecycle.rs"));
    let lifecycle =
        read_module(&proxy_src().join("runtime/udp_flow/registered/state/lifecycle.rs"));
    for module_name in ["mod build;", "mod managed;", "mod upstream;"] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) fn new(",
        "pub(crate) fn register_managed_flow(",
        "pub(crate) fn managed_flow_resume(",
        "pub(crate) async fn recv_upstream_response(",
        "pub(crate) fn close_idle_upstream(",
        "fn closed_registered_upstream_association(",
    ] {
        assert!(
            !lifecycle_root.contains(forbidden),
            "udp_flow registered state lifecycle facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "pub(crate) fn new(",
        "pub(crate) fn register_managed_flow(",
        "pub(crate) fn managed_flow_resume(",
        "pub(crate) async fn recv_upstream_response(",
        "pub(crate) fn close_idle_upstream(",
        "fn closed_registered_upstream_association(",
    ] {
        assert!(
            lifecycle.contains(expected),
            "udp_flow registered state lifecycle module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn outbound_execution_state_machines_live_in_runtime_dispatch() {
    for relative in [
        "inventory/tcp/outbound.rs",
        "inventory/tcp/leaf.rs",
        "inventory/udp/outbound.rs",
        "inventory/udp/leaf.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            !source.contains("use crate::runtime::Proxy"),
            "{relative} must not import Proxy directly"
        );
        assert!(
            !source.contains("&Proxy"),
            "{relative} must not carry raw Proxy references"
        );
        assert!(
            !source.contains(".await"),
            "{relative} must prepare operations without executing them"
        );
        assert!(
            !source.contains(".execute("),
            "{relative} must not execute prepared operations"
        );
    }

    let tcp_candidate = read(&proxy_src().join("runtime/tcp_dispatch/candidate.rs"));
    let tcp_outbound = read(&proxy_src().join("runtime/tcp_dispatch/outbound.rs"));
    let udp_outbound = read(&proxy_src().join("runtime/udp_dispatch/outbound.rs"));
    assert!(tcp_candidate.contains("dispatch_prepared_tcp_candidate("));
    assert!(tcp_candidate.contains("operation.execute("));
    assert!(tcp_outbound.contains("execute_prepared_tcp_outbound("));
    assert!(tcp_outbound.contains("PreparedTcpOutbound::Fallback"));
    assert!(udp_outbound.contains("execute_prepared_udp_outbound("));
    assert!(udp_outbound.contains("PreparedUdpOutbound::Fallback"));
    assert!(udp_outbound.contains("operation.execute("));
    for source in [tcp_candidate, tcp_outbound, udp_outbound] {
        assert!(!source.contains("ResolvedLeafOutbound"));
        assert!(!source.contains("use crate::runtime::Proxy"));
        assert!(!source.contains("&Proxy"));
    }
}

#[test]
fn inventory_runtime_delegates_leaf_claim_logic_to_registry() {
    let runtime = read(&proxy_src().join("inventory/runtime.rs"));
    assert!(runtime.contains("struct ClaimedInventoryLeaf"));
    assert!(runtime.contains("struct ClaimedRelayChain"));
    assert!(runtime.contains("fn claim_relay_chain<'a, E, F, G>("));
    assert!(!runtime.contains("enum ClaimedResolvedOutbound"));
    assert!(!runtime.contains("fn claim_tcp_outbound"));
    assert!(!runtime.contains("fn claim_udp_outbound"));
    assert!(!runtime.contains("fn claim_tcp_relay_chain"));
    assert!(!runtime.contains("fn claim_udp_relay_chain"));
    assert!(runtime.contains("self.registry.claim_outbound_leaf(config, leaf)"));
    assert!(!runtime.contains("claimed_tcp_outbound_leaf"));
    assert!(!runtime.contains("claimed_udp_flow_leaf"));
    assert!(!runtime.contains("claimed_udp_packet_path_leaf"));
    assert!(!runtime.contains("find_outbound_leaf("));
    assert!(!runtime.contains("find_udp_flow_leaf("));
    assert!(!runtime.contains("find_udp_packet_path_leaf("));
    assert!(!runtime.contains("ResolvedOutbound"));
    assert!(!runtime.contains("TcpPathCategory::Block"));
}

#[test]
fn registry_outbound_claim_surface_replaces_lookup_only_helpers() {
    let outbound = read(&proxy_src().join("protocol_registry/registry/outbound.rs"));
    assert!(outbound.contains("fn claim_outbound_leaf"));
    assert!(outbound.contains("fn outbound_protocol_entry("));
    assert!(!outbound.contains("fn claimed_tcp_outbound_leaf"));
    assert!(!outbound.contains("fn claimed_tcp_entry"));
    assert!(!outbound.contains("fn claimed_udp_flow_leaf"));
    assert!(!outbound.contains("fn claimed_udp_packet_path_leaf"));
    assert!(!outbound.contains("fn find_outbound_leaf"));
    assert!(!outbound.contains("fn find_udp_flow_leaf"));
    assert!(!outbound.contains("fn find_udp_packet_path_leaf"));
    assert!(!outbound.contains("fn outbound_leaf_runtime("));
}

#[test]
fn claimed_outbound_leaf_owns_capability_preparation() {
    let outbound = read(&proxy_src().join("protocol_registry/registry/outbound.rs"));
    let capability = read(&proxy_src().join("protocol_registry/capability.rs"));
    let build = read(&proxy_src().join("protocol_registry/registry/build.rs"));
    let registry_mod = read(&proxy_src().join("protocol_registry/registry/mod.rs"));
    for method in [
        "fn prepare_tcp_connect(",
        "fn prepare_tcp_relay_hop(",
        "fn prepare_udp_flow(",
        "fn prepare_udp_relay(",
        "fn prepare_udp_packet_path(",
    ] {
        assert!(
            outbound.contains(method),
            "claimed outbound leaves should expose `{method}` so inventory stays generic after claim"
        );
    }
    assert!(!outbound.contains("pub(crate) struct ClaimedOutboundLeaf<'a> {\r\n    leaf:"));
    assert!(!outbound.contains("pub(crate) struct ClaimedOutboundLeaf<'a> {\n    leaf:"));
    assert!(!outbound.contains("fn new(\n        _leaf: ResolvedLeafOutbound<'a>,"));
    assert!(!outbound.contains("fn new(\r\n        _leaf: ResolvedLeafOutbound<'a>,"));
    assert!(!outbound.contains("leaf.protocol_name()"));
    assert!(outbound.contains("let protocol = outbound.protocol.protocol_name();"));
    assert!(outbound.contains("ResolvedLeafOutbound::Proxy { identity }"));
    assert!(!outbound.contains("for entry in &self.entries {\n            if let Some(claimed) = entry.tcp.claim_tcp_outbound_leaf(leaf.clone()) {"));
    assert!(!outbound.contains("for entry in &self.entries {\r\n            if let Some(claimed) = entry.tcp.claim_tcp_outbound_leaf(leaf.clone()) {"));
    assert!(capability.contains("struct OutboundLeafClaim<'a>"));
    assert!(!capability.contains("trait OutboundLeafClaimCapability"));
    assert!(registry_mod.contains("trait OutboundLeafClaimer"));
    assert!(build.contains("type OutboundLeafClaimFn"));
    assert!(outbound.contains("entry.outbound.claim_outbound_leaf(input)"));
    assert!(outbound.contains("fn claim_outbound_hooks<'a>("));
    assert!(!outbound.contains("claim_tcp_outbound_leaf(leaf.clone())"));
    assert!(!outbound.contains("claim_udp_flow_leaf(leaf.clone())"));
    assert!(!outbound.contains("claim_udp_packet_path_leaf(leaf.clone())"));
    assert!(!outbound.contains("fn claim_tcp_hooks<'a>("));
    assert!(!outbound.contains("fn claim_udp_hooks<'a>("));
    assert!(outbound.contains("struct ClaimedTcpHooks"));
    assert!(outbound.contains("struct ClaimedUdpHooks"));
    assert!(!outbound.contains("HookClaimedTcpLeaf"));
    assert!(!outbound.contains("HookClaimedUdpLeaf"));
    assert!(!outbound.contains("HookClaimedUdpPacketPathLeaf"));
    assert!(!outbound.contains("self.leaf"));
    assert!(!outbound.contains("let runtime = capability.runtime();"));
    assert!(!outbound.contains("udp: build_udp_hooks("));
    assert!(!capability.contains(
        "fn prepare_tcp_connect<'a>(\n        &self,\n        _leaf: ResolvedLeafOutbound<'a>,"
    ));
    assert!(!capability.contains(
        "fn prepare_udp_flow<'a>(\n        &self,\n        _leaf: ResolvedLeafOutbound<'a>,"
    ));
    assert!(!capability.contains(
        "fn prepare_udp_packet_path<'a>(\n        &self,\n        _leaf: ResolvedLeafOutbound<'a>,"
    ));
    assert!(!build.contains("adapter.claim_tcp_outbound_leaf("));
    assert!(!build.contains("adapter.claim_udp_flow_leaf("));
    assert!(!build.contains("adapter.claim_udp_packet_path_leaf("));
}

#[test]
fn transport_bridge_adapters_offer_claim_time_tcp_projection() {
    let helper = read(&proxy_src().join("protocol_registry/transport_leaf/tcp.rs"));
    assert!(helper.contains("struct ClaimedTransportTcpLeaf"));
    assert!(helper.contains("claim_transport_tcp_leaf"));

    for relative in [
        "adapters/vless.rs",
        "adapters/vmess.rs",
        "adapters/trojan.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("fn claim_outbound_leaf_impl<'a>("),
            "{relative} should expose a unified claim-time outbound projection path"
        );
        assert!(
            source.contains("claim_transport_tcp_leaf("),
            "{relative} should project into the shared claimed transport leaf helper"
        );
    }
}

#[test]
fn transport_bridge_adapters_offer_claim_time_udp_projection() {
    let helper = read(&proxy_src().join("protocol_registry/transport_leaf/udp.rs"));
    assert!(helper.contains("struct ClaimedTransportUdpLeaf"));
    assert!(helper.contains("claim_transport_udp_leaf"));
    assert!(helper.contains("ClaimedRelayTwoStreamTransportUdpLeaf"));
    assert!(helper.contains("claim_relay_two_stream_transport_udp_leaf"));

    for relative in [
        "adapters/vless.rs",
        "adapters/vmess.rs",
        "adapters/trojan.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("fn claim_outbound_leaf_impl<'a>("),
            "{relative} should expose a unified claim-time outbound projection path"
        );
    }

    let vless = read(&proxy_src().join("adapters/vless.rs"));
    assert!(vless.contains("claim_relay_two_stream_transport_udp_leaf("));
    assert!(!vless.contains("VlessStreamBridge"));

    for relative in ["adapters/vmess.rs", "adapters/trojan.rs"] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("claim_transport_udp_leaf("),
            "{relative} should project into the shared claimed UDP transport leaf helper"
        );
    }

    let vmess = read(&proxy_src().join("adapters/vmess.rs"));
    assert!(!vmess.contains("VmessStreamBridge"));

    let trojan = read(&proxy_src().join("adapters/trojan.rs"));
    assert!(!trojan.contains("TrojanTlsBridge"));
}

#[test]
fn managed_stream_udp_handlers_key_off_resume_metadata_not_bridge_types() {
    let managed_udp_traits = read(&workspace_root().join("crates/traits/src/udp_flow.rs"));
    let transport_lib = read(&workspace_root().join("crates/transport/src/lib.rs"));
    let proxy_connector =
        read(&proxy_src().join("runtime/udp_flow/managed/stream_manager/connector.rs"));
    let handler =
        read(&proxy_src().join("runtime/udp_flow/managed/bridge/stream_packet/handler.rs"));
    assert!(!managed_udp_traits.contains("ProtocolManagedStreamUdpBridgeHandlerMetadata"));
    assert!(managed_udp_traits.contains("ProtocolUdpFlowLeaf"));
    assert!(managed_udp_traits.contains("ProtocolRelayTwoStreamUdpFlowLeaf"));
    assert!(!managed_udp_traits.contains("ManagedStream"));
    assert!(!workspace_root()
        .join("crates/traits/src/managed_udp.rs")
        .exists());
    assert!(!managed_udp_traits.contains("ESTABLISH_STAGE"));
    assert!(!managed_udp_traits.contains("MISMATCH_MESSAGE"));
    assert!(!managed_udp_traits.contains("ProtocolManagedStreamFlowStages"));
    assert!(!managed_udp_traits.contains("ManagedTupleUdpResume"));
    assert!(!managed_udp_traits.contains("ManagedPacketUdpResume"));
    assert!(!managed_udp_traits.contains("ManagedConnectorFlow"));
    assert!(!managed_udp_traits.contains("ProtocolManagedStreamConnectorParts"));
    assert!(!transport_lib.contains("pub mod managed_udp"));
    assert!(!workspace_root()
        .join("crates/transport/src/managed_udp.rs")
        .exists());
    assert!(proxy_connector.contains("struct ManagedTupleUdpResume"));
    assert!(proxy_connector.contains("struct ManagedPacketUdpResume"));
    assert!(handler.contains("managed_stream_udp_handler_for_resume"));
    assert!(!handler.contains("managed_stream_udp_handler_for_bridge"));

    for relative in [
        "adapters/vless.rs",
        "adapters/vmess.rs",
        "adapters/trojan.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("managed_stream_udp_handler_for_resume::<"),
            "{relative} should register managed stream UDP handlers by resume type"
        );
        assert!(
            !source.contains("managed_stream_udp_handler_for_bridge::<"),
            "{relative} must not route managed stream UDP handler registration through bridge types"
        );
        assert!(source.contains("type RuntimeResume = Managed"));
        assert!(source.contains("impl ManagedStreamConnectorParts for"));
    }

    for relative in [
        "protocols/vless/src/transport/managed_udp.rs",
        "protocols/vmess/src/transport/managed_udp.rs",
        "protocols/trojan/src/transport/managed_udp.rs",
        "protocols/mieru/src/transport/managed_udp.rs",
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(!source.contains("ManagedTupleUdpResume"));
        assert!(!source.contains("ManagedPacketUdpResume"));
        assert!(!source.contains("ManagedConnectorFlowOps"));
        assert!(!source.contains("ManagedStreamUdpResume"));
    }
}

#[test]
fn proxy_owns_transport_leaf_execution_contracts_and_stages() {
    let transport_lib = read(&workspace_root().join("crates/transport/src/lib.rs"));
    let proxy_contract = read(&proxy_src().join("runtime/transport_leaf.rs"));
    assert!(!transport_lib.contains("pub mod outbound_leaf"));
    assert!(!workspace_root()
        .join("crates/transport/src/outbound_leaf.rs")
        .exists());
    assert!(!proxy_src()
        .join("protocol_registry/transport_leaf/prepared.rs")
        .exists());
    assert_sources_exclude(
        &proxy_src().join("protocol_registry/transport_leaf"),
        &["trait ProxyTransport"],
    );
    assert!(proxy_contract.contains("pub(crate) trait ProxyTransportTcpLeaf"));
    assert!(proxy_contract.contains("pub(crate) trait ProxyTransportUdpLeaf"));
    assert!(proxy_contract.contains("pub(crate) trait ProxyRelayTwoStreamTransportLeaf"));
    assert!(proxy_contract.contains("const TCP_CONNECT_STAGE"));
    assert!(proxy_contract.contains("const UDP_DIRECT_STAGE"));

    for relative in [
        "protocol_registry/transport_leaf/udp.rs",
        "runtime/transport_leaf.rs",
        "runtime/udp_dispatch/operation/transport.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(!source.contains("ProtocolUdpFlowLeaf"));
        assert!(!source.contains("ProtocolRelayTwoStreamUdpFlowLeaf"));
    }

    for (adapter, leaf, managed_trait) in [
        (
            "adapters/vless.rs",
            "protocols/vless/src/transport/leaf.rs",
            "impl ProtocolUdpFlowLeaf for VlessOutboundLeaf",
        ),
        (
            "adapters/vmess.rs",
            "protocols/vmess/src/transport/leaf.rs",
            "impl ProtocolUdpFlowLeaf for VmessOutboundLeaf",
        ),
        (
            "adapters/trojan.rs",
            "protocols/trojan/src/transport/leaf.rs",
            "impl ProtocolUdpFlowLeaf for TrojanOutboundLeaf",
        ),
    ] {
        let adapter_source = read(&proxy_src().join(adapter));
        let leaf_source = read(&workspace_root().join(leaf));
        assert!(adapter_source.contains("impl ProxyTransportTcpLeaf"));
        assert!(adapter_source.contains("impl ProxyTransportUdpLeaf"));
        assert!(leaf_source.contains(managed_trait));
        assert!(!leaf_source.contains("TCP_CONNECT_STAGE"));
        assert!(!leaf_source.contains("UDP_DIRECT_STAGE"));
    }

    for relative in [
        "protocols/vless/src/transport.rs",
        "protocols/vmess/src/transport.rs",
        "protocols/trojan/src/transport.rs",
    ] {
        let source = read(&workspace_root().join(relative));
        assert!(
            !source.contains("mod bridge;"),
            "{relative} must not compile a standalone transport bridge module"
        );
        assert!(
            !source.contains("pub use bridge::"),
            "{relative} must not re-export standalone transport bridge types"
        );
    }
}

#[test]
fn adapters_receive_narrow_runtime_services_only() {
    for path in rust_sources(&proxy_src().join("adapters")) {
        let source = read(&path);
        for forbidden in [
            "TcpRuntimeServices",
            "UdpRuntimeServices",
            "UdpAdapterContext",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} must not receive broad runtime service `{forbidden}`",
                path.display()
            );
        }
    }

    let transport_leaf = read(&proxy_src().join("runtime/transport_leaf.rs"));
    assert!(transport_leaf.contains("services: UpstreamConnectServices"));
    let packet_path = read(&proxy_src().join("runtime/udp_dispatch/packet_path_operation.rs"));
    assert!(packet_path.contains("_services: UdpNetworkServices"));
}

#[test]
fn adapter_support_and_endpoint_projection_have_one_shared_implementation() {
    let identity = read(&proxy_src().join("adapters/identity.rs"));
    assert!(identity.contains("impl<T> ProtocolSupportCapability for T"));

    for path in rust_sources(&proxy_src().join("adapters")) {
        let source = read(&path);
        if path.ends_with("identity.rs") {
            continue;
        }
        assert!(
            !source.contains("impl ProtocolSupportCapability for"),
            "{} must use the shared named adapter capability implementation",
            path.display()
        );
        assert!(
            !source.contains("impl ProxyTransportLeaf for"),
            "{} must use protocol-owned ProtocolOutboundLeaf endpoint facts",
            path.display()
        );
    }
}

#[test]
fn protocol_registry_context_root_stays_a_facade() {
    let root = read(&proxy_src().join("protocol_registry/context.rs"));
    for module in ["mod adapter;", "mod tcp;", "mod upstream;", "mod udp;"] {
        assert!(root.contains(module));
    }
    for forbidden in [
        "struct TcpRuntimeServices",
        "struct UdpRuntimeServices",
        "struct UpstreamConnectServices",
        "struct UdpNetworkServices",
    ] {
        assert!(!root.contains(forbidden));
    }
}

#[test]
fn packet_path_adapters_offer_claim_time_projection() {
    for relative in [
        "adapters/socks5.rs",
        "adapters/shadowsocks.rs",
        "adapters/hysteria2.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("fn claim_outbound_leaf_impl<'a>("),
            "{relative} should expose a unified claim-time outbound projection path"
        );
    }

    for relative in [
        "adapters/socks5/udp.rs",
        "adapters/shadowsocks/udp.rs",
        "adapters/hysteria2/udp.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("ClaimedUdpPacketPathLeaf"),
            "{relative} should materialize packet-path plans into claimed leaves"
        );
        assert!(
            source.contains("claim_udp_packet_path_leaf_impl"),
            "{relative} should own the packet-path claim-time projection helper"
        );
    }
}

#[test]
fn non_transport_bridge_adapters_offer_claim_time_tcp_projection() {
    for relative in [
        "adapters/direct.rs",
        "adapters/socks5.rs",
        "adapters/shadowsocks.rs",
        "adapters/hysteria2.rs",
        "adapters/mieru.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("fn claim_outbound_leaf_impl<'a>("),
            "{relative} should expose a unified claim-time outbound projection path"
        );
    }

    let direct = read(&proxy_src().join("adapters/direct/tcp.rs"));
    assert!(direct.contains("ClaimedTcpOutboundLeaf"));
    assert!(direct.contains("claim_tcp_outbound_leaf_impl"));

    for (relative, shared_claim) in [
        ("adapters/socks5/tcp.rs", "claim_socket_tcp_leaf"),
        ("adapters/shadowsocks/tcp.rs", "claim_socket_tcp_leaf"),
        ("adapters/hysteria2/tcp.rs", "claim_session_tcp_leaf"),
        ("adapters/mieru/tcp.rs", "claim_socket_tcp_leaf"),
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("ClaimedTcpOutboundLeaf"),
            "{relative} should materialize TCP leaves into claimed projections"
        );
        assert!(
            source.contains("claim_tcp_outbound_leaf_impl"),
            "{relative} should own the TCP claim-time projection helper"
        );
        assert!(
            source.contains(shared_claim),
            "{relative} should delegate generic claim wrapping through `{shared_claim}`"
        );
        assert!(
            !source.contains("struct Claimed"),
            "{relative} must not recreate generic claimed TCP wrappers"
        );
    }
}

#[test]
fn non_transport_bridge_adapters_offer_claim_time_udp_projection() {
    for relative in [
        "adapters/direct.rs",
        "adapters/socks5.rs",
        "adapters/shadowsocks.rs",
        "adapters/hysteria2.rs",
        "adapters/mieru.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("fn claim_outbound_leaf_impl<'a>("),
            "{relative} should expose a unified claim-time outbound projection path"
        );
    }

    for relative in [
        "adapters/direct/udp.rs",
        "adapters/socks5/udp.rs",
        "adapters/shadowsocks/udp.rs",
        "adapters/hysteria2/udp.rs",
        "adapters/mieru/udp.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        assert!(
            source.contains("ClaimedUdpFlowLeaf"),
            "{relative} should materialize UDP flow plans into claimed projections"
        );
        assert!(
            source.contains("claim_udp_flow_leaf_impl"),
            "{relative} should own the UDP claim-time projection helper"
        );
    }
}

#[test]
fn outbound_probe_owns_generic_tcp_dispatch_outside_urltest_policy() {
    let urltest = read(&proxy_src().join("groups/urltest.rs"));
    let urltest_refresh = read(&proxy_src().join("groups/urltest/refresh.rs"));
    let outbound_probe = read(&proxy_src().join("runtime/outbound_probe.rs"));
    let runtime_urltest = read(&proxy_src().join("runtime/listeners/urltest.rs"));
    assert!(urltest.contains("struct UrlTestRuntime"));
    assert!(urltest.contains("OutboundProbeRuntime"));
    assert!(urltest_refresh.contains("probe_target_shared("));
    assert!(!urltest.contains("dispatch_tcp_outbound("));
    assert!(!urltest_refresh.contains("dispatch_tcp_outbound("));
    assert!(outbound_probe.contains("dispatch_tcp_outbound("));
    assert!(!outbound_probe.contains("crate::groups"));
    assert!(!outbound_probe.to_ascii_lowercase().contains("urltest"));
    assert!(!urltest.contains("use crate::runtime::Proxy"));
    assert!(!urltest.contains("from_proxy("));
    assert!(!urltest.contains("prepare_tcp_candidate("));
    assert!(!urltest.contains("ResolvedLeafOutbound"));
    assert!(!urltest.contains("impl Proxy"));
    assert!(!runtime_urltest.contains("use super::super::Proxy"));
    assert!(!runtime_urltest.contains("proxy: &Proxy"));
}
