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

#[test]
fn direct_inbound_uses_standard_sources_and_has_no_legacy_listener_executor() {
    let adapter = read(&proxy_src().join("adapters/direct.rs"));
    let inbound = read_module(&proxy_src().join("adapters/direct/inbound.rs"));
    assert!(!adapter.contains("InboundListenerCapability"));
    assert!(inbound.contains("impl ComponentFactory for Factory"));
    assert!(inbound.contains("udp::source"));
    for source in [
        "inbound/direct.rs",
        "runtime/inbound_operation/tcp_and_peer_datagram.rs",
    ] {
        assert!(
            !proxy_src().join(source).exists(),
            "retired Direct listener: {source}"
        );
    }
    assert!(!inbound.contains("tokio::spawn"));
    assert!(!inbound.contains("JoinSet"));
}

#[test]
fn native_udp_peer_admission_matches_original_and_claims_cleanup_before_handshake() {
    let peer = read(&proxy_src().join("runtime/inbound_operation/source/peer.rs"));
    assert!(peer.contains("admission.matches(original, context.limits)"));
    assert!(peer.find("prepare_datagram_peer").unwrap() < peer.find("complete_request").unwrap());
    assert!(peer.contains("spawn_bound"));
    let source =
        read_module(&workspace_root().join("crates/platform/tokio/src/contract/inbound/udp.rs"));
    assert!(!source.contains("AcceptedCaller"));
    assert!(!source.contains("tokio::spawn"));
    assert!(!source.contains("UdpSocket::bind"));
}

#[test]
fn runtime_host_owns_component_lifecycle_and_configuration_work_outside_protocol_lookup() {
    let registry =
        fs::read_to_string(proxy_src().join("protocol_registry/registry/mod.rs")).unwrap();
    let host = fs::read_to_string(workspace_root().join("crates/runtime/src/host.rs")).unwrap();
    assert!(!registry.contains("ComponentCatalog"));
    assert!(!registry.contains("ApplyOwner"));
    assert!(host.contains("components: ComponentCatalog"));
    assert!(host.contains("apply: Arc<ApplyOwner>"));
    assert!(host.contains("reload_ack: Mutex<Option<A>>"));
    assert!(
        !host.contains("tokio::spawn"),
        "moving the owner must not create a second worker framework"
    );
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

fn production_text(path: &Path) -> String {
    let text = read(path);
    let mut lines = text.lines();
    let mut output = String::new();
    while let Some(line) = lines.next() {
        if line.trim_start().starts_with("#[cfg(") {
            let mut attribute = line.to_owned();
            while !attribute.contains(")]") {
                let next = lines.next().expect("complete cfg attribute");
                attribute.push('\n');
                attribute.push_str(next);
            }
            if attribute
                .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .any(|word| word == "test")
            {
                continue;
            }
            output.push_str(&attribute);
        } else {
            output.push_str(line);
        }
        output.push('\n');
    }
    output
}

fn read_module(path: &Path) -> String {
    let mut combined = String::new();
    if path.is_file() {
        combined.push_str(&production_text(path));
        combined.push('\n');
    }
    let module_dir = if path.extension().is_some_and(|extension| extension == "rs") {
        path.with_extension("")
    } else {
        path.to_path_buf()
    };
    if module_dir.is_dir() {
        for source in rust_sources(&module_dir)
            .into_iter()
            .filter(|path| !is_test_source(path))
        {
            combined.push_str(&production_text(&source));
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

fn runtime_protocol_crate_feature_names() -> Vec<String> {
    let mut features = fs::read_dir(workspace_root().join("protocols"))
        .expect("read protocol crates")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let manifest_path = path.join("Cargo.toml");
            if !manifest_path.is_file() {
                return None;
            }
            let manifest = read(&manifest_path);
            let declares_features = manifest.lines().any(|line| line.trim() == "[features]");
            let declares_runtime = manifest
                .lines()
                .any(|line| line.trim_start().starts_with("runtime ="));
            if declares_features && !declares_runtime {
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

fn is_test_source(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "tests")
        || path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "tests.rs" || name.ends_with("_tests.rs"))
}

fn assert_non_test_sources_exclude(root: &Path, forbidden: &[&str]) {
    for path in rust_sources(root) {
        if is_test_source(&path) {
            continue;
        }
        let source = production_text(&path);
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
    assert_non_test_sources_exclude(
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
    // Cold adapter admission may inspect configured recipes and references.
    // Engine leaf recovery and construction of runtime facts remain forbidden.
    assert_non_test_sources_exclude(
        &proxy_src().join("adapters"),
        &[
            "ResolvedLeafOutbound",
            "OutboundLeafRuntime",
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

    for retired in [
        "runtime/inbound_operation/quic.rs",
        "runtime/inbound_operation/multiplex.rs",
        "runtime/inbound_operation/group.rs",
        "adapters/hysteria2/listener.rs",
    ] {
        assert!(
            !proxy_src().join(retired).exists(),
            "retired listener execution must stay removed: {retired}"
        );
    }
    let adapter = read(&proxy_src().join("adapters/hysteria2.rs"));
    assert!(!adapter.contains("impl InboundListenerCapability"));
    let registry = read(&proxy_src().join("register.rs"));
    assert!(registry.contains("::hysteria2::component::Factory"));
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
    assert!(proxy.contains(".outbound_endpoint(snapshot, leaf)"));
    assert!(!proxy.contains("claim_outbound_leaf"));
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
    assert!(tun.contains("reserve_system_tun_in_config"));
}

#[test]
fn outbound_companion_sockets_use_the_shared_egress_factory() {
    let workspace = workspace_root();
    let sources = [
        workspace.join("crates/transport/src/udp_packet_path.rs"),
        workspace.join("crates/transport/src/quic/client.rs"),
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

    let hysteria2 = read(&workspace.join("protocols/hysteria2/src/transport/connection.rs"));
    assert!(hysteria2.contains("zero_transport::quic::connect_quic_endpoint"));
    assert!(hysteria2.contains("options.socket_factory"));
    assert!(!hysteria2.contains("Endpoint::new"));

    let vless = read(&workspace.join("protocols/vless/src/transport/outbound/direct.rs"));
    assert!(vless.contains("OutboundDatagramSocketFactory"));
    assert!(vless.contains("TokioSocket::connect_addr_on"));
    assert!(!vless.contains("TokioSocket::connect_addr(peer)"));
}

#[test]
fn tun_udp_admission_is_bounded_and_preserves_the_real_source() {
    let tun_udp = read_module(&proxy_src().join("inbound/tun/udp.rs"));
    for required in [
        "MAX_ACTIVE_ASSOCIATIONS",
        "MAX_NEW_ASSOCIATIONS_PER_SECOND",
        "try_send",
        "record_failure",
        "with_source_addr",
        "MAX_CONCURRENT_DNS_QUERIES",
    ] {
        assert!(
            tun_udp.contains(required),
            "TUN UDP runtime must retain `{required}` storm protection"
        );
    }
    assert!(!tun_udp.contains("association.sender.send(datagram).await"));

    let dispatch_model = read(&proxy_src().join("runtime/udp_dispatch/model.rs"));
    let dispatch_backoff = read(&proxy_src().join("runtime/udp_dispatch/backoff.rs"));
    assert!(dispatch_model.contains("UdpFlowStartBackoff"));
    assert!(dispatch_backoff.contains("MAX_TRACKED_FAILURES"));
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

    assert!(!proxy_src().join("runtime/inbound_fallback.rs").exists());
    let fallback_runtime =
        read(&proxy_src().join("runtime/route_runtime/route/source/fallback.rs"));
    let route_runtime = read_module(&proxy_src().join("runtime/route_runtime/route/source.rs"));
    for required in [
        "AcceptedFallback",
        "accepted_tasks()",
        "open_inbound_stream",
        ".replay(",
        "ensure_configuration_committed",
    ] {
        assert!(
            fallback_runtime.contains(required),
            "fallback must retain {required}"
        );
    }
    assert!(!fallback_runtime.contains("Session::new"));
    assert!(route_runtime.contains("AcceptedRequest::Fallback"));

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
        if is_test_source(&path) {
            continue;
        }
        // Test-only module attributes carry no production protocol dependency.
        let source = production_text(&path);
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
fn runtime_protocol_crates_adapters_manifests_and_registration_share_one_feature_inventory() {
    let adapter_features = adapter_feature_names();
    assert_eq!(adapter_features, runtime_protocol_crate_feature_names());

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
    // Each compiled protocol has one assembly entry; no second handler table
    // or executable-provider inventory may enumerate it again.
    for feature in adapter_features {
        let gate = format!("feature = \"{feature}\"");
        assert_eq!(
            adapters
                .matches(&format!("#[cfg({gate})]\npub(crate) mod {feature};"))
                .count(),
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
        if feature == "wireguard" {
            assert!(
                !full_feature.contains("\"wireguard\""),
                "experimental WireGuard must remain opt-in until its audit closes"
            );
        } else {
            assert!(
                full_feature.contains(&format!("\"{feature}\"")),
                "root `full` feature must include protocol `{feature}`"
            );
        }
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
    assert!(resolve
        .split_whitespace()
        .collect::<String>()
        .contains("Proxy{identity:OutboundIdentity,}"));
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
    let protocol_identity = session
        .split_once("pub struct ProtocolType")
        .expect("ProtocolType declaration")
        .1
        .split_once("pub struct SessionAuth")
        .expect("SessionAuth declaration after ProtocolType")
        .0
        .to_ascii_lowercase();
    for protocol in adapter_feature_names() {
        assert!(!protocol_identity.contains(protocol.as_str()));
    }
    assert_non_test_sources_exclude(
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
fn runtime_standard_roles_are_protocol_neutral() {
    for retired in [
        "runtime/tcp_dispatch/operation.rs",
        "runtime/udp_dispatch/operation.rs",
        "runtime/tcp_dispatch/operation/socket.rs",
        "runtime/tcp_dispatch/relay.rs",
    ] {
        assert!(
            !proxy_src().join(retired).exists(),
            "retired executable {retired}"
        );
    }
    let tcp = read(&proxy_src().join("runtime/tcp_dispatch/candidate.rs"));
    let udp = read(&proxy_src().join("runtime/udp_dispatch/outbound.rs"));
    assert!(tcp.contains("PreparedTcpCandidateExecution::Native"));
    assert!(udp.contains("PreparedUdpLeafCandidate::Datagram"));
    for source in [tcp, udp] {
        for forbidden in [
            "ResolvedLeafOutbound",
            "OutboundProtocolConfig",
            "SocketTcpOperation",
            "operation.execute(",
        ] {
            assert!(!source.contains(forbidden));
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
fn tcp_selection_retains_weak_current_stream_generations_without_protocol_executors() {
    let selection = read(&proxy_src().join("inventory/tcp/leaf.rs"));
    let native = read(&proxy_src().join("inventory/tcp/native.rs"));
    assert!(selection.contains("Native(super::native::StreamRoleRef)"));
    assert!(native.contains("Weak<ComponentSlot>"));
    assert!(native.contains("slot.instance_id() == self.instance"));
    for forbidden in [
        "Datagram",
        "UdpFlow",
        "PreparedTcpConnectOperation",
        "Box<dyn",
        "TcpExecutionServices",
        "&'a",
    ] {
        assert!(
            !selection.contains(forbidden),
            "TCP selection retains {forbidden}"
        );
    }
}

#[test]
fn proxy_runtime_does_not_retain_engine_leaf_types() {
    assert_non_test_sources_exclude(&proxy_src().join("runtime"), &["ResolvedLeafOutbound"]);
}

#[test]
fn udp_runtime_dispatch_does_not_retain_engine_leaf_types() {
    assert_non_test_sources_exclude(
        &proxy_src().join("runtime/udp_dispatch"),
        &["ResolvedLeafOutbound"],
    );
}

#[test]
fn udp_managed_bridge_runtime_does_not_retain_engine_leaf_types() {
    assert!(!proxy_src().join("runtime/udp_flow/managed/bridge").exists());
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
fn shadowsocks_legacy_inbound_bridge_is_removed() {
    for retired in [
        "adapters/shadowsocks/inbound.rs",
        "runtime/inbound_operation/tcp_and_datagram.rs",
        "runtime/inbound_operation/tcp_listener.rs",
        "runtime/inbound_operation/context.rs",
    ] {
        assert!(!proxy_src().join(retired).exists());
    }
    let projection = read(&proxy_src().join("adapters/shadowsocks/inbound_component.rs"));
    for forbidden in [
        "tokio::spawn",
        "serve_inbound",
        "JoinSet",
        "TcpAndDatagramInboundListenerOperation",
    ] {
        assert!(!projection.contains(forbidden));
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
    let inventory_inbound = read(&proxy_src().join("inventory/inbound.rs"));
    let runtime_listeners = read_module(&proxy_src().join("runtime/listeners/inbound.rs"));
    assert!(!inbound_operation.contains("PreparedInboundListenerOperation"));
    assert!(!inbound_operation.contains("InboundConnectionContext"));
    assert!(inbound_operation.contains("pending.spawn(TaskClass::Auxiliary"));
    let owners = read_module(&proxy_src().join("runtime/inbound_owners.rs"));
    assert!(
        owners.contains("TaskScope"),
        "the real host retains actual task joins"
    );
    assert!(inventory_inbound.contains("prepare_inbound_bind("));
    assert!(!inventory_inbound.contains("check_inbound_enabled("));
    assert!(!runtime_listeners.contains("check_inbound_enabled("));
    assert!(!inventory_inbound.contains("JoinSet"));
    assert!(!inventory_inbound.contains("listeners.spawn("));
    assert!(!inventory_inbound.contains("operation.execute("));
    assert!(runtime_listeners.contains("listeners.spawn("));
    assert!(!runtime_listeners.contains(".execute(listener_runtime, bound, shutdown_rx)"));
    assert!(runtime_listeners.contains("component::spawn("));
}

#[test]
fn standard_role_execution_keeps_context_narrow_without_a_compatibility_spi() {
    let context = read_module(&proxy_src().join("protocol_registry/context.rs"));
    assert!(context.contains("TcpExecutionServices"));
    assert!(context.contains("UdpAdapterContext"));
    assert!(!context.contains("proxy: &'a Proxy"));
    let leaf = read(&proxy_src().join("inventory/tcp/leaf.rs"));
    assert!(leaf.contains("StreamRoleRef"));
    assert!(!leaf.contains("PreparedTcpConnectOperation"));
    assert!(!proxy_src()
        .join("runtime/tcp_dispatch/operation.rs")
        .exists());
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/operation.rs")
        .exists());
}

#[test]
fn transport_bridge_helpers_drop_raw_leaf_resolution_paths() {
    assert!(!proxy_src().join("adapters/tcp.rs").exists());
    assert!(!proxy_src().join("adapters/udp.rs").exists());
    assert_non_test_sources_exclude(&proxy_src().join("adapters"), &["ResolvedLeafOutbound"]);
}

#[test]
fn udp_selection_keeps_real_association_targets_without_adapter_operations() {
    let selection = read(&proxy_src().join("inventory/udp/leaf.rs"));
    let capability = read(&proxy_src().join("protocol_registry/capability.rs"));
    for forbidden in [
        "UdpFlowResult",
        "UdpRelayResult",
        "prepare_udp_flow",
        "prepare_udp_relay",
        "PreparedUdpFlowOperation",
    ] {
        assert!(!capability.contains(forbidden));
        assert!(!selection.contains(forbidden));
    }
    assert!(selection.contains("Datagram(super::DatagramAssociationTarget)"));
    assert!(selection.contains("DatagramFlow(super::DatagramAssociationTarget)"));
    assert!(!selection.contains("Box<dyn"));
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/operation.rs")
        .exists());
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
    let source = read_module(&proxy_src().join("runtime/route_runtime/route/source.rs"));
    for required in [
        "AcceptedRequest::Stream",
        "AcceptedRequest::Message",
        "AcceptedRequest::Datagram",
        "AcceptedRequest::Multiplex",
        "serve_accepted_stream",
        "serve_accepted_message",
        "runtime.run_requests(source)",
    ] {
        assert!(
            source.contains(required),
            "the standard host must dispatch {required}"
        );
    }
    for retired in [
        "runtime/inbound_route/recorded.rs",
        "runtime/inbound_route/stream.rs",
        "runtime/inbound_route/stream/dispatch.rs",
    ] {
        assert!(!proxy_src().join(retired).exists());
    }
    assert!(!source.contains("run_udp: move |proxy: Proxy"));
    assert!(!source.contains("run_mux: move |proxy: Proxy"));
    let runtime = read(&proxy_src().join("runtime.rs"));
    assert!(!runtime.contains("mod inbound_route;"));
    assert!(!proxy_src().join("runtime/inbound_route.rs").exists());
    assert!(!proxy_src().join("runtime/inbound_route").exists());
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
        .filter(|path| !is_test_source(path))
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
    for relative in ["adapters/vmess/inbound.rs", "adapters/trojan/inbound.rs"] {
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
        ("adapters/vmess.rs", "OwnedVmessOutboundLeafConfig"),
        (
            "adapters/vmess/inbound.rs",
            "OwnedVmessInboundListenerConfig",
        ),
        ("adapters/trojan.rs", "OwnedTrojanOutboundLeafConfig"),
        (
            "adapters/trojan/inbound.rs",
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
        (
            "adapters/shadowsocks/inbound_component.rs",
            "OwnedShadowsocksInbound",
        ),
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
    let adapter = read_module(&proxy_src().join("adapters/vless.rs"));
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
            !transport.lines().filter(|line| line.trim_start().starts_with("pub use ")).any(|line| line.contains(forbidden)),
            "protocols/vless/src/transport.rs must not re-export protocol-private VLESS profile surface `{forbidden}`"
        );
    }
}

#[test]
fn vless_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read_module(&proxy_src().join("adapters/vless.rs"));
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

    assert!(!proxy_src().join("adapters/vless/listener.rs").exists());
    assert!(!adapter.contains("InboundListenerCapability"));
    assert!(!adapter.contains("prepare_inbound_listener"));
}

#[test]
fn vmess_adapter_uses_protocol_outbound_option_refs() {
    let adapter = read_module(&proxy_src().join("adapters/vmess.rs"));
    for required in [
        "VmessOutboundBuildOptionsRef",
        "VmessOutboundOptionsRef",
        "VmessTransportRuntime",
        "::vmess::component::Options::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "adapters/vmess.rs should project through protocol-owned VMess outbound option surface `{required}`"
        );
    }
}

#[test]
fn standard_stream_sources_supervise_the_actual_grpc_connection_and_hunk_io() {
    let carrier = read(&workspace_root().join("crates/transport/src/grpc/scoped_server.rs"));
    let compact: String = carrier.split_whitespace().collect();
    for required in [
        "caller.spawn",
        "io_jobs.push",
        "builder.handshake(io)",
        "conn.accept()",
    ] {
        assert!(
            compact.contains(required),
            "scoped carrier must retain {required}"
        );
    }
    for forbidden in ["tokio::spawn", ".abort(", "CloseReport"] {
        assert!(!carrier.contains(forbidden));
    }
    for protocol in ["vmess", "trojan"] {
        let request =
            read(&workspace_root().join(format!("protocols/{protocol}/src/transport/inbound.rs")));
        let standard = request
            .split("pub async fn accept_standard_request")
            .nth(1)
            .unwrap();
        assert!(standard.contains("accept_tls_inbound_stream_stack_scoped"));
    }
}

#[test]
fn standard_vmess_and_trojan_sources_enable_actual_datagram_ownership_without_default_features() {
    let manifest = read(&proxy_src().parent().unwrap().join("Cargo.toml"));
    for protocol in ["vmess", "trojan"] {
        let stanza = manifest
            .split(&format!("{protocol} = ["))
            .nth(1)
            .unwrap()
            .split(']')
            .next()
            .unwrap();
        assert!(stanza.contains("managed-datagram-runtime"), "{protocol} requires real Datagram ownership and same-Scope cleanup in an independent feature build");
    }
}

#[test]
fn vmess_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/vmess/inbound.rs"));
    for required in [
        "VmessInboundOptionsRef",
        "VmessInboundListenerRequest::from_options_refs_staged",
        "&self.runtime",
    ] {
        assert!(adapter.contains(required));
    }
    assert!(!proxy_src().join("adapters/vmess/listener.rs").exists());
    assert!(!read(&proxy_src().join("adapters/vmess.rs")).contains("InboundListenerCapability"));
    assert!(
        !read(&workspace_root().join("protocols/vmess/src/transport/inbound.rs"))
            .contains("VmessInboundAcceptedRoute")
    );
    let source = read(&workspace_root().join("protocols/vmess/src/component/inbound/listener.rs"));
    for required in [
        "impl ComponentFactory for Factory",
        "tcp::source",
        "erase_inbound",
        "prepared.activate()",
    ] {
        assert!(source.contains(required));
    }
    for forbidden in [
        "InboundProtocolConfig",
        "zero_proxy",
        "zero_runtime",
        "tokio::spawn",
        "PreparedInboundListenerOperation",
    ] {
        assert!(!source.contains(forbidden));
    }
    let mux = read(&workspace_root().join("protocols/vmess/src/mux/standard.rs"));
    assert!(mux.contains("caller.spawn"));
    assert!(mux.contains("Arc::new(Mutex::new(responder))"));
    assert!(!mux.contains("tokio::spawn"));
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
    let adapter = read_module(&proxy_src().join("adapters/trojan.rs"));
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
        "::trojan::component::Options::from_options_refs",
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
            "fn prepare_options",
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
    let adapter = read_module(&proxy_src().join("adapters/hysteria2.rs"));
    let forbidden = "Hysteria2InboundBindPlan::from_paths";
    assert!(
        !adapter.contains(forbidden),
        "hysteria2 adapter module must not build hysteria2 bind plans via `{forbidden}`"
    );
    for required in [
        "Hysteria2InboundBindOptionsRef",
        "Hysteria2InboundBindPlan::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "hysteria2 adapter module should project through protocol-owned hysteria2 bind option surface `{required}`"
        );
    }
}

#[test]
fn hysteria2_adapter_uses_protocol_outbound_option_refs() {
    let adapter = read_module(&proxy_src().join("adapters/hysteria2.rs"));
    let forbidden = "Hysteria2TransportLeaf::new(";
    assert!(
        !adapter.contains(forbidden),
        "adapters/hysteria2.rs must not construct hysteria2 transport leaves via `{forbidden}`"
    );
    for required in ["Hysteria2OutboundOptionsRef", "Profile::from_options"] {
        assert!(
            adapter.contains(required),
            "adapters/hysteria2.rs should project outbound through protocol-owned hysteria2 option surface `{required}`"
        );
    }
}

#[test]
fn hysteria2_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read_module(&proxy_src().join("adapters/hysteria2.rs"));
    for required in [
        "Hysteria2InboundOptionsRef",
        "Hysteria2AuthenticatedInboundProfile::from_options_refs",
    ] {
        assert!(
            adapter.contains(required),
            "hysteria2 adapter module should project through protocol-owned hysteria2 inbound option surface `{required}`"
        );
    }

    let projection = read(&proxy_src().join("adapters/hysteria2/component.rs"));
    assert!(projection.contains("::hysteria2::component::Options::new"));
    for forbidden in [
        "bind_inbound",
        "prepare_inbound_listener",
        "tokio::spawn",
        "JoinSet",
        "serve_inbound",
    ] {
        assert!(
            !projection.contains(forbidden),
            "thin projection must not execute {forbidden}"
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
    let listener =
        read(&workspace_root().join("protocols/shadowsocks/src/component/inbound/listener.rs"));
    let accepted = listener.find(".accept_stream_facts(").unwrap();
    let handed = listener.find("let facts = connection.request(").unwrap();
    assert!(accepted < handed);
    assert!(listener.contains("super::TcpReply::new"));
    assert!(!listener.contains("Session::new"));
    let inbound = read_module(&workspace_root().join("protocols/shadowsocks/src/inbound.rs"));
    assert!(
        inbound.contains("pub async fn accept_stream_facts"),
        "authenticated SS facts and cipher state must originate in the protocol"
    );
    assert!(
        !inbound.contains("accept_and_dispatch_stream"),
        "protocols/shadowsocks/src/inbound.rs must stop at accepted facts/stream instead of owning runtime handoff helpers"
    );
    let standard = read_module(&proxy_src().join("runtime/tcp_ingress/runtime/standard.rs"));
    for forbidden in [
        "decrypt_tcp_chunk",
        "derive_session_key",
        "parse_target_data",
        "SS_2022",
        "ShadowsocksAeadStream",
    ] {
        assert!(
            !listener.contains(forbidden),
            "SS adapter must not parse wire or own crypto via `{forbidden}`"
        );
        assert!(
            !standard.contains(forbidden),
            "generic standard ingress must not parse SS wire via `{forbidden}`"
        );
    }
}

#[test]
fn mieru_standard_factory_owns_wire_acceptance_and_returns_standard_roles() {
    let adapter = read(&proxy_src().join("adapters/mieru/inbound.rs"));
    for forbidden in [
        ".accept_multiplexer(",
        ".run_route_multiplexer(",
        "MieruInboundAcceptedSession::",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "Mieru adapter only projects config: {forbidden}"
        );
    }
    let source = read_module(&workspace_root().join("protocols/mieru/src/component/inbound.rs"));
    assert!(source.contains("impl ComponentFactory for Factory"));
    assert!(source.contains("AcceptedRequest::Multiplex"));
    assert!(
        !source.contains("tokio::spawn"),
        "real original driver tasks use the host's accepted Scope"
    );
    let creates_business_session = source.match_indices("Session::new(").any(|(start, _)| {
        start == 0 || !matches!(source.as_bytes()[start - 1], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
    });
    assert!(
        !creates_business_session,
        "physical/unfinished logical handshakes cannot invent business sessions"
    );
    let inbound = read(&workspace_root().join("protocols/mieru/src/inbound.rs"));
    assert!(!inbound.contains("pub async fn dispatch<"));
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
    let request =
        read_module(&workspace_root().join("protocols/vless/src/transport/inbound/request.rs"));
    assert!(request.contains(".accept_standard_facts(&mut stream)"));
    assert!(request.contains("accept_standard_stream"));
    for retired in [
        "accept_recorded_tcp_route",
        "accept_tcp_route",
        ".accept_route_owned_with_sni_or_else(",
    ] {
        assert!(!plan.contains(retired));
    }
    let inbound = read(&workspace_root().join("protocols/vless/src/inbound.rs"));
    assert!(!inbound.contains("pub async fn accept_route_owned_with"));
    assert!(!proxy_src().join("adapters/vless/listener.rs").exists());
    let mkcp = read(&workspace_root().join("protocols/vless/src/transport/inbound/mkcp.rs"));
    assert!(!mkcp.contains("accept_recorded_mkcp_route"));
    assert!(!mkcp.contains("accept_mkcp_peer"));
}

#[test]
fn socks5_standard_source_accepts_protocol_requests_before_runtime_handoff() {
    let protocol =
        read(&workspace_root().join("protocols/socks5/src/component/inbound/connection.rs"));
    for required in [
        ".accept_command_facts(&mut socket)",
        "Socks5Command::Connect",
        "Socks5Command::UdpAssociate",
        "AcceptedRequest::Stream",
        "AcceptedRequest::Datagram",
        "self.facts.request(",
    ] {
        assert!(
            protocol.contains(required),
            "standard protocol handoff must retain {required}"
        );
    }
    assert!(!proxy_src()
        .join("adapters/socks5/inbound/listener.rs")
        .exists());
    let adapter = read(&proxy_src().join("adapters/socks5.rs"));
    assert!(!adapter.contains("impl InboundListenerCapability"));
    let listener = read(&workspace_root().join("protocols/socks5/src/component/listener.rs"));
    assert!(listener.contains("impl ComponentFactory"));
    assert!(listener.contains("tcp::source("));
    assert!(!protocol.contains("Session::new"));
    assert!(!protocol.contains("tokio::spawn"));
    assert!(!protocol.contains("serve_inbound"));
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
fn mieru_outbound_uses_its_actual_standard_pool() {
    let adapter = read_module(&proxy_src().join("adapters/mieru/component.rs"));
    assert!(adapter.contains("component::{PoolFactory, PoolOptions"));
    assert!(!proxy_src().join("adapters/mieru/tcp.rs").exists());
    assert!(!proxy_src().join("adapters/mieru/udp.rs").exists());
}

#[test]
fn mieru_library_keeps_its_protocol_owned_managed_connector_flow_type() {
    assert!(!proxy_src().join("adapters/mieru/udp.rs").exists());
    let protocol = read(&workspace_root().join("protocols/mieru/src/transport/managed_udp.rs"));
    assert!(protocol.contains("pub type MieruManagedUdpConnectorFlow"));
}

#[test]
fn mieru_inbound_projects_standard_options_without_restoring_listener_execution() {
    let adapter = read(&proxy_src().join("adapters/mieru.rs"));
    assert!(!adapter.contains("impl InboundListenerCapability"));
    assert!(!adapter.contains("MieruInboundListenerRequest"));
    let projection = read(&proxy_src().join("adapters/mieru/inbound.rs"));
    for required in [
        "::mieru::component::inbound::Options",
        "::mieru::inbound::MieruInboundProfile::from_config_users",
    ] {
        assert!(
            projection.contains(required),
            "Mieru config stays a thin projection into protocol-owned values: {required}"
        );
    }
    for forbidden in [
        "MieruInboundListenerRequest",
        "PeerRouteInboundListenerOperation",
        "tokio::spawn",
        "JoinSet",
        "PacketCodec",
        "serve_inbound",
    ] {
        assert!(
            !projection.contains(forbidden),
            "Mieru adapter must not own listener, tasks or wire parsing: {forbidden}"
        );
    }
    let peer = read(&proxy_src().join("runtime/inbound_operation/source/peer.rs"));
    assert!(
        !peer.contains("::mieru"),
        "native peer ownership is protocol-neutral"
    );
    assert!(
        !peer.contains("AcceptedTcp"),
        "native peer admission must not fabricate a TCP entry"
    );
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
    let projection = read(&proxy_src().join("adapters/shadowsocks/inbound_component.rs"));
    for required in [
        "prepare_with_identity",
        "Options::new",
        ".with_plugin(plugin.clone())",
    ] {
        assert!(projection.contains(required));
    }
    let adapter = read(&proxy_src().join("adapters/shadowsocks.rs"));
    assert!(!adapter.contains("impl InboundListenerCapability"));
    let protocol = read_module(
        &workspace_root().join("protocols/shadowsocks/src/component/inbound/listener.rs"),
    );
    for required in [
        "ListenerHandoff",
        "ResourceDeclaration::Process",
        "spawn_process",
        "TaskClass::Critical",
        "try_wait",
        "StartupAttempt",
    ] {
        assert!(
            protocol.contains(required),
            "missing actual plugin owner {required}"
        );
    }
    for forbidden in [
        "tokio::spawn",
        "process::spawn(",
        "UdpSocket::bind",
        "TcpStream::connect",
    ] {
        assert!(
            !protocol.contains(forbidden),
            "selected sources must use declared original providers: {forbidden}"
        );
    }
    assert!(!workspace_root()
        .join("crates/transport/src/inbound_carrier.rs")
        .exists());
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
    let adapter = read(&proxy_src().join("adapters/socks5/inbound.rs"));
    assert!(adapter.contains("Socks5InboundUserRef"));
    assert!(adapter.contains("InboundOptions::new("));
    for forbidden in [
        "Socks5InboundTcpAcceptor",
        ".accept_command_facts(",
        "TcpInboundListenerOperation",
        "serve_inbound",
        "tokio::spawn",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "thin projection must not contain {forbidden}"
        );
    }
    let protocol = read(&workspace_root().join("protocols/socks5/src/component/listener.rs"));
    assert!(protocol.contains("Socks5InboundAcceptor::from_options_refs"));
}

#[test]
fn mixed_standard_source_reuses_protocol_owned_options_and_accepted_roles() {
    let listener = read(&proxy_src().join("adapters/mixed/inbound.rs"));
    for required in [
        "socks5::inbound::options(",
        "impl ComponentFactory",
        "tcp::source(",
        "component::inbound::Connection::from_stream",
        "http::component::Requests::from_stream",
    ] {
        assert!(
            listener.contains(required),
            "mixed source must retain {required}"
        );
    }
    for forbidden in [
        "InboundListenerCapability",
        "TcpInboundListenerOperation",
        "tokio::spawn",
        "serve_inbound",
        "Socks5InboundTcpAcceptor",
    ] {
        assert!(
            !listener.contains(forbidden),
            "mixed source must not restore {forbidden}"
        );
    }
}

#[test]
fn trojan_inbound_projection_happens_at_adapter_boundary() {
    let adapter = read(&proxy_src().join("adapters/trojan/inbound.rs"));
    for required in [
        "TrojanInboundOptionsRef",
        "TrojanInboundListenerRequest::from_options_refs",
    ] {
        assert!(adapter.contains(required));
    }
    assert!(!proxy_src().join("adapters/trojan/listener.rs").exists());
    assert!(!read(&proxy_src().join("adapters/trojan.rs")).contains("InboundListenerCapability"));
    let source = read(&workspace_root().join("protocols/trojan/src/component/inbound/listener.rs"));
    for required in [
        "impl ComponentFactory for Factory",
        "tcp::source",
        "erase_inbound",
        "update.commit()",
    ] {
        assert!(source.contains(required));
    }
    for forbidden in [
        "InboundProtocolConfig",
        "zero_proxy",
        "zero_runtime",
        "tokio::spawn",
        "PreparedInboundListenerOperation",
    ] {
        assert!(!source.contains(forbidden));
    }
}

#[test]
fn heavy_transport_bridge_adapters_use_protocol_owned_outbound_options_directly() {
    for (relative, variant, options) in [
        (
            "adapters/vless.rs",
            "OutboundProtocolConfig::Vless",
            "VlessOutboundBuildOptionsRef",
        ),
        (
            "adapters/vmess.rs",
            "OutboundProtocolConfig::Vmess",
            "VmessOutboundBuildOptionsRef",
        ),
        (
            "adapters/trojan.rs",
            "OutboundProtocolConfig::Trojan",
            "TrojanOutboundBuildOptionsRef",
        ),
    ] {
        let source = read_module(&proxy_src().join(relative));
        assert!(source.contains("fn outbound_options<'a>"));
        assert!(source.contains(variant));
        assert!(source.contains(options));
        assert!(!source.contains("claim_outbound_leaf"));
        assert!(!source.contains("OutboundLeafInput"));
    }
}

#[test]
fn active_ingress_preparation_receives_one_outbound_and_runtime_owns_inventory() {
    let capability = read(&proxy_src().join("protocol_registry/service.rs"));
    assert!(capability.contains("outbound: &OutboundConfig"));
    assert!(capability.contains("Result<Option<NativeServiceOperation>, EngineError>"));
    assert!(!capability.contains("source_dir"));
    assert!(!capability.contains("Box<dyn"));
    assert!(!capability.contains("RuntimeConfig"));
    let registry = read(&proxy_src().join("protocol_registry/registry/service.rs"));
    assert!(registry.contains("for outbound in &config.outbounds"));
    let adapter = read(&proxy_src().join("adapters/vless/reverse/service.rs"));
    for forbidden in [
        "RuntimeConfig",
        "config.outbounds",
        "tokio::spawn",
        "JoinSet",
        "serve_inbound",
    ] {
        assert!(!adapter.contains(forbidden));
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
    assert!(!dispatch.contains("UdpPacketRef"));
    assert!(!dispatch.contains("ResolvedLeafOutbound"));
    assert!(!dispatch.contains("enum UdpCandidate"));
    assert!(!dispatch.contains(".outbound_leaf_runtime("));
    assert!(!dispatch.contains("start_udp_leaf_flow("));
    assert!(!dispatch.contains("start_udp_relay_chain("));
}

#[test]
fn inventory_tcp_dispatch_selects_standard_nodes_without_a_whole_chain_claim() {
    let dispatch = read(&proxy_src().join("inventory/tcp/outbound.rs"));
    let selection = read(&proxy_src().join("inventory/tcp/native.rs"));
    assert!(!dispatch.contains("ClaimedResolvedOutbound"));
    assert!(!dispatch.contains("claim_tcp_outbound(resolved)?"));
    assert!(dispatch.contains("match resolved"));
    assert!(dispatch.contains("prepare_tcp_outbound<'a>("));
    assert!(!dispatch.contains("async fn"));
    assert!(!dispatch.contains(".await"));
    assert!(!dispatch.contains(".execute("));
    assert!(dispatch.contains("prepare_tcp_candidate("));
    assert!(dispatch.contains("prepare_native_tcp_relay_chain("));
    assert!(!dispatch.contains("claim_relay_chain("));
    assert!(selection.contains("prepare_native_tcp_relay_chain"));
    assert!(selection.contains("PreparedTcpCandidateExecution::Native"));
    assert!(!selection.contains("last_legacy"));
    assert!(!selection.contains(".await"));
    assert!(!selection.contains(".execute("));
    assert!(!selection.contains("&Proxy"));
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
    for relative in ["inventory/udp/leaf.rs", "inventory/udp/standard_relay.rs"] {
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
    let udp_relay = read(&proxy_src().join("inventory/udp/standard_relay.rs"));
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
fn packet_codec_relay_retains_only_the_actual_standard_role_graph() {
    assert!(!proxy_src().join("inventory/udp/packet_path.rs").exists());
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/packet_path_operation.rs")
        .exists());
    let model = read(&proxy_src().join("runtime/udp_flow/packet_path_chain/model.rs"));
    assert!(model.contains("struct PacketPathCarrierRequest"));
    assert!(model.contains("RolePacketPath"));
    assert!(!model.contains("Compatibility"));
    let entry = read(&proxy_src().join("runtime/udp_flow/packet_path_chain/entry.rs"));
    assert!(entry.contains("carrier.0.build(services.clone()).await?"));
    assert!(!entry.contains("operation.build_carrier"));
}

#[test]
fn runtime_tcp_relay_executes_prepared_chain_without_engine_leaf_roundtrip() {
    let outbound = read(&proxy_src().join("runtime/tcp_dispatch/outbound.rs"));
    assert!(outbound.contains("PreparedTcpOutbound::NativeRelay"));
    assert!(outbound.contains("stream_bridge::connect_relay"));
    assert!(!outbound.contains("ResolvedLeafOutbound"));
    let relay = read(&proxy_src().join("component/stream_bridge/relay.rs"));
    assert!(relay.contains("StreamRoleRef") || relay.contains("node.slot"));
    assert!(!relay.contains("Session::new"));
    for retired in [
        "runtime/tcp_dispatch/relay.rs",
        "runtime/tcp_dispatch/relay/hop.rs",
        "component/stream_bridge/relay/selected_prefix.rs",
        "component/stream_bridge/relay/hop.rs",
    ] {
        assert!(!proxy_src().join(retired).exists());
    }
}

#[test]
fn inventory_udp_relay_selects_actual_standard_roles_or_existing_packet_codecs() {
    assert!(!proxy_src().join("inventory/udp/relay.rs").exists());
    let outbound = read(&proxy_src().join("inventory/udp/outbound.rs"));
    assert!(outbound.contains("prepare_standard_datagram_relay"));
    assert!(outbound.contains("prepare_standard_packet_relay"));
    assert!(!outbound.contains("into_udp_relay"));
}

#[test]
fn inventory_relay_preparation_keeps_carrier_execution_in_runtime() {
    for relative in [
        "inventory/tcp/native.rs",
        "inventory/tcp/leaf.rs",
        "inventory/udp/standard_relay.rs",
    ] {
        let relay = read(&proxy_src().join(relative));
        for forbidden in [
            "use crate::runtime::Proxy",
            "&Proxy",
            ".await",
            ".execute(",
            "tokio::spawn",
            "TcpStream::connect(",
            "UdpSocket::bind(",
            "dispatch_prepared_tcp_candidate(",
            "dispatch_prepared_tcp_relay_carrier(",
            "resolve_route_decision(",
        ] {
            assert!(
                !relay.contains(forbidden),
                "{relative} must leave runtime execution outside cold preparation: {forbidden}"
            );
        }
    }
}

#[test]
fn runtime_packet_codec_relay_executes_the_original_packet_path() {
    let runtime = read(&proxy_src().join("runtime/udp_dispatch/relay.rs"));
    assert!(runtime.contains("send_packet_path_chain"));
    assert!(!runtime.contains("bind_final_hop") && !runtime.contains("bind_two_stream"));
    assert!(!runtime.contains("ResolvedLeafOutbound"));
}

#[test]
fn retired_two_stream_proxy_execution_does_not_return() {
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/operation/transport/relay.rs")
        .exists());
    assert!(!proxy_src().join("inventory/udp/datagram_relay.rs").exists());
}

#[test]
fn managed_stream_connectors_use_runtime_services_instead_of_proxy() {
    assert!(!proxy_src()
        .join("runtime/udp_flow/managed/stream_manager/connector")
        .exists());
}

#[test]
fn retired_socket_datagram_manager_is_replaced_by_registered_standard_upstream() {
    assert!(!proxy_src()
        .join("runtime/udp_flow/managed/datagram_manager/connector")
        .join("socket.rs")
        .exists());
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/managed/start/datagram.rs")
        .exists());
    let source = read_module(&proxy_src().join("runtime/udp_flow/registered/upstream.rs"));
    assert!(!source.contains("ShadowsocksManagedDatagramFlowResume"));
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
    assert!(!proxy_src()
        .join("runtime/udp_flow/managed/bridge/stream_packet")
        .exists());
}

#[test]
fn managed_udp_forward_paths_use_runtime_services_instead_of_proxy() {
    {
        let relative = "runtime/udp_flow/registered/forward.rs";
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
fn direct_datagram_selection_uses_the_original_provider_without_a_boxed_adapter_operation() {
    let direct = read(&proxy_src().join("runtime/udp_dispatch/direct.rs"));
    assert!(direct.contains(".admit_bound_direct(&target, session)"));
    assert!(direct.contains("resolve_direct_targets"));
    assert!(direct.contains("send_new_direct_packet"));
    assert!(!direct.contains("impl PreparedUdpFlowOperation"));
    let selection = read(&proxy_src().join("inventory/udp/leaf.rs"));
    assert!(selection.contains(".map(PreparedUdpLeafCandidate::Direct)"));
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/operation/direct.rs")
        .exists());
    let adapter = read(&proxy_src().join("adapters/direct.rs"));
    assert!(!adapter.contains("DirectUdpFlowOperation"));
    assert!(!adapter.contains("PreparedUdpFlowOperation"));
}

#[test]
fn retired_transport_leaf_udp_execution_does_not_return() {
    assert!(!proxy_src().join("adapters/udp.rs").exists());
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/operation/transport.rs")
        .exists());
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
    let source = read(&proxy_src().join("runtime/udp_socket.rs"));
    assert!(!source.contains("use crate::runtime::Proxy"));
    assert!(!source.contains("&Proxy"));
}

#[test]
fn temp_wg_has_no_parallel_native_slot_or_preopened_peer_carrier_entry() {
    for relative in [
        "runtime/udp_dispatch/packet_owner.rs",
        "runtime/udp_dispatch/packet_owner",
        "runtime/udp_flow/packet_path_chain/carriers/udp_socket_carrier.rs",
        "runtime/udp_flow/packet_path_chain/carriers/udp_socket_carrier/owned.rs",
    ] {
        assert!(!proxy_src().join(relative).is_file(), "retired {relative}");
    }
    for relative in [
        "runtime/raw_ip/carrier.rs",
        "runtime/udp_flow/packet_path/carrier.rs",
        "runtime/udp_flow/packet_path_chain/carriers/encoded.rs",
        "runtime/udp_flow/packet_path_chain/carriers/observed.rs",
        "adapters/wireguard/lifecycle/pending.rs",
        "adapters/wireguard/lifecycle/pending/close.rs",
        "adapters/wireguard/inbound/io.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        for retired in [
            "PacketCarrierOwnerSlot",
            "native_owner(",
            "carrier_owners_match",
            "peer_carriers",
        ] {
            assert!(!source.contains(retired), "{relative}: {retired}");
        }
    }
    let peers = read(&proxy_src().join("runtime/inbound_operation/raw_ip.rs"));
    let peers = peers
        .split("pub(crate) struct EndpointPeerState {")
        .nth(1)
        .unwrap()
        .split('}')
        .next()
        .unwrap();
    assert!(!peers.contains("RawIpWireCarrier"));
    let outer = read(&proxy_src().join("runtime/inbound_operation/raw_ip/outer.rs"));
    assert!(!outer.contains("peers.carriers"));
    let provider = read(&proxy_src().join("runtime/raw_ip/carrier/registered/retirement.rs"));
    assert!(provider.contains("retire_driver_handoff"));
    assert!(provider.contains("self.execution.retire_carriers"));
    assert!(provider.contains("wait_registration_released"));
    assert!(provider.contains("report.is_complete()"));
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

    let association =
        read_module(&proxy_src().join("runtime/route_runtime/route/source/datagram.rs"));
    assert!(association.contains("UdpIngressRuntime"));
    assert!(!association.contains("use crate::runtime::Proxy"));
    assert!(!association.contains("proxy: &Proxy"));
    assert!(!proxy_src().join("runtime/udp_association.rs").exists());

    for relative in [
        "runtime/route_runtime/route/source/datagram.rs",
        "runtime/datagram_udp/lifecycle.rs",
        "runtime/packet_session_udp/lifecycle.rs",
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

    for relative in [
        "runtime/datagram_udp/lifecycle.rs",
        "runtime/packet_session_udp/lifecycle.rs",
        "runtime/route_runtime/route/source/datagram.rs",
    ] {
        let source = read_module(&proxy_src().join(relative));
        assert!(
            !source.contains("&Proxy"),
            "{relative} must use narrow runtime services"
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
    assert!(!route_runtime.contains("struct MuxSubstreamRuntime"));

    let inbound_operation = read_module(&proxy_src().join("runtime/inbound_operation.rs"));
    assert!(inbound_operation.contains("InboundListenerRuntime"));
    assert!(!inbound_operation.contains("proxy: Proxy"));
    assert!(!inbound_operation.contains("execute(proxy"));

    assert!(!proxy_src()
        .join("runtime/inbound_operation/context.rs")
        .exists());
    let source = read_module(&proxy_src().join("runtime/route_runtime/route/source.rs"));
    for required in [
        "PendingInbound",
        "RequestMultiplexer",
        "register_principal_cancellation",
    ] {
        assert!(source.contains(required));
    }

    let inventory_inbound = read(&proxy_src().join("inventory/inbound.rs"));
    assert!(!inventory_inbound.contains("PreparedInboundListenerOperation"));
    assert!(!inventory_inbound.contains("InboundListenerRuntime"));
    assert!(!inventory_inbound.contains("JoinSet"));
    assert!(!inventory_inbound.contains("operation.execute("));
    assert!(!inventory_inbound.contains("use crate::runtime::Proxy"));
    assert!(!inventory_inbound.contains("execute(proxy.clone()"));

    let listener_inbound = read_module(&proxy_src().join("runtime/listeners/inbound.rs"));
    assert!(listener_inbound.contains("ProtocolInventory"));
    assert!(listener_inbound.contains("InboundListenerRuntimeFactory"));
    assert!(listener_inbound.contains("listeners.spawn("));
    assert!(!listener_inbound.contains(".execute(listener_runtime, bound, shutdown_rx)"));
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

    for relative in ["runtime/listener_loop/tcp.rs", "inbound/system/source.rs"] {
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
        "runtime/route_runtime/route/source.rs",
        "runtime/inbound_service/native.rs",
        "runtime/route_runtime/route/source.rs",
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
fn legacy_quic_listener_execution_is_removed_after_standard_source_migration() {
    for retired in [
        "runtime/listener_loop/quic.rs",
        "runtime/listener_loop/quic/connection.rs",
        "runtime/inbound_operation/quic_connection.rs",
    ] {
        assert!(!proxy_src().join(retired).exists());
    }
    let source = read_module(&workspace_root().join("crates/transport/src/quic/source.rs"));
    for required in [
        "OwnedEndpointOwner",
        "MetadataUsersCompletion",
        "TaskRegistrar",
        "confirm_metadata_users",
    ] {
        assert!(source.contains(required), "source must retain {required}");
    }
}

#[test]
fn listener_loop_tcp_root_stays_facade_only() {
    let root = read(&proxy_src().join("runtime/listener_loop.rs"));
    assert!(root.contains(
        "#[cfg(test)]
mod tcp;"
    ));
    let tcp_root = read(&proxy_src().join("runtime/listener_loop/tcp.rs"));
    let tcp = read_module(&proxy_src().join("runtime/listener_loop/tcp.rs"));
    for module in ["mod connection;", "mod owned;"] {
        assert!(tcp_root.contains(module));
    }
    for required in [
        "pub(crate) struct TcpListenerLoopRequest<H>",
        "pub(crate) async fn run_tcp_listener_loop<H, Fut>(",
        "pub(crate) struct OwnedLoop<H>",
    ] {
        assert!(!tcp_root.contains(required));
        assert!(tcp.contains(required));
    }
    assert!(!proxy_src()
        .join("runtime/listener_loop/tcp/logged.rs")
        .exists());
    assert!(!tcp.contains("LoggedTcpSocketListenerRequest"));
    let production = read_module(&proxy_src().join("runtime/inbound_operation/source.rs"));
    for required in [
        "DynInboundSource",
        "source.next(AcceptContext",
        "pending.accepted_tcp()",
        "pending.spawn(TaskClass::Auxiliary",
        "with_accepted_tasks",
        "complete_request",
    ] {
        assert!(
            production.contains(required),
            "production source must retain {required}"
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
        "pub(crate) async fn prepare_udp_session",
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
        "pub(crate) async fn prepare_udp_session",
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
        "pub(crate) async fn prepare_session",
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
        "pub(crate) async fn prepare_session",
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
    let current = read_module(&proxy_src().join("runtime/tcp_ingress/runtime/standard.rs"));
    assert!(current.contains("serve_accepted_stream"));
    assert!(current.contains("serve_with_identity"));
    let lifecycle_root = read(&proxy_src().join("runtime/tcp_ingress/lifecycle.rs"));
    let lifecycle = read_module(&proxy_src().join("runtime/tcp_ingress/lifecycle.rs"));
    assert!(!lifecycle.contains("serve_inbound_with_client_response"));
    for module_name in ["mod rate_limit;", "mod result;", "mod serve;"] {
        assert!(lifecycle_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn serve_inbound<",
        "pub(crate) async fn serve_inbound_with_client_response",
        "pub(crate) fn apply_kernel_rate_limits_from_config",
        "activity.wait_for_idle(idle_timeout)",
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
        "activity.wait_for_idle(idle_timeout)",
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
    assert!(!proxy_src()
        .join("runtime/tcp_ingress/contract/client_response.rs")
        .exists());
    assert!(!contract.contains("struct ClientResponseInboundProtocol"));
    assert!(!contract.contains("use client_response::"));
    for module_name in ["mod accounting;", "mod no_response;", "mod protocol;"] {
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
        "pub(crate) struct NoClientResponseStreamProtocol",
        "TcpRuntimeServices",
        "TcpRelayActivity",
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
    let relay = read(&proxy_src().join("inventory/tcp/native.rs"));
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
        "mod fake_ip;",
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
        "CommandRequest::FakeIpClear",
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
        "CommandRequest::FakeIpClear",
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
        "pub(super) struct Waiter",
        "pub(super) async fn dispatch_via_entry(",
        "pub(super) async fn recv_loop(",
    ] {
        assert!(
            !bridge_root.contains(forbidden),
            "udp_flow packet_path_chain bridge facade root must not keep `{forbidden}` inline"
        );
    }
    for expected in [
        "PacketMetadataArc<Waiters>",
        "struct Waiter",
        "fn remove_id(",
        "async fn dispatch_via_entry(",
        "async fn recv_loop(",
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
        "pub(crate) fn finish_all(",
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
        "pub(crate) fn finish_all(",
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
    for module_name in ["mod context;", "mod upstream;"] {
        assert!(start_root.contains(module_name));
    }
    assert!(!start_root.contains("mod datagram;"));
    for retired in [
        "start_transport_managed_socket",
        "start_transport_managed_datagram",
    ] {
        assert!(!start.contains(retired));
    }
    for expected in [
        "pub(crate) fn flow_start_context(&mut self)",
        "pub(crate) async fn start_datagram",
    ] {
        assert!(!start_root.contains(expected));
        assert!(start.contains(expected));
    }
}

#[test]
fn active_ingress_has_only_standard_source_and_accepted_scope_execution() {
    for retired in [
        "runtime/mux_session.rs",
        "runtime/mux_session",
        "runtime/mux_tcp.rs",
        "runtime/route_runtime/mux.rs",
        "runtime/mux_udp/handler.rs",
        "runtime/mux_udp/relay.rs",
        "runtime/mux_udp/task.rs",
    ] {
        assert!(
            !proxy_src().join(retired).exists(),
            "retired executor: {retired}"
        );
    }
    let operation = read(&proxy_src().join("runtime/inbound_service/operation.rs"));
    assert!(operation.contains("native: super::native::Installed"));
    assert!(!operation.contains("Option<super::native::Installed>"));
    let source = read(&proxy_src().join("runtime/inbound_service/native.rs"));
    for required in [
        "source.next(",
        "runtime.complete_request(pending",
        "reserve()",
        "install_group(graph.clone(), witness)",
        "for_stream_carrier(",
    ] {
        assert!(
            source.contains(required),
            "actual standard source duty: {required}"
        );
    }
    let adapter = read(&proxy_src().join("adapters/vless/reverse/service.rs"));
    assert!(adapter.contains("::vless::reverse::source::Factory"));
    assert!(!adapter.contains("VlessTransportRuntime::default"));
    assert_non_test_sources_exclude(
        &proxy_src().join("runtime"),
        &[
            "DialedMuxSource",
            "MuxServiceOperation",
            "MuxSubstreamRuntime",
            "PreparedInboundServiceOperation",
            "run_protocol_mux_session",
            "run_protocol_mux_udp",
            "sniff_mux_tcp_session",
        ],
    );
}

#[test]
fn mux_udp_keeps_only_shared_continuity_and_standard_datagram_routing() {
    let root = read(&proxy_src().join("runtime/mux_udp.rs"));
    assert!(root.contains("mod continuity;"));
    for forbidden in ["mod handler;", "mod relay;", "mod task;"] {
        assert!(!root.contains(forbidden));
    }
    let source = read_module(&proxy_src().join("runtime/route_runtime/route/source/datagram.rs"));
    assert!(source.contains("continuity::run("));
    assert!(source.contains("DirectPeerDriverSlot::channel()"));
    assert!(source.contains("wait_closed("));
}

#[test]
fn no_client_mux_root_stays_facade_only() {
    for retired in [
        "runtime/inbound_route/mux/no_client.rs",
        "runtime/inbound_route/mux/no_client/request.rs",
        "runtime/inbound_route/mux/no_client/route.rs",
    ] {
        assert!(!proxy_src().join(retired).exists());
    }
    let source = read_module(&proxy_src().join("runtime/route_runtime/route/source.rs"));
    assert!(source.contains("AcceptedRequest::Multiplex"));
    assert!(source.contains("runtime.run_requests(source)"));
    let runtime = read(&proxy_src().join("runtime.rs"));
    assert!(!runtime.contains("mod inbound_route;"));
    assert!(!proxy_src().join("runtime/inbound_route.rs").exists());
    assert!(!proxy_src().join("runtime/inbound_route").exists());
    assert_sources_exclude(
        &proxy_src().join("runtime"),
        &[
            "dispatch_no_client_mux_route",
            "NoClientMuxRouteDefaults",
            "LoggedTcpSocketListenerRequest",
            "serve_inbound_with_client_response",
        ],
    );
}

#[test]
fn retired_recorded_dispatch_chain_is_absent() {
    for relative in [
        "runtime/inbound_route/recorded.rs",
        "runtime/inbound_route/recorded",
        "runtime/inbound_operation/context/recorded.rs",
    ] {
        assert!(
            !proxy_src().join(relative).exists(),
            "retired chain: {relative}"
        );
    }
    assert_non_test_sources_exclude(
        &proxy_src().join("runtime"),
        &[
            "RecordedProtocolMuxRouteDefaults",
            "dispatch_recorded_mux_stream_route",
            "dispatch_recorded_protocol_mux_stream_request_with_defaults",
        ],
    );
}

#[test]
fn retired_peer_listener_cannot_restore_an_unowned_stream_task_loop() {
    for relative in [
        "runtime/inbound_operation/peer_route.rs",
        "runtime/inbound_operation/context/transport.rs",
    ] {
        assert!(
            !proxy_src().join(relative).exists(),
            "retired task loop: {relative}"
        );
    }
    assert_non_test_sources_exclude(
        &proxy_src().join("runtime"),
        &["PeerRouteInboundListenerOperation", "run_transport_streams"],
    );
}

#[test]
fn retired_stream_udp_bridge_cannot_bypass_standard_datagram_accounting() {
    for retired in [
        "runtime/stream_udp.rs",
        "runtime/stream_udp/handler.rs",
        "runtime/stream_udp/recording.rs",
        "runtime/stream_udp/relay.rs",
    ] {
        assert!(!proxy_src().join(retired).exists());
    }
    let standard = read_module(&proxy_src().join("runtime/route_runtime/route/source/datagram.rs"));
    for required in [
        "UdpIngressRuntime",
        ".spawn(TaskClass::Critical",
        "with_accepted_caller(driver.caller()?)",
        "logical Datagram original cleanup remains pending",
        "record_session_inbound_traffic",
        "record_session_inbound_rx",
    ] {
        assert!(
            standard.contains(required),
            "standard Datagram must retain {required}"
        );
    }
    assert!(!standard.contains("use crate::runtime::Proxy"));
    assert_non_test_sources_exclude(
        &proxy_src().join("runtime"),
        &[
            "StreamPacketSessionUdpHandler",
            "StreamUdpRelayRequest",
            "run_mapped_protocol_stream_udp_relay",
        ],
    );
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
    ] {
        assert!(
            sessions.contains(expected),
            "udp_flow sessions module tree must still provide `{expected}`"
        );
    }
}

#[test]
fn direct_udp_replies_are_owned_by_socket_policy_identity() {
    let sessions = read_module(&proxy_src().join("runtime/udp_flow/sessions.rs"));
    assert!(!sessions.contains("fn direct_response_session_id("));
    let sockets = read_module(&proxy_src().join("runtime/udp_socket.rs"));
    assert!(sockets.contains("struct DirectUdpPolicy"));
    assert!(sockets.contains("replies: ownership::DirectUdpReplies"));
    assert!(sockets.contains("flows: HashMap<SocketAddr, DirectUdpResponseFlow>"));
    assert!(sockets.contains("entry.replies.flow(&sender)"));
    assert!(!sockets.contains("response_flows: HashMap"));
    assert!(sockets.contains("direct_policy_is_current"));
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
fn retired_udp_association_contract_and_dispatcher_traits_cannot_return() {
    for retired in [
        "runtime/udp_association.rs",
        "runtime/udp_association/contract.rs",
        "runtime/udp_association/contract/handler.rs",
        "runtime/udp_association/contract/dispatch.rs",
    ] {
        assert!(
            !proxy_src().join(retired).exists(),
            "retired {retired} must remain deleted"
        );
    }
    let core = read(&workspace_root().join("crates/core/src/udp.rs"));
    for retired in [
        "trait InboundUdpAssociation",
        "trait InboundUdpAssociationDispatcher",
        "trait InboundUdpAssociationResponder",
    ] {
        assert!(
            !core.contains(retired),
            "retired {retired} must not become another protocol SPI"
        );
    }
    let standard = read_module(&proxy_src().join("runtime/route_runtime/route/source/datagram.rs"));
    for expected in [
        "AcceptedDatagram",
        "dispatch_inbound_packet",
        "record_direct_udp_response_parts",
        "record_upstream_udp_response_received",
        "record_chain_udp_response_parts",
    ] {
        assert!(
            standard.contains(expected),
            "standard executor must preserve {expected}"
        );
    }
}

#[test]
fn standard_datagram_retains_actual_driver_and_original_close_owners() {
    let owner = read(&proxy_src().join("runtime/route_runtime/route/source/datagram/owner.rs"));
    for required in [
        "finish_retaining_io",
        "responses.shutdown(deadline)",
        "receipt.matches(&responses)",
        "close_originals_until(deadline)",
        "wait_returns(&self.returns, deadline)",
    ] {
        assert!(
            owner.contains(required),
            "actual UDP ownership must retain {required}"
        );
    }
    let runtime = read(&proxy_src().join("runtime/route_runtime/route/source/datagram.rs"));
    assert!(runtime.contains("retain_logical_resource("));
    assert!(runtime.contains("retained.bind_driver(&driver)"));
    assert!(runtime.contains(".close(tokio::time::Instant::now()"));
    assert!(runtime.contains(".spawn(TaskClass::Critical"));
    assert!(!runtime.contains("tokio::spawn"));
    assert!(!proxy_src()
        .join("runtime/udp_association/lifecycle.rs")
        .exists());
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
        "pub(crate) async fn forward_existing_managed_flow(",
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
        "pub(crate) async fn forward_existing_managed_flow(",
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
    let root = read(&proxy_src().join("runtime/udp_flow/managed/mod.rs"));
    assert!(root.contains("mod connection;"));
    assert!(!root.contains("struct Connection"));
    assert!(!root.contains("async fn send"));
    assert!(!root.contains("fn supports_"));
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
fn retired_private_packet_connection_does_not_return() {
    assert!(!proxy_src()
        .join("runtime/udp_flow/managed/connection/packet.rs")
        .exists());
}

#[test]
fn udp_flow_registered_root_stays_facade_only() {
    let registered_root = read(&proxy_src().join("runtime/udp_flow/registered/mod.rs"));
    let registered = read_module(&proxy_src().join("runtime/udp_flow/registered"));
    for module_name in ["mod forward;", "mod state;", "mod upstream;"] {
        assert!(registered_root.contains(module_name));
    }
    for forbidden in [
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
        "pub(crate) struct RegisteredUdpState",
        "pub(crate) struct UpstreamAssociationSend",
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
        "pub(crate) struct UpstreamAssociationSend",
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
fn standard_upstream_datagram_sessions_do_not_restore_the_handler_spi() {
    let registry = read_module(&proxy_src().join("protocol_registry"));
    assert_non_test_sources_exclude(
        &proxy_src().join("runtime/udp_flow/registered/upstream"),
        &[
            "ManagedUdpFlowResume",
            "supports_upstream_resume",
            "execution.config()",
            ".protocol.endpoint()",
            "downcast_ref",
            "UpstreamAssociationHandler",
            "UpstreamUdpHandlers",
            "boxed_registered_upstream_handler",
        ],
    );
    assert!(!registry.contains("UpstreamUdpHandlerProvider"));
    let adapter = read_module(&proxy_src().join("adapters/socks5"));
    assert!(!adapter.contains("RegisteredAssociationUdpOperation"));
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/operation/registered.rs")
        .exists());
    let selection = read(&proxy_src().join("inventory/udp/leaf.rs"));
    assert!(selection.contains("PreparedUdpLeafCandidate::Datagram"));
    assert!(!adapter.contains("claim_udp_flow_leaf_impl"));
    assert!(!adapter.contains("ClaimedSocks5UdpLeaf"));
    assert!(!adapter.contains("Socks5ManagedUdpAssociationTarget"));
    assert!(!proxy_src()
        .join("adapters/socks5/udp/native_association.rs")
        .exists());
    assert!(!proxy_src().join("adapters/socks5/udp/handler.rs").exists());
    let native = read(&proxy_src().join("runtime/udp_flow/registered/upstream/native.rs"));
    assert!(native.contains("stream_bridge::datagram::open"));
    assert!(native.contains("self.control"));
    let close = read(
        &proxy_src()
            .join("runtime/udp_flow/registered/upstream/runtime/association/lifecycle/close.rs"),
    );
    assert!(close.contains(".wait_closed(deadline)"));
    assert!(close.contains("report.is_complete()"));
    assert!(!native.contains("::socks5"));
}

#[test]
fn standard_upstream_association_keeps_one_typed_runtime_without_handler_shells() {
    let state = read_module(&proxy_src().join("runtime/udp_flow/registered/upstream/state"));
    assert!(
        state.contains("UpstreamAssociationRuntime<DatagramAssociationTarget, NativeAssociation>")
    );
    for removed in [
        "runtime/handler.rs",
        "runtime/control.rs",
        "state/handlers.rs",
    ] {
        assert!(!proxy_src()
            .join("runtime/udp_flow/registered/upstream")
            .join(removed)
            .exists());
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
    {
        let module_name = "mod upstream;";
        assert!(start_root.contains(module_name));
    }
    for forbidden in [
        "pub(crate) async fn start_upstream_udp_flow(",
        "pub(crate) fn handles_upstream_resume(",
        "fn upstream_send(",
    ] {
        assert!(
            !start_root.contains(forbidden),
            "udp_flow registered state start facade root must not keep `{forbidden}` inline"
        );
    }
    {
        let expected = "pub(crate) async fn start_upstream_udp_flow(";
        assert!(
            start.contains(expected),
            "udp_flow registered state start module tree must still provide `{expected}`"
        );
    }
    assert!(!start.contains("fn handles_upstream_resume("));
    assert!(!start.contains("fn upstream_send("));
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
        "pub(in crate::runtime) fn register_connection(",
        "pub(crate) fn release_session(",
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
        "pub(in crate::runtime) fn register_connection(",
        "pub(crate) fn release_session(",
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
    assert!(tcp_candidate.contains("stream_bridge::connect"));
    assert!(!tcp_candidate.contains("operation.execute("));
    assert!(tcp_outbound.contains("execute_prepared_tcp_outbound("));
    assert!(tcp_outbound.contains("PreparedTcpOutbound::Fallback"));
    assert!(udp_outbound.contains("execute_prepared_udp_outbound("));
    assert!(udp_outbound.contains("PreparedUdpOutbound::Fallback"));
    assert!(udp_outbound.contains("PreparedUdpLeafCandidate::Datagram"));
    assert!(!udp_outbound.contains("operation.execute("));
    for source in [tcp_candidate, tcp_outbound, udp_outbound] {
        assert!(!source.contains("ResolvedLeafOutbound"));
        assert!(!source.contains("use crate::runtime::Proxy"));
        assert!(!source.contains("&Proxy"));
    }
}

#[test]
fn inventory_resolves_current_standard_roles_and_kernel_block_facts() {
    let runtime = read(&proxy_src().join("inventory/runtime.rs"));
    assert!(runtime.contains("fn native_tcp_candidate("));
    assert!(runtime.contains("components?.configured(&outbound.tag)"));
    for forbidden in [
        "claim_outbound_leaf",
        "ClaimedOutboundLeaf",
        "ClaimedRelayChain",
        "claim_tcp_outbound",
        "claim_udp_outbound",
    ] {
        assert!(!runtime.contains(forbidden));
    }
    let leaf = read(&proxy_src().join("inventory/tcp/leaf.rs"));
    assert!(leaf.contains("ResolvedLeafOutbound::Block"));
}

#[test]
fn registry_outbound_surface_is_cold_metadata_only() {
    let outbound = read(&proxy_src().join("protocol_registry/registry/outbound.rs"));
    assert!(outbound.contains("fn outbound_protocol_entry("));
    assert!(outbound.contains("ColdPacketPathProjection"));
    for forbidden in [
        "claim_outbound_leaf",
        "ClaimedOutboundLeaf",
        "fn tcp_connect",
        "into_udp_flow",
    ] {
        assert!(!outbound.contains(forbidden));
    }
}

#[test]
fn executable_protocol_claimers_and_test_only_projection_toggles_are_removed() {
    for relative in [
        "protocol_registry/capability.rs",
        "protocol_registry/registry/build.rs",
        "protocol_registry/registry/mod.rs",
        "protocol_registry/registry/outbound.rs",
    ] {
        let source = read(&proxy_src().join(relative));
        for retired in [
            "OutboundLeafClaim",
            "ProjectedOutboundClaimer",
            "claim_outbound_leaf",
            "without_outbound_projection",
            "OutboundLeafInput",
        ] {
            assert!(!source.contains(retired), "{relative} restores {retired}");
        }
    }
    assert!(!proxy_src()
        .join("protocol_registry/registry/outbound/plane.rs")
        .exists());
    assert!(!proxy_src()
        .join("protocol_registry/model/outbound.rs")
        .exists());
}

#[test]
fn migrated_protocols_have_no_parallel_private_tcp_udp_executors() {
    for relative in [
        "adapters/tcp.rs",
        "adapters/udp.rs",
        "runtime/transport_leaf.rs",
        "runtime/tcp_dispatch/operation/transport.rs",
        "runtime/udp_dispatch/operation/transport.rs",
        "runtime/udp_dispatch/operation/stream_packet.rs",
        "inventory/udp/relay.rs",
        "inventory/udp/datagram_relay.rs",
        "adapters/vless/claim.rs",
        "adapters/vmess/claim.rs",
        "adapters/trojan/claim.rs",
        "adapters/vless/packet_path.rs",
        "adapters/vless/udp_compat.rs",
        "adapters/mieru/tcp.rs",
        "adapters/mieru/udp.rs",
        "runtime/udp_flow/managed/stream_manager.rs",
        "runtime/udp_flow/managed/bridge.rs",
        "runtime/tcp_dispatch/relay/datagram.rs",
        "runtime/tcp_dispatch/relay/identity.rs",
    ] {
        assert!(
            !proxy_src().join(relative).exists(),
            "retired execution path: {relative}"
        );
    }
    for relative in [
        "adapters/vless.rs",
        "adapters/vmess.rs",
        "adapters/trojan.rs",
        "adapters/mieru.rs",
    ] {
        let source = read_module(&proxy_src().join(relative));
        for forbidden in [
            "ProxyTransportTcpLeaf",
            "ProxyTransportUdpLeaf",
            "claim_outbound_leaf_impl",
            "ManagedStreamConnectorFlow",
        ] {
            assert!(!source.contains(forbidden), "{relative}: {forbidden}");
        }
    }
    let registry = read(&proxy_src().join("register.rs"));
    for protocol in ["Vless", "Vmess", "Trojan", "Mieru"] {
        assert!(!registry.contains(&format!("{protocol}Adapter::claim_outbound_leaf_impl")));
    }
    let relay = read(&proxy_src().join("inventory/udp/standard_relay.rs"));
    assert!(relay.contains("DatagramAssociationTarget"));
    let request = read(&proxy_src().join("protocol_registry/capability.rs"));
    assert!(!request.contains("UdpRelayResult") && !request.contains("udp_relay:"));
}

#[test]
fn protocol_library_compatibility_remains_protocol_owned() {
    let traits = read(&workspace_root().join("crates/traits/src/udp_flow.rs"));
    assert!(traits.contains("ProtocolUdpFlowLeaf"));
    assert!(!traits.contains("ManagedStream"));
    for protocol in ["vless", "vmess", "trojan", "mieru"] {
        let source = read(
            &workspace_root().join(format!("protocols/{protocol}/src/transport/managed_udp.rs")),
        );
        for forbidden in [
            "ManagedTupleUdpResume",
            "ManagedPacketUdpResume",
            "ManagedConnectorFlowOps",
            "ManagedStreamUdpResume",
        ] {
            assert!(!source.contains(forbidden));
        }
    }
}

#[test]
fn active_source_selects_its_canonical_donor_independently_of_business_aliases() {
    let service = read(&proxy_src().join("adapters/vless/reverse/service.rs"));
    assert!(service.contains("::vless::reverse::source::Factory"));
    assert!(!service.contains("standard: bool") && !service.contains("if !standard"));
    let selection = read_module(&proxy_src().join("adapters/vless/component/mux/production.rs"));
    assert!(!selection.contains("mux_prefix::qualified"));
    let registry = read(&proxy_src().join("protocol_registry/registry/service.rs"));
    assert!(registry.contains("prepare_inbound_service(outbound)"));
    assert!(!registry.contains("ComponentPlan"));
    let carrier = read(&proxy_src().join("runtime/inbound_service/native/carrier.rs"));
    assert!(carrier.contains("service::connect_ingress"));
    assert!(carrier.contains("node.slot.instance_id() != self.donor"));
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

    let services = read(&proxy_src().join("protocol_registry/context/udp/packet_path.rs"));
    let fields = services
        .split("pub(crate) struct PacketPathExecutionServices {")
        .nth(1)
        .expect("packet paths have a dedicated execution context")
        .split('}')
        .next()
        .unwrap();
    assert!(fields.contains("tcp: TcpExecutionServices"));
    assert!(fields.contains("network: UdpNetworkServices"));
    for forbidden in [
        "ProtocolInventory",
        "TcpRuntimeServices",
        "UdpRuntimeServices",
        "Proxy",
    ] {
        assert!(
            !fields.contains(forbidden),
            "packet path execution must not retain {forbidden}"
        );
    }
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
fn packet_metadata_has_no_parallel_socket_or_executable_projection() {
    for relative in ["adapters/socks5/udp.rs", "adapters/shadowsocks/udp.rs"] {
        let metadata = read(&proxy_src().join(relative));
        assert!(metadata.contains("cold_packet_path_projection"));
        for forbidden in [
            "PreparedUdpPacketPathOperation",
            "UdpNetworkServices",
            "SocketTcpOperation",
            "build_udp_socket_carrier",
        ] {
            assert!(!metadata.contains(forbidden), "{relative}: {forbidden}");
        }
    }
    for relative in [
        "adapters/socks5/tcp.rs",
        "adapters/socks5/udp/packet_path.rs",
        "adapters/socks5/udp/upstream_association.rs",
    ] {
        assert!(!proxy_src().join(relative).exists());
    }
    let socks = read(&proxy_src().join("adapters/socks5.rs"));
    assert!(!socks.contains("claim_outbound_leaf_impl"));
    let capability = read(&proxy_src().join("protocol_registry/capability.rs"));
    assert!(!capability.contains("packet_path:"));
}

#[test]
fn plugin_hysteria_and_reverse_select_their_real_standard_factories() {
    let ss = read(&proxy_src().join("adapters/shadowsocks.rs"));
    assert!(!ss.contains("SocketTcpOperation"));
    assert!(!ss.contains("claim_tcp_outbound_leaf_impl"));
    assert!(!proxy_src().join("adapters/shadowsocks/tcp.rs").exists());
    let source = read(&proxy_src().join("adapters/hysteria2/outbound.rs"));
    assert!(source.contains("with_source_dir(resources.source_dir())"));
    let reverse = read(&proxy_src().join("adapters/vless/reverse.rs"));
    assert!(!reverse.contains("LogicalTcpConnectOperation"));
    let standard = read_module(&proxy_src().join("adapters/vless/component/mux.rs"));
    assert!(standard.contains("::vless::reverse::component::Factory"));
    assert!(!proxy_src()
        .join("runtime/tcp_dispatch/operation/logical.rs")
        .exists());
    assert!(!proxy_src()
        .join("runtime/udp_dispatch/operation/logical.rs")
        .exists());
}

#[test]
fn native_plugin_process_roles_replace_the_last_socket_compatibility_executor() {
    for retired in [
        "adapters/shadowsocks/tcp.rs",
        "adapters/shadowsocks/udp/plugin.rs",
        "runtime/udp_dispatch/operation/managed_datagram.rs",
        "runtime/udp_flow/managed/datagram_manager.rs",
    ] {
        assert!(!proxy_src().join(retired).exists());
    }
    let adapter = read(&proxy_src().join("adapters/shadowsocks/component.rs"));
    assert!(adapter.contains("with_process_endpoint"));
    assert!(adapter.contains("PluginFactory"));
    assert!(adapter.contains("cfg!(any(unix, windows))"));
}

#[test]
fn outbound_probe_owns_generic_tcp_dispatch_outside_urltest_policy() {
    let urltest = read(&proxy_src().join("groups/urltest.rs"));
    let urltest_refresh = read(&proxy_src().join("groups/urltest/refresh.rs"));
    let outbound_probe = read_module(&proxy_src().join("runtime/outbound_probe.rs"));
    let dispatch_intent = read(&proxy_src().join("runtime/tcp_dispatch/intent.rs"));
    let dispatch_candidate = read(&proxy_src().join("runtime/tcp_dispatch/candidate.rs"));
    let runtime_urltest = read(&proxy_src().join("runtime/listeners/urltest.rs"));
    assert!(urltest.contains("struct UrlTestRuntime"));
    assert!(urltest.contains("OutboundProbeRuntime"));
    assert!(urltest_refresh.contains("probe_target_shared("));
    assert!(!urltest.contains("dispatch_tcp_outbound("));
    assert!(!urltest_refresh.contains("dispatch_tcp_outbound("));
    assert!(outbound_probe.contains("dispatch_tcp_outbound("));
    assert!(outbound_probe.contains("TcpDispatchIntent::DiagnosticProbe"));
    assert!(outbound_probe.contains("TcpDispatchIntent::PolicyProbe"));
    assert!(dispatch_intent.contains("enum TcpDispatchIntent"));
    assert!(dispatch_intent.contains("DiagnosticProbe"));
    assert!(dispatch_intent.contains("PolicyProbe"));
    assert!(!dispatch_intent.contains("checks_outbound_health"));
    assert!(!dispatch_candidate.contains("check_outbound_health"));
    assert!(dispatch_candidate.contains("records_outbound_health"));
    assert!(urltest_refresh.contains("apply_urltest_probe_result("));
    assert!(!urltest_refresh.contains("urltest.select("));
    let engine_policy = read_module(&workspace_root().join("crates/engine/src/runtime/policy.rs"));
    assert!(engine_policy.contains("pub fn apply_urltest_probe_result("));
    assert!(engine_policy.contains("fn reconcile_urltest_group_health("));
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

#[path = "runtime_boundary/multiplex.rs"]
mod multiplex;

#[test]
fn reusable_transport_dialing_does_not_retain_its_own_protocol_registry() {
    let upstream = read_module(&proxy_src().join("protocol_registry/context/upstream.rs"));
    assert!(upstream.contains("struct UpstreamConnectServices"));
    assert!(upstream.contains("DirectConnector"));
    assert!(upstream.contains("DnsSystem"));
    assert!(upstream.contains("EgressInterfaceControl"));
    for forbidden in [
        "ProtocolInventory",
        "ProtocolRegistry",
        "Proxy",
        "engine: Engine",
    ] {
        assert!(
            !upstream.contains(forbidden),
            "network dialing must not retain {forbidden}"
        );
    }
}

#[test]
fn tcp_execution_state_cannot_prepare_capabilities_or_retain_registry() {
    let execution = read_module(&proxy_src().join("protocol_registry/context/tcp/execution.rs"));
    assert!(execution.contains("struct TcpExecutionServices"));
    for forbidden in [
        "ProtocolInventory",
        "ProtocolRegistry",
        "prepare_tcp_outbound",
        "claim_outbound_leaf",
        "PreparedTcpConnectOperation",
        "Arc<Proxy>",
    ] {
        assert!(
            !execution.contains(forbidden),
            "TCP execution retains {forbidden}"
        );
    }
}

#[test]
fn udp_execution_domain_rejects_erased_resume_and_handler_assembly() {
    let removed = [
        "ManagedUdpHandlerProvider",
        "ManagedUdpFlowResume",
        "ManagedUdpFlowResumeObject",
        "ManagedDatagramFlowHandler",
        "ManagedStreamPacketFlowHandler",
        "ManagedRelayFlowHandler",
        "ManagedStreamHandlerPair",
        "ManagedUdpHandlers",
        "RegisteredUdpHandlers",
        "ManagedUdpState",
        "ManagedStreamState",
        "ManagedDatagramState",
        "ManagedUdpFlowRequest",
        "ManagedUdpFlowKind",
        "ManagedDatagramExistingSend",
        "ManagedStreamExistingSend",
        "ManagedRelayExistingSend",
        "register_managed_capability",
        "managed_udp_handler_providers",
        "register_managed_flow",
        "managed_flow_resume",
        "registered_udp_handlers",
        "managed_stream_handler_box",
        "managed_datagram_handler_box",
        "managed_datagram_socket_handler_box",
        "managed_stream_udp_handler_for_resume",
    ];
    for path in rust_sources(&proxy_src()) {
        let source = read(&path);
        for token in source.split(|ch: char| !ch.is_alphanumeric() && ch != '_') {
            assert!(
                !removed.contains(&token),
                "{} restores {token}",
                path.display()
            );
        }
    }
    for directory in [
        "state",
        "model",
        "stream",
        "stream_manager/manager",
        "datagram_manager/manager",
        "cache/stream",
    ] {
        assert!(!proxy_src()
            .join("runtime/udp_flow/managed")
            .join(directory)
            .exists());
    }
    let setup = read(&proxy_src().join("runtime/udp_dispatch/lifecycle/setup.rs"));
    assert!(setup.contains("UdpFlowState::new()"));
    assert!(!setup.contains("ProtocolInventory"));
    assert!(!setup.contains("register::"));
}

#[test]
fn udp_registered_packets_execute_the_original_connection_without_recovering_protocol_state() {
    let forward = read(&proxy_src().join("runtime/udp_flow/registered/forward.rs"));
    assert!(forward.contains("RegisteredUdpConnection::Logical"));
    assert!(!forward.contains("RegisteredUdpConnection::Tuple"));
    assert!(!forward.contains("RegisteredUdpConnection::Datagram"));
    assert!(forward.contains("connection.send("));
    assert!(!forward.contains(".send_datagram("));
    for forbidden in [
        ".supports_",
        "downcast",
        "as_any",
        "resume",
        ".connector_flow(",
        ".establish_",
    ] {
        assert!(
            !forward.contains(forbidden),
            "per-packet forwarding must not recover {forbidden}"
        );
    }
    let state = read(&proxy_src().join("runtime/udp_flow/registered/state/model.rs"));
    assert!(!state.contains("Legacy("));
    assert!(state.contains("Logical(super::super::LogicalConnection)"));
    assert!(!state.contains("SharedManagedUdpConnection"));
    assert!(!state.contains("SharedManagedDatagramUdpConnection"));
}
#[test]
fn udp_native_datagrams_retain_the_actual_standard_connection_without_a_second_cache() {
    assert!(!proxy_src()
        .join("runtime/udp_flow/managed/cache.rs")
        .exists());
    assert!(!proxy_src()
        .join("runtime/udp_flow/managed/cache/datagram.rs")
        .exists());
    let logical = read(&proxy_src().join("runtime/udp_flow/registered/logical.rs"));
    assert!(logical.contains("Datagram(Arc<datagram::DatagramConnection>)"));
    let actual = read(&proxy_src().join("runtime/udp_flow/registered/logical/datagram.rs"));
    assert!(actual.contains("target.current_slot(execution)"));
    assert!(actual.contains("reservation.adopt(channel.control)"));
    assert!(actual.contains("control: Mutex<"));
    for forbidden in [
        "dyn Any",
        "downcast",
        "Box<dyn Fn",
        "SharedManagedDatagramUdpConnection",
    ] {
        assert!(!actual.contains(forbidden));
    }
}
#[test]
fn udp_executable_registration_releases_business_indices_without_rearming_all_cached_flows() {
    let lifetime =
        read(&proxy_src().join("runtime/udp_flow/registered/state/lifecycle/managed.rs"));
    assert!(lifetime.contains("self.release_session(session_id)"));
    assert!(lifetime.contains("self.connections.remove(&id)"));
    let logical = read(&proxy_src().join("runtime/udp_flow/registered/logical.rs"));
    assert!(logical.contains("in &self.logical_sessions"));
    assert!(!logical.contains("in &self.sessions"));
    for file in [
        "runtime/udp_dispatch/failure.rs",
        "runtime/udp_dispatch/lifecycle/poll.rs",
    ] {
        assert!(read(&proxy_src().join(file)).contains(".release_session("));
    }
}

#[test]
fn packet_device_domain_has_no_claim_or_dynamic_lifecycle_registry() {
    for path in rust_sources(&proxy_src()) {
        let source = read(&path);
        for forbidden in [
            "ClaimedPacketLeaf",
            "OutboundDeviceLifecycleCapability",
            "PreparedOutboundDeviceState",
            "OutboundDeviceCompletion",
            "register_outbound_device_lifecycle",
            ".packet_flow_adapter(",
            "prepare_outbound_devices",
            "shutdown_outbound_devices",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} restores {forbidden}",
                path.display()
            );
        }
    }
    let registry = read_module(&proxy_src().join("protocol_registry/registry/mod.rs"));
    assert!(!registry.contains("outbound_devices:"));
    assert!(!proxy_src()
        .join("protocol_registry/context/device.rs")
        .exists());
    let assembly = read(&proxy_src().join("adapters/devices.rs"));
    assert!(assembly.contains("Arc<super::direct::DirectAdapter>"));
    assert!(assembly.contains("Arc<super::wireguard::WireguardAdapter>"));
    assert!(!assembly.contains("pub(crate) trait"));
    assert!(!assembly.contains("dyn Fn"));
}

#[test]
fn packet_device_replacement_keeps_original_cleanup_outcomes_and_never_uses_grace_as_evidence() {
    let raw = read_module(&proxy_src().join("runtime/raw_ip/pool.rs"));
    assert!(raw.contains("VecDeque<Arc<SharedRawIpDevice>>"));
    assert!(raw.contains("original.wait_closed().await?"));
    assert!(!raw.contains("RETIRE_GRACE"));
    assert!(!raw.contains("compatibility_retiring:"));
    assert!(!raw.contains("entry.compatibility"));
    assert!(
        raw.find("original.wait_closed().await?").unwrap()
            < raw.find("state.retiring.pop_front()").unwrap()
    );
    let raw_device = read_module(&proxy_src().join("runtime/raw_ip/device.rs"));
    assert!(!raw_device.contains("fn wait_stopped"));
    let reconcile = read(&proxy_src().join("runtime/orchestration/reconcile.rs"));
    let cancelled = reconcile
        .split(".stage_device_publication(")
        .next()
        .unwrap()
        .rsplit("if let Err(error) = preparing.ensure_open() {")
        .next()
        .unwrap();
    assert!(
        cancelled.find("drop(prepared_resources.take())").unwrap()
            < cancelled.find("self.reject_reload").unwrap()
    );
    let recovery = read(&proxy_src().join("runtime/orchestration/reconcile/recovery.rs"));
    assert!(recovery.find("restore_dns(").unwrap() < recovery.find(".prepare_devices(").unwrap());
    let direct = read(&proxy_src().join("runtime/raw_ip/carrier/direct.rs"));
    assert!(direct.contains("socket.close()"));
    let prepare = read(&proxy_src().join("adapters/wireguard/lifecycle/prepare.rs"));
    assert!(
        prepare
            .find("self.pool.retire_changed(&desired).await?")
            .unwrap()
            < prepare.find("self.pool.begin_stage()").unwrap()
    );
}

#[test]
fn inbound_listener_domain_has_no_retired_execution_spi_or_legacy_fallback() {
    let root = proxy_src();
    for path in rust_sources(&root) {
        let source = read(&path);
        for retired in [
            "InboundListenerCapability",
            "PreparedInboundListenerOperation",
            "InboundDefinition",
            "BoundInbound",
            "prepare_inbound_listener(",
            "prepare_rollback_inbound_listener(",
        ] {
            assert!(
                !source.contains(retired),
                "{} restored retired listener execution {retired}",
                path.display()
            );
        }
    }
    for removed in [
        "protocol_registry/registry/inbound.rs",
        "protocol_registry/model/inbound.rs",
        "runtime/inbound_operation/contract.rs",
    ] {
        assert!(!root.join(removed).exists());
    }
    let source = read_module(&root.join("runtime/inbound_operation/raw_ip/source.rs"));
    for actual in [
        "from_packet_listener",
        "AcceptedRequest::Packet",
        "spawn_tracked",
        "witness.wait()",
    ] {
        assert!(
            source.contains(actual),
            "Packet source must retain {actual}"
        );
    }
    let native = read(&root.join("runtime/inbound_owners/bind/native/udp/packet_reads.rs"));
    assert!(native
        .contains("reserve_launch(zero_platform_tokio::contract::scope::TaskClass::Critical)"));
    assert!(source.contains("take_packet_source_wire_launch(0)"));
    let execution = read_module(&root.join("runtime/inbound_operation/raw_ip/run.rs"));
    assert!(execution.contains("NoBusinessIdentity"));
    assert!(execution.contains("packet::send"));
    assert!(execution.contains("packet::recv"));
}

#[test]
fn hysteria2_outbound_uses_one_actual_standard_pool_for_all_existing_callers() {
    let adapter = read(&proxy_src().join("adapters/hysteria2.rs"));
    let projection = read(&proxy_src().join("adapters/hysteria2/outbound.rs"));
    let registry = read(&proxy_src().join("register.rs"));
    assert!(adapter.contains("Arc<::hysteria2::transport::owned::Pool>"));
    for retired in [
        "claim_outbound_leaf_impl",
        "SessionTcpConnectOperation",
        "Hysteria2ConnectionPool",
    ] {
        assert!(!adapter.contains(retired), "HY2 must not restore {retired}");
    }
    for retired in [
        "adapters/hysteria2/tcp.rs",
        "adapters/hysteria2/udp.rs",
        "runtime/tcp_dispatch/operation/session.rs",
    ] {
        assert!(
            !proxy_src().join(retired).exists(),
            "retired HY2 execution bridge: {retired}"
        );
    }
    let compact_projection: String = projection.chars().filter(|c| !c.is_whitespace()).collect();
    assert!(compact_projection.contains("self.outbound_pool.options("));
    assert!(compact_projection.contains(r#"resources.share("hysteria2.authenticated_pool"#));
    assert!(projection.contains("PoolFactory.build_with_upstreams"));
    assert!(registry.contains("::hysteria2::transport::owned::RoleFactory"));
    assert!(!registry.contains("Hysteria2Adapter::claim_outbound_leaf_impl"));
    let packet = read(&proxy_src().join("component/stream_bridge/datagram/packet_path.rs"));
    for required in [
        "struct StandardPacketPath",
        "StreamRoleRef",
        "self.selected.matches(&current)",
        "slot.datagram(end)",
    ] {
        assert!(
            packet.contains(required),
            "standard packet-path must retain {required}"
        );
    }
    for forbidden in [
        "hysteria2::",
        "Hysteria2ConnectionPool",
        "ClaimedUdpPacketPathLeaf",
        "Box<dyn Any>",
    ] {
        assert!(
            !packet.contains(forbidden),
            "generic packet execution must not contain {forbidden}"
        );
    }
}

#[test]
fn embedded_engine_entries_leave_source_transaction_order_with_the_original_owner() {
    let engine = read(&workspace_root().join("crates/engine/src/runtime/configuration.rs"));
    assert!(engine.contains(".commit_embedded("));
    for removed in [
        ".persist_ready(",
        ".rollback_pending(",
        ".finalize_committed(",
        "SourceDisposition",
    ] {
        assert!(
            !engine.contains(removed),
            "embedded Engine must not restore source ordering: {removed}"
        );
    }
    let source =
        read(&workspace_root().join("crates/runtime/src/configuration/source/embedded.rs"));
    for required in [
        "startup::with_source(self.clone()",
        "self.state.lock()",
        "self.persist_ready_locked(",
        "self.rollback_pending_locked(",
        "self.finalize_committed_locked(",
    ] {
        assert!(
            source.contains(required),
            "embedded source retains actual original operation: {required}"
        );
    }
}

#[test]
fn proxy_dns_executes_only_its_actual_native_provider_and_never_installs_a_library_connector() {
    let runtime = read(&proxy_src().join("runtime.rs"));
    assert!(!runtime.contains("ProxyDnsOutboundConnector"));
    assert!(!runtime.contains("set_outbound_connector"));
    assert!(runtime.contains("build_with_native_queries"));
    assert!(runtime.contains("with_native_query_queries"));
    assert!(runtime.contains("with_native_node_queries"));
    let old = read(&proxy_src().join("runtime/dns_outbound.rs"));
    assert!(!old.contains("impl DnsOutboundConnector"));
    assert!(!old.contains("Session::new"));
    assert!(!old.contains("dispatch_tcp_outbound"));
    for retired in [
        "runtime/dns_outbound/service.rs",
        "runtime/dns_outbound/service/error.rs",
        "runtime/raw_ip/datagram.rs",
    ] {
        assert!(!proxy_src().join(retired).exists(), "retired {retired}");
    }
    let registry = read(&proxy_src().join("protocol_registry/registry/outbound.rs"));
    assert!(!registry.contains("prepare_datagram_exchange"));
    let packet = read(&proxy_src().join("runtime/packet_route.rs"));
    assert!(!packet.contains("PreparedDatagramExchangeOperation"));
    let native = read(&proxy_src().join("component/native_dns/provider/detour.rs"));
    assert!(native.contains("prepare_service_datagram"));
    assert!(native.contains("caller: TaskJoinWitness"));
    let datagram = read(&proxy_src().join("component/native_dns/provider/detour/datagram.rs"));
    assert!(datagram.contains("connect_carrier_datagram"));
    assert!(datagram.contains(".adopt_datagram_loan("));
    assert!(datagram.contains("close_attempt_originals"));
}

#[test]
fn direct_tcp_has_no_claim_raw_socket_fallback_or_factory_compatibility_switch() {
    let adapter = read(&proxy_src().join("adapters/direct.rs"));
    assert!(!adapter.contains("claim_outbound_leaf_impl"));
    for old in [
        "runtime/tcp_dispatch/operation/direct.rs",
        "runtime/tcp_dispatch/service/root.rs",
        "protocol_registry/context/upstream/root.rs",
    ] {
        assert!(
            !proxy_src().join(old).exists(),
            "retired Direct/Service path: {old}"
        );
    }
    let factory = read(&proxy_src().join("adapters/direct/factory.rs"));
    assert!(!factory.contains("retain_legacy_calls"));
    assert!(factory.contains("match self.control.as_ref()"));
    let udp = read(&proxy_src().join("adapters/direct/datagram.rs"));
    assert!(udp.contains(".into_session()"));
    assert!(udp.contains("open_controlled_datagram_loan"));
    assert!(!proxy_src()
        .join("component/stream_bridge/relay/selected_prefix.rs")
        .exists());
    let selected = read(&proxy_src().join("component/stream_bridge/relay/background.rs"));
    assert!(selected.contains("select_observed_with_identity"));
    assert!(!selected.contains("Session::new"));
    let service = read(&proxy_src().join("runtime/tcp_dispatch/service.rs"));
    assert!(service.contains("service/ingress_root.rs"));
    let ingress = read(&proxy_src().join("runtime/tcp_dispatch/service/ingress_root.rs"));
    assert!(ingress.contains("direct_bridge::service_transport"));
    assert!(ingress.contains("open_controlled_stream_loan"));
    assert!(!ingress.contains(".open_root_stream("));
    let original =
        read(&proxy_src().join("runtime/tcp_dispatch/service/ingress_root/controlled.rs"));
    assert!(original.contains("plan.validate_carrier_adoption"));
    assert!(original.contains("prepare_stream(*next, grant)"));
    assert!(!proxy_src().join("runtime/tcp_dispatch/relay.rs").exists());
    let dispatch = read(&proxy_src().join("runtime/tcp_dispatch/outbound.rs"));
    assert!(dispatch.contains("stream_bridge::connect_relay"));
}

#[test]
fn http_message_dispatch_retains_its_original_admission_and_upstream_fixture_is_not_production() {
    let message = read(&proxy_src().join("runtime/tcp_ingress/runtime/standard/message.rs"));
    assert!(message.contains("let admission = self"));
    assert!(message.contains("AdmittedBusinessDemand::new"));
    assert!(message.contains("admitted: Some(demand)"));
    assert!(!message.contains("admitted: None"));
    let source = read(&proxy_src().join("runtime/tcp_ingress/runtime/serve.rs"));
    assert!(source.contains("#[cfg(all(test, feature = \"managed-stream-runtime\"))]"));
    assert!(source.contains("pub(crate) async fn open_tcp_upstream"));
}

#[test]
fn prefix_callbacks_transfer_original_standard_loans_and_source_fanout_has_no_local_executor() {
    let prefix = read(&proxy_src().join("component/stream_bridge/relay/prefix.rs"));
    assert!(!prefix.contains("mod compatibility"));
    let call = read(&proxy_src().join("component/stream_bridge/relay/prefix/call.rs"));
    for actual in [
        "self.borrow_selected_in_context",
        "self.borrow_datagram_request",
        "loan.into_session()",
        "opening_stop.disarm()",
    ] {
        assert!(
            call.contains(actual),
            "actual callback must retain {actual}"
        );
    }
    assert!(!call.contains("self.role.connect("));
    let fanout = read(&proxy_src().join("runtime/route_runtime/route/source/tasks.rs"));
    assert!(fanout.contains("registrar: AcceptedTasks"));
    assert!(fanout.contains("with_accepted_tasks(tasks)"));
    assert!(!fanout.contains("JoinSet") && !fanout.contains("Self::Local"));
    for old in [
        "component/stream_bridge/relay/prefix/compatibility.rs",
        "runtime/udp_flow/packet_path/tuple_flow.rs",
        "runtime/udp_flow/managed/datagram_manager/connection.rs",
        "runtime/udp_flow/managed/datagram_manager/connector/flow.rs",
    ] {
        assert!(
            !proxy_src().join(old).exists(),
            "retired private executor {old}"
        );
    }
    let start = read_module(&proxy_src().join("runtime/udp_dispatch/managed/start.rs"));
    assert!(!start.contains("start_transport_managed_datagram"));
    assert!(!start.contains("start_transport_managed_socket"));
}
