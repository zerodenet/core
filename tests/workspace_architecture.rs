//! Workspace-level dependency direction tests.
//!
//! These assertions lock ownership boundaries rather than file layouts.

use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
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
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn adapter_feature_names() -> Vec<String> {
    let adapters = read(&workspace_root().join("crates/proxy/src/adapters/mod.rs"));
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

fn non_http_protocol_feature_names() -> Vec<String> {
    adapter_feature_names()
        .into_iter()
        .filter(|feature| feature != "http")
        .collect()
}

fn upper_camel_identifier(value: &str) -> String {
    value
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join("")
}

#[test]
fn transport_does_not_project_engine_outbound_leaves() {
    let transport = workspace_root().join("crates/transport/src");
    for path in rust_sources(&transport) {
        let source = read(&path);
        for forbidden in [
            "ResolvedLeafOutbound",
            "ProtocolTransportLeafResolver",
            "prepare_transport_bridge_leaf",
            "resolve_transport_leaf",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} must remain independent of engine leaf projection `{forbidden}`",
                path.display()
            );
        }
    }
}

#[test]
fn dns_owned_udp_and_dot_sockets_follow_the_shared_egress_authority() {
    let dns = workspace_root().join("crates/dns/src");
    let dns_root = read(&dns.join("lib.rs"));
    let udp = read(&dns.join("udp.rs"));
    let backends = read(&dns.join("backends.rs"));
    let proxy_runtime = read(&workspace_root().join("crates/proxy/src/runtime.rs"));

    assert!(dns_root.contains("build_with_egress"));
    assert!(proxy_runtime.contains("DnsSystem::build_with_egress"));
    assert!(udp.contains("TokioDatagramSocket::bind_for_peer_on"));
    assert!(udp.contains("current_for_peer"));
    assert!(!udp.contains("tokio::net::UdpSocket"));
    assert!(backends.contains("TokioSocket::connect_addr_on"));
    assert!(backends.contains("current_for_peer"));
    assert!(!backends.contains("TcpStream::connect"));

    let direct = [
        read(&workspace_root().join("crates/proxy/src/transport/direct.rs")),
        read(&workspace_root().join("crates/proxy/src/transport/direct_dial.rs")),
    ]
    .join("\n");
    let datagram = read(&workspace_root().join("crates/transport/src/outbound_datagram.rs"));
    assert!(direct.contains("select_for_peer"));
    assert!(direct.contains("ensure_connectable"));
    assert!(datagram.contains("current_for_peer"));
}

#[test]
fn udp_egress_generation_stays_in_platform_and_proxy_orchestration() {
    let platform = read(&workspace_root().join("crates/platform/tokio/src/egress.rs"));
    let transport = read(&workspace_root().join("crates/transport/src/outbound_datagram.rs"));
    let proxy = read(&workspace_root().join("crates/proxy/src/runtime/udp_socket.rs"));
    let stack = read(&workspace_root().join("crates/stack/src/udp.rs"));

    assert!(platform.contains("pub fn generation"));
    assert!(transport.contains("egress_generation"));
    assert!(proxy.contains("refresh_if_stale"));
    assert!(!stack.contains("egress_generation"));
}

#[test]
fn peer_identity_and_process_attribution_stay_in_their_owning_layers() {
    let traits = read(&workspace_root().join("crates/traits/src/lib.rs"));
    assert!(traits.contains("Option<SocketAddress>"));
    assert!(!traits.contains("Option<IpAddress>) -> Result<(Self::Stream"));

    let platform = workspace_root().join("crates/platform/tokio/src");
    let platform_root = read(&platform.join("lib.rs"));
    let process = read(&platform.join("process.rs"));
    assert!(platform_root.contains("mod process;"));
    assert!(platform_root.contains("lookup_local_tcp_process"));
    assert!(platform_root.contains("lookup_local_udp_process"));
    assert!(process.contains("spawn_blocking"));
    for platform_contract in [
        "GetExtendedTcpTable",
        "GetExtendedUdpTable",
        "get_sockets_info",
        "/proc/net/{table}",
    ] {
        assert!(
            process.contains(platform_contract),
            "platform process lookup must retain `{platform_contract}` in zero-platform-tokio"
        );
    }

    let proxy = workspace_root().join("crates/proxy/src");
    assert!(!proxy.join("process_lookup.rs").exists());
    for relative in [
        "runtime/tcp_ingress/runtime/session.rs",
        "runtime/udp_ingress/session.rs",
    ] {
        let source = read(&proxy.join(relative));
        assert!(source.contains("zero_platform_tokio::lookup_local_"));
        for forbidden in ["GetExtendedTcpTable", "get_sockets_info", "/proc/net/"] {
            assert!(
                !source.contains(forbidden),
                "{relative} must not own platform process lookup detail `{forbidden}`"
            );
        }
    }
}

fn dependency_line_is_optional(manifest: &str, dependency: &str) -> bool {
    manifest.lines().any(|line| {
        line.contains(dependency)
            && line.contains('=')
            && line.contains('{')
            && line.contains("optional = true")
    })
}

fn manifest_line<'a>(manifest: &'a str, prefix: &str) -> &'a str {
    manifest
        .lines()
        .find(|line| line.trim_start().starts_with(prefix))
        .unwrap_or_else(|| panic!("missing manifest line `{prefix}`"))
}

#[test]
fn protocol_crates_keep_runtime_support_deps_optional_and_out_of_config_space() {
    let protocols = workspace_root().join("protocols");
    for entry in fs::read_dir(protocols).expect("read protocols") {
        let manifest = entry.expect("protocol crate").path().join("Cargo.toml");
        if !manifest.exists() {
            continue;
        }
        let source = read(&manifest);
        for forbidden in [
            "zero-api",
            "zero-config",
            "zero-dns",
            "zero-engine",
            "zero-proxy",
            "zero-router",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} must not depend on non-protocol boundary crate `{forbidden}`",
                manifest.display()
            );
        }
        for runtime_support in ["zero-platform-tokio", "zero-transport"] {
            if source.contains(runtime_support) {
                assert!(
                    source.contains("runtime = ["),
                    "{} must isolate `{runtime_support}` behind a runtime feature",
                    manifest.display()
                );
                assert!(
                    dependency_line_is_optional(&source, runtime_support),
                    "{} must keep `{runtime_support}` optional",
                    manifest.display()
                );
            }
        }
    }
}

#[test]
fn foundational_crates_do_not_depend_on_runtime_or_integration_crates() {
    let boundaries = [
        (
            "crates/traits/Cargo.toml",
            ["zero-core", "zero-engine", "zero-proxy", "zero-transport"],
        ),
        (
            "crates/core/Cargo.toml",
            ["zero-config", "zero-engine", "zero-proxy", "zero-transport"],
        ),
        (
            "crates/router/Cargo.toml",
            ["zero-config", "zero-engine", "zero-proxy", "zero-transport"],
        ),
        (
            "crates/stack/Cargo.toml",
            ["zero-config", "zero-engine", "zero-proxy", "zero-transport"],
        ),
    ];
    for (relative, forbidden) in boundaries {
        let manifest = workspace_root().join(relative);
        let source = read(&manifest);
        for dependency in forbidden {
            assert!(
                !source.contains(dependency),
                "{} must not depend on `{dependency}`",
                manifest.display()
            );
        }
    }
}

#[test]
fn engine_leaf_projection_stops_at_the_inventory_registry_claim_boundary() {
    let proxy = workspace_root().join("crates/proxy/src");
    for path in rust_sources(&proxy) {
        let source = read(&path);
        if !source.contains("ResolvedLeafOutbound") {
            continue;
        }
        let relative = path.strip_prefix(&proxy).expect("proxy-relative path");
        let allowed = relative == Path::new("inventory/runtime.rs")
            || relative == Path::new("protocol_registry/registry/outbound.rs")
            || relative.starts_with("protocol_registry/registry/tests");
        assert!(
            allowed,
            "{} must not carry engine leaves past the inventory-to-registry claim boundary",
            path.display()
        );
    }

    for path in rust_sources(&proxy.join("adapters")) {
        assert!(
            !read(&path).contains("ResolvedLeafOutbound"),
            "{} must consume registry claim inputs instead of projecting engine leaves",
            path.display()
        );
    }
}

#[test]
fn engine_outbound_leaves_are_protocol_neutral() {
    let engine = workspace_root().join("crates/engine/src");
    let protocols = non_http_protocol_feature_names();
    for path in rust_sources(&engine) {
        let source = read(&path).to_ascii_lowercase();
        for protocol in &protocols {
            assert!(
                !source.contains(&format!("resolvedleafoutbound::{protocol}")),
                "{} must not expose a concrete protocol leaf `{protocol}`",
                path.display()
            );
            assert!(
                !source.contains(&format!("outboundtarget::{protocol}")),
                "{} must not expose a concrete protocol target `{protocol}`",
                path.display()
            );
        }
    }
}

#[test]
fn proxy_adapters_materialize_protocol_config_once() {
    let adapters = workspace_root().join("crates/proxy/src/adapters");
    for entry in fs::read_dir(&adapters).expect("adapter directory") {
        let root = entry.expect("adapter entry").path();
        if root.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let source = read(&root);
        if !source.contains("fn claim_outbound_leaf_impl")
            || source.contains("OutboundLeafInput::Direct")
        {
            continue;
        }

        let protocol = root
            .file_stem()
            .and_then(|stem| stem.to_str())
            .expect("adapter file stem");
        let config_variant = upper_camel_identifier(protocol);
        assert!(
            source.contains("fn claim_outbound_leaf_impl"),
            "{} must own the protocol materialization boundary",
            root.display()
        );
        assert!(
            source.contains(&format!("OutboundProtocolConfig::{config_variant}")),
            "{} must materialize its typed protocol config",
            root.display()
        );
        assert!(
            !source.contains(&format!("ResolvedLeafOutbound::{config_variant}")),
            "{} must consume the neutral engine leaf",
            root.display()
        );

        let adapter_dir = adapters.join(protocol);
        if adapter_dir.exists() {
            for path in rust_sources(&adapter_dir) {
                assert!(
                    !read(&path).contains("ResolvedLeafOutbound"),
                    "{} must consume the protocol-owned projected leaf",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn generic_proxy_modules_do_not_repeat_the_complete_protocol_feature_set() {
    let proxy = workspace_root().join("crates/proxy/src");
    let protocols = adapter_feature_names();
    for relative in ["inventory", "runtime", "protocol_registry"] {
        for path in rust_sources(&proxy.join(relative)) {
            if path
                .components()
                .any(|component| component.as_os_str() == "tests")
            {
                continue;
            }
            let source = read(&path);
            for attribute in source.split("#[cfg(").skip(1) {
                let attribute = attribute.split(")]").next().unwrap_or(attribute);
                let enumerated = protocols
                    .iter()
                    .filter(|protocol| attribute.contains(&format!("feature = \"{protocol}\"")))
                    .count();
                assert_ne!(
                    enumerated,
                    protocols.len(),
                    "{} must use a capability/runtime feature instead of repeating the complete protocol list in one cfg",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn protocol_registry_does_not_project_proxy_leaf_runtime_facts() {
    let registry =
        read(&workspace_root().join("crates/proxy/src/protocol_registry/registry/outbound.rs"));
    for forbidden in ["fn proxy_leaf_runtime", ".proxy_endpoint()", "leaf.tag()"] {
        assert!(
            !registry.contains(forbidden),
            "protocol registry must not derive adapter-owned runtime facts through `{forbidden}`"
        );
    }
    assert_eq!(
        registry.matches(".endpoint()").count(),
        1,
        "registry must resolve the configured proxy endpoint exactly once before claim"
    );
    assert!(registry.contains("OutboundLeafInput::Proxy { outbound, endpoint }"));
}

#[test]
fn config_delegates_protocol_private_value_validation() {
    let validator = read(&workspace_root().join("crates/config/src/validate/protocol.rs"));
    for delegated in [
        "vless::parse_uuid",
        "vless::validation::validate_reality_key",
        "vless::validation::validate_reality_short_id",
        "vless::validation::validate_reality_cipher_suites",
        "vless::validation::validate_xhttp_mode",
    ] {
        assert!(
            validator.contains(delegated),
            "config must delegate protocol-private validation through `{delegated}`"
        );
    }
    assert!(
        validator.contains("vmess::parse_uuid"),
        "config must delegate VMess UUID validation through the protocol-owned validator"
    );
    assert!(
        validator.contains("vmess::VmessCipher::from_name"),
        "config must delegate VMess cipher validation through the protocol-owned validator"
    );
    assert!(
        validator.contains("socks5::validate_credential_part"),
        "config must delegate SOCKS5 credential validation through the protocol-owned validator"
    );
    assert!(
        validator.contains("shadowsocks::validation::validate_cipher"),
        "config must delegate Shadowsocks cipher validation through the protocol-owned validator"
    );
    assert!(
        validator.contains("shadowsocks::validation::validate_password"),
        "config must delegate Shadowsocks password validation through the protocol-owned validator"
    );
    for duplicate in [
        "fn validate_uuid_literal",
        "fn shadowsocks_2022_key_len",
        "fn decode_shadowsocks_2022_key_len",
    ] {
        assert!(
            !validator.contains(duplicate),
            "config must not recreate protocol validator `{duplicate}`"
        );
    }
}

#[test]
fn config_protocol_dependencies_use_validation_features() {
    let manifest = read(&workspace_root().join("crates/config/Cargo.toml"));
    for dependency in ["socks5", "shadowsocks", "vmess", "vless"] {
        let line = manifest_line(&manifest, dependency);
        assert!(
            line.contains("default-features = false"),
            "config dependency `{dependency}` must disable protocol default features"
        );
        assert!(
            line.contains("validation"),
            "config dependency `{dependency}` must depend on the protocol validation surface"
        );
    }

    let shadowsocks = manifest_line(&manifest, "shadowsocks");
    assert!(
        shadowsocks.contains("blake3"),
        "config Shadowsocks dependency must keep the `blake3` validation surface enabled"
    );
}

#[test]
fn engine_runtime_domains_do_not_regrow_in_the_facade() {
    let runtime = read(&workspace_root().join("crates/engine/src/runtime.rs"));
    for implementation in [
        "pub fn reload_config",
        "pub fn subscribe_reload",
        "pub fn set_selector_target",
        "pub fn update_urltest_state",
        "pub fn events_snapshot",
        "pub fn update_sink_status",
        "pub fn dns_lookup",
        "pub fn probe_target",
        "pub fn prepare_session",
        "pub fn finish_session",
    ] {
        assert!(
            !runtime.contains(implementation),
            "engine runtime facade must not re-own `{implementation}`"
        );
    }
    for domain in [
        "configuration",
        "diagnostics",
        "observability",
        "policy",
        "session",
    ] {
        assert!(
            runtime.contains(&format!("mod {domain};")),
            "engine runtime must delegate the `{domain}` domain"
        );
    }
}

#[test]
fn generic_transport_carriers_do_not_depend_on_protocol_crates() {
    let transport = workspace_root().join("crates/transport/src");
    let protocols = non_http_protocol_feature_names();
    for path in rust_sources(&transport) {
        let source = read(&path);
        for protocol in &protocols {
            assert!(
                !source.contains(&format!("use {protocol}::")),
                "generic carrier {} must not import protocol crate `{protocol}`",
                path.display()
            );
        }
    }
}

#[test]
fn engine_and_transport_sources_do_not_name_concrete_proxy_protocols() {
    let protocols = non_http_protocol_feature_names();
    for root in [
        workspace_root().join("crates/engine/src"),
        workspace_root().join("crates/transport/src"),
    ] {
        for path in rust_sources(&root) {
            let source = read(&path).to_ascii_lowercase();
            for protocol in &protocols {
                assert!(
                    !source.contains(protocol.as_str()),
                    "{} must not know concrete protocol `{protocol}`",
                    path.display()
                );
            }
        }
    }
}

#[test]
fn transport_does_not_hardcode_protocol_service_or_alpn_defaults() {
    let transport = workspace_root().join("crates/transport/src");
    for path in rust_sources(&transport) {
        let source = read(&path);
        for forbidden in [
            "/v2ray.core.proxy.vless.encap.GrpcService/Tun",
            "b\"hysteria2\".to_vec()",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} must not own protocol default `{forbidden}`",
                path.display()
            );
        }
    }
}

#[test]
fn outbound_carrier_stack_does_not_require_tls_for_other_carrier_features() {
    let stack = read(&workspace_root().join("crates/transport/src/outbound_stack.rs"));
    assert!(stack.contains("#[cfg(feature = \"tls\")]"));
    assert!(stack.contains("use crate::tls;"));
    assert!(stack.matches("#[cfg(not(feature = \"tls\"))]").count() >= 2);
}

#[test]
fn generic_config_models_do_not_hardcode_protocol_transport_defaults() {
    let transport_model = read(&workspace_root().join("crates/config/src/model/transport.rs"));
    for forbidden in [
        "/v2ray.core.proxy.vless.encap.GrpcService/Tun",
        "default_grpc_service_names",
    ] {
        assert!(
            !transport_model.contains(forbidden),
            "generic config transport model must not own protocol default `{forbidden}`"
        );
    }
}

#[test]
fn transport_does_not_depend_on_config_protocol_adts() {
    let transport = workspace_root().join("crates/transport/src");
    for path in rust_sources(&transport) {
        let source = read(&path);
        if !source.contains("zero_config::") && !source.contains("use zero_config") {
            continue;
        }
        panic!(
            "{} must not import zero-config from transport",
            path.display()
        );
    }

    let manifest = read(&workspace_root().join("crates/transport/Cargo.toml"));
    assert!(
        !manifest.contains("zero-config"),
        "crates/transport/Cargo.toml must not depend on zero-config"
    );
    for protocol in non_http_protocol_feature_names() {
        assert!(
            !manifest.contains(&format!("name = \"{protocol}\""))
                && !manifest.contains(&format!("path = \"../../protocols/{protocol}\"")),
            "crates/transport/Cargo.toml must not depend on protocol crate `{protocol}`"
        );
    }
}

#[test]
fn transport_and_engine_manifests_lock_the_orchestration_dependency_direction() {
    let transport = read(&workspace_root().join("crates/transport/Cargo.toml"));
    for forbidden in [
        "zero-config",
        "zero-engine",
        "zero-proxy",
        "../../protocols/",
    ] {
        assert!(
            !transport.contains(forbidden),
            "zero-transport must not depend on orchestration boundary `{forbidden}`"
        );
    }

    let engine = read(&workspace_root().join("crates/engine/Cargo.toml"));
    for forbidden in ["zero-proxy", "zero-transport", "../../protocols/"] {
        assert!(
            !engine.contains(forbidden),
            "zero-engine must not depend on execution boundary `{forbidden}`"
        );
    }
}

#[test]
fn stack_keeps_packet_parsing_separate_from_connection_lifecycle() {
    let stack = workspace_root().join("crates/stack/src");
    let packet = read(&stack.join("packet.rs"));
    for forbidden in ["tokio::", "TcpState", "UserTcpStack", "mpsc::"] {
        assert!(
            !packet.contains(forbidden),
            "packet parser must not own `{forbidden}` lifecycle state"
        );
    }
    let tcp = read(&stack.join("tcp.rs"));
    assert!(
        tcp.contains("crate::packet"),
        "TCP lifecycle must consume the packet parser boundary"
    );
    assert!(
        !tcp.contains("fn parse_ip("),
        "TCP lifecycle must not recreate IP parsing"
    );
}

#[test]
fn root_process_entrypoint_delegates_command_execution() {
    let main = read(&workspace_root().join("src/main.rs"));
    assert!(main.contains("application::execute"));
    for request in [
        "method: \"mode.set\"",
        "method: \"tun.start\"",
        "method: \"tun.stop\"",
        "IpcRequest",
        "ProxyHandle",
        "EngineHandle",
        "spawn_ipc_server",
        "spawn_http_server",
        "spawn_event_dispatcher",
        "wait_for_shutdown_signal",
        "status_server_spec",
    ] {
        assert!(
            !main.contains(request),
            "process entrypoint must not own application runtime responsibility `{request}`"
        );
    }
    for implementation in [
        "async fn main",
        "async fn try_main",
        "fn init_tracing_from_config",
    ] {
        assert!(
            main.contains(implementation),
            "process entrypoint must retain `{implementation}`"
        );
    }
}
