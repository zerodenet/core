use zero_api::{CapabilitiesQuery, QueryRequest, QueryResponse, QueryService};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

#[test]
fn direct_packet_binding_rejects_foreign_platform_backend_before_device_execution() {
    let backend = if cfg!(target_os = "windows") {
        serde_json::json!({"fd":9,"interface":"host-l3","router_addresses":["10.64.0.1"]})
    } else {
        serde_json::json!({"backend":"wintun","interface":"HostL3","router_addresses":["10.64.0.1"]})
    };
    let config = RuntimeConfig::parse(&serde_json::json!({"runtime":{"network":{"direct_packet_device":backend}},"route":{"rules":[],"final":{"type":"direct"}}}).to_string()).unwrap();
    let error = match Proxy::new(config) {
        Ok(_) => panic!("foreign host backend must fail before startup"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("host binding backend supported on this platform"));
}

#[test]
fn proxy_exports_network_facts_and_stable_global_limitations() {
    let config = RuntimeConfig::parse(
        r#"{
            "route": {
                "rules": [],
                "final": { "type": "direct" }
            }
        }"#,
    )
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy);
    let QueryResponse::Capabilities(capabilities) = handle
        .query(QueryRequest::Capabilities(CapabilitiesQuery))
        .expect("query capabilities")
    else {
        panic!("expected capabilities response");
    };

    assert!(capabilities.contracts.is_some());
    #[cfg(feature = "wireguard")]
    assert!(
        !capabilities
            .protocols
            .iter()
            .find(|p| p.protocol == "wireguard")
            .unwrap()
            .limitations
            .iter()
            .any(|limit| limit == "direct_packet_sink_unavailable"),
        "host PacketSink availability is declared by the platform capability, not WireGuard"
    );
    let mut expected_features = vec![
        "query",
        "route_bypass_v1",
        "config_snapshot",
        "runtime_snapshot",
        "traffic_observation_v1",
        "traffic_period_reset_v1",
        "traffic_scopes_sampling_v1",
        "traffic_failed_flow_counters_v1",
        "traffic_peer_flow_bindings_v1",
        "traffic_packet_pin_activity_v1",
        "flow_snapshot",
        "policy_snapshot",
        "runtime_generation",
        "operation_correlation",
        "event_recovery",
        "principal_flow_observations_v1",
        "urltest_tolerance",
        "diagnostic_probe_health_isolation_v1",
        "direct_tcp_dial_attempt_observability_v1",
        "traffic_outbound_carrier_io_v1",
        "traffic_local_drop_reasons_v1",
        "direct_tcp_trusted_target_candidate_fallback",
        "network_endpoint_catalog_v1",
        "network_endpoint_orchestration_lifecycle_v1",
    ];
    #[cfg(feature = "raw-ip-runtime")]
    expected_features.extend([
        "traffic_outbound_inner_role_io_v1",
        "network_endpoint_device_incarnation_v1",
        "network_endpoint_network_recovery_v1",
        "packet_route_management_v1",
        "raw_ip_local_echo_v1",
        "traffic_packet_route_idle_observation_v1",
    ]);
    #[cfg(all(
        feature = "host-network-stats",
        any(target_os = "linux", target_os = "macos")
    ))]
    expected_features.push("traffic_host_interface_statistics_v1");
    let mut expected_limitations = vec![
        "traffic_network_loss_unobservable",
        "direct_udp_trusted_candidate_retarget_unsupported",
    ];
    #[cfg(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    ))]
    expected_features.push("direct_packet_host_descriptor_v1");
    #[cfg(not(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    )))]
    expected_limitations.push("direct_packet_host_descriptor_unavailable");
    #[cfg(all(feature = "raw-ip-runtime", target_os = "windows"))]
    expected_features.push("direct_packet_host_wintun_v1");
    #[cfg(feature = "wireguard")]
    {
        expected_features.push("network_endpoint_control_v1");
        expected_features.extend([
            "network_endpoint_control_preconditions_v1",
            "network_endpoint_operation_capabilities_v1",
        ]);
        expected_limitations.extend(["legacy_endpoint_source_file_control_unsupported"]);
    }
    #[cfg(not(feature = "wireguard"))]
    expected_limitations.push("endpoint_runtime_lifecycle_commands_not_registered");

    #[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
    {
        expected_features.extend([
            "traffic_host_tun_io_v1",
            "tun_dual_stack_ingress",
            "tun_family_aware_egress",
            "direct_tun_domain_family_fallback",
            "tun_runtime_egress_reconciliation",
            "tun_strict_route",
        ]);
        expected_limitations.extend([
            "tun_nat64_unsupported",
            "tun_bare_ipv6_requires_trusted_domain",
        ]);
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    expected_limitations.push("tun_platform_unsupported");

    #[cfg(feature = "dns")]
    {
        expected_features.extend([
            "tun_dns_hijack_udp_tcp",
            "dns_split_dispatch",
            "dns_fake_ip_dual_stack",
            "dns_fake_ip_persistence",
            "dns_fake_ip_transactional_reload",
            "dns_real_reverse_mapping",
            "dns_upstream_egress_binding",
            "dns_address_family_policy",
            "dns_wire_ttl_aging",
        ]);
        expected_limitations.extend([
            "dns_encrypted_client_queries_not_intercepted",
            "dns_ech_hostname_recovery_unavailable",
            "dns_doq_detour_unsupported",
        ]);
        #[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
        expected_features.push("tun_dns_system_auto");
    }

    #[cfg(not(feature = "dns"))]
    expected_limitations.push("tun_dns_hijack_unavailable");

    expected_features.sort_unstable();
    expected_limitations.sort_unstable();
    assert_eq!(capabilities.features, expected_features);
    assert_eq!(capabilities.global_limitations, expected_limitations);
    assert!(capabilities.features.iter().all(|code| is_snake_case(code)));
    assert!(capabilities
        .global_limitations
        .iter()
        .all(|code| is_snake_case(code)));
}

fn is_snake_case(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        && !value.starts_with('_')
        && !value.ends_with('_')
        && !value.contains("__")
}
