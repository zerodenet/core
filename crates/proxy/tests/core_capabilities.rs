use zero_api::{CapabilitiesQuery, QueryRequest, QueryResponse, QueryService};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

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
    let mut expected_features = vec![
        "query",
        "config_snapshot",
        "runtime_snapshot",
        "flow_snapshot",
        "policy_snapshot",
        "runtime_generation",
        "operation_correlation",
        "event_recovery",
        "principal_flow_observations_v1",
        "urltest_tolerance",
        "diagnostic_probe_health_isolation_v1",
        "direct_tcp_dial_attempt_observability_v1",
        "direct_tcp_trusted_target_candidate_fallback",
    ];
    let mut expected_limitations = vec!["direct_udp_trusted_candidate_retarget_unsupported"];

    #[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
    {
        expected_features.extend([
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
