//! One immutable feature-code census shared by owned and prepared discovery.
pub(in crate::runtime::handle) fn host_sampling() -> bool {
    #[cfg(feature = "host-network-stats")]
    {
        zero_platform_tokio::network_statistics::SUPPORTED
    }
    #[cfg(not(feature = "host-network-stats"))]
    {
        false
    }
}
pub(super) fn extend(capabilities: &mut zero_api::ApiCapabilities, endpoint_control: bool) {
    visit_codes(
        endpoint_control,
        |code| capabilities.features.push(code.to_owned()),
        |code| capabilities.global_limitations.push(code.to_owned()),
    );
    if host_sampling() {
        if let Some(stats) = &mut capabilities.traffic_statistics {
            stats.host_interface_sampling = true;
            stats.resettable_scopes.push("host_interface".into());
            stats
                .events
                .push(zero_api::event_type::STATS_HOST_INTERFACES_SAMPLED.into());
        }
    }
    capabilities.features.sort_unstable();
    capabilities.features.dedup();
    capabilities.global_limitations.sort_unstable();
    capabilities.global_limitations.dedup();
}
pub(in crate::runtime::handle) fn visit_codes(
    endpoint_control: bool,
    mut feature: impl FnMut(&'static str),
    mut limitation: impl FnMut(&'static str),
) {
    for code in [
        "diagnostic_probe_health_isolation_v1",
        "network_endpoint_catalog_v1",
    ] {
        feature(code);
    }
    if endpoint_control {
        for code in [
            "network_endpoint_control_v1",
            "network_endpoint_control_preconditions_v1",
            "network_endpoint_operation_capabilities_v1",
        ] {
            feature(code);
        }
        limitation("legacy_endpoint_source_file_control_unsupported");
    } else {
        limitation("endpoint_runtime_lifecycle_commands_not_registered");
    }
    feature("network_endpoint_orchestration_lifecycle_v1");
    #[cfg(feature = "raw-ip-runtime")]
    for code in [
        "network_endpoint_device_incarnation_v1",
        "network_endpoint_network_recovery_v1",
        "packet_route_management_v1",
    ] {
        feature(code);
    }
    #[cfg(all(feature = "raw-ip-runtime", target_os = "windows"))]
    feature("direct_packet_host_wintun_v1");
    #[cfg(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    ))]
    feature("direct_packet_host_descriptor_v1");
    #[cfg(not(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    )))]
    limitation("direct_packet_host_descriptor_unavailable");
    for code in [
        "direct_tcp_dial_attempt_observability_v1",
        "direct_outbound_dial_policy_v1",
        "traffic_outbound_carrier_io_v1",
        "traffic_local_drop_reasons_v1",
        "direct_tcp_trusted_target_candidate_fallback",
    ] {
        feature(code);
    }
    if host_sampling() {
        feature("traffic_host_interface_statistics_v1");
    }
    limitation("traffic_network_loss_unobservable");
    #[cfg(feature = "raw-ip-runtime")]
    for code in [
        "traffic_outbound_inner_role_io_v1",
        "raw_ip_local_echo_v1",
        "traffic_packet_route_idle_observation_v1",
    ] {
        feature(code);
    }
    for code in [
        "direct_udp_trusted_candidate_retarget_unsupported",
        "nondefault_dial_direct_tcp_udp_only",
        "nondefault_dial_native_packet_icmp_unsupported",
        "ipv6_wildcard_dual_stack_platform_dependent",
    ] {
        limitation(code);
    }
    #[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
    {
        for code in [
            "traffic_host_tun_io_v1",
            "tun_dual_stack_ingress",
            "tun_family_aware_egress",
            "direct_tun_domain_family_fallback",
            "tun_runtime_egress_reconciliation",
            "tun_strict_route",
        ] {
            feature(code);
        }
        for code in [
            "tun_nat64_unsupported",
            "tun_bare_ipv6_requires_trusted_domain",
        ] {
            limitation(code);
        }
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    limitation("tun_platform_unsupported");
    #[cfg(feature = "dns")]
    {
        for code in [
            "tun_dns_hijack_udp_tcp",
            "dns_split_dispatch",
            "dns_fake_ip_dual_stack",
            "dns_fake_ip_persistence",
            "dns_fake_ip_transactional_reload",
            "dns_real_reverse_mapping",
            "dns_upstream_egress_binding",
            "dns_address_family_policy",
            "dns_wire_ttl_aging",
        ] {
            feature(code);
        }
        for code in [
            "dns_encrypted_client_queries_not_intercepted",
            "dns_ech_hostname_recovery_unavailable",
            "dns_doq_detour_unsupported",
        ] {
            limitation(code);
        }
    }
    #[cfg(all(
        feature = "dns",
        any(target_os = "windows", target_os = "linux", target_os = "macos")
    ))]
    feature("tun_dns_system_auto");
    #[cfg(not(feature = "dns"))]
    limitation("tun_dns_hijack_unavailable");
}
