pub(super) fn extend(capabilities: &mut zero_api::ApiCapabilities) {
    capabilities.features.extend(
        [
            "direct_tcp_dial_attempt_observability_v1",
            "direct_tcp_trusted_target_candidate_fallback",
        ]
        .into_iter()
        .map(str::to_owned),
    );
    capabilities
        .global_limitations
        .push("direct_udp_trusted_candidate_retarget_unsupported".to_owned());

    #[cfg(any(target_os = "windows", target_os = "linux", target_os = "macos"))]
    {
        capabilities.features.extend(
            [
                "tun_dual_stack_ingress",
                "tun_family_aware_egress",
                "direct_tun_domain_family_fallback",
                "tun_runtime_egress_reconciliation",
                "tun_strict_route",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        capabilities.global_limitations.extend(
            [
                "tun_nat64_unsupported",
                "tun_bare_ipv6_requires_trusted_domain",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    capabilities
        .global_limitations
        .push("tun_platform_unsupported".to_owned());

    #[cfg(feature = "dns")]
    {
        capabilities.features.extend(
            [
                "tun_dns_hijack_udp_tcp",
                "dns_split_dispatch",
                "dns_fake_ip_dual_stack",
                "dns_fake_ip_persistence",
                "dns_fake_ip_transactional_reload",
                "dns_real_reverse_mapping",
                "dns_upstream_egress_binding",
                "dns_address_family_policy",
                "dns_wire_ttl_aging",
            ]
            .into_iter()
            .map(str::to_owned),
        );
        capabilities.global_limitations.extend(
            [
                "dns_encrypted_client_queries_not_intercepted",
                "dns_ech_hostname_recovery_unavailable",
                "dns_doq_detour_unsupported",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }

    #[cfg(all(
        feature = "dns",
        any(target_os = "windows", target_os = "linux", target_os = "macos")
    ))]
    capabilities.features.push("tun_dns_system_auto".to_owned());

    #[cfg(not(feature = "dns"))]
    capabilities
        .global_limitations
        .push("tun_dns_hijack_unavailable".to_owned());

    capabilities.features.sort_unstable();
    capabilities.features.dedup();
    capabilities.global_limitations.sort_unstable();
    capabilities.global_limitations.dedup();
}
