use zero_config::{RuntimeConfig, RuntimeOptionsConfig};

fn parse(name: &str, value: usize) -> Result<RuntimeConfig, zero_config::ConfigError> {
    let mut raw = serde_json::json!({"route":{"final":{"type":"direct"}}});
    raw["runtime"] = serde_json::json!({name:value});
    RuntimeConfig::parse(&raw.to_string())
}

#[test]
fn actual_owner_budgets_are_optional_finite_and_preserve_omitted_export_shape() {
    let defaults = RuntimeOptionsConfig::default();
    let exported = serde_json::to_value(&defaults).unwrap();
    type BudgetField = (
        &'static str,
        fn(&RuntimeOptionsConfig) -> usize,
        usize,
        usize,
    );
    let fields: [BudgetField; 31] = [
        (
            "carrier_max_total_originals",
            RuntimeOptionsConfig::effective_carrier_max_total_originals,
            1024,
            65536,
        ),
        (
            "ss_udp_max_pending_responses",
            RuntimeOptionsConfig::effective_ss_udp_max_pending_responses,
            4096,
            65536,
        ),
        (
            "ss_udp_total_max_pending_responses",
            RuntimeOptionsConfig::effective_ss_udp_total_max_pending_responses,
            4096,
            65536,
        ),
        (
            "ss_legacy_replay_max_bytes",
            RuntimeOptionsConfig::effective_ss_legacy_replay_max_bytes,
            64 * 1024 * 1024,
            usize::MAX - 1,
        ),
        (
            "grpc_pool_max_carriers",
            RuntimeOptionsConfig::effective_grpc_pool_max_carriers,
            4,
            4096,
        ),
        (
            "grpc_pool_max_streams",
            RuntimeOptionsConfig::effective_grpc_pool_max_streams,
            256,
            65536,
        ),
        (
            "grpc_total_max_carriers",
            RuntimeOptionsConfig::effective_grpc_total_max_carriers,
            16,
            4096,
        ),
        (
            "grpc_total_max_streams",
            RuntimeOptionsConfig::effective_grpc_total_max_streams,
            1024,
            65536,
        ),
        (
            "diagnostic_max_operations",
            RuntimeOptionsConfig::effective_diagnostic_max_operations,
            8,
            8,
        ),
        (
            "ss_udp_replay_max_windows",
            RuntimeOptionsConfig::effective_ss_udp_replay_max_windows,
            8192,
            usize::MAX - 1,
        ),
        (
            "ss_udp_max_senders",
            RuntimeOptionsConfig::effective_ss_udp_max_senders,
            8192,
            usize::MAX - 1,
        ),
        (
            "ss_udp_max_associations",
            RuntimeOptionsConfig::effective_ss_udp_max_associations,
            8192,
            usize::MAX - 1,
        ),
        (
            "ss_udp_max_return_bindings",
            RuntimeOptionsConfig::effective_ss_udp_max_return_bindings,
            65536,
            usize::MAX - 1,
        ),
        (
            "ss_udp_max_authenticated_codecs",
            RuntimeOptionsConfig::effective_ss_udp_max_authenticated_codecs,
            4096,
            usize::MAX - 1,
        ),
        (
            "packet_return_max_frames",
            RuntimeOptionsConfig::effective_packet_return_max_frames,
            256,
            65536,
        ),
        (
            "packet_return_max_bytes",
            RuntimeOptionsConfig::effective_packet_return_max_bytes,
            16777216,
            1073741824,
        ),
        (
            "packet_return_total_max_frames",
            RuntimeOptionsConfig::effective_packet_return_total_max_frames,
            1024,
            65536,
        ),
        (
            "packet_return_total_max_bytes",
            RuntimeOptionsConfig::effective_packet_return_total_max_bytes,
            67108864,
            1073741824,
        ),
        (
            "ss_tcp_replay_max_salts",
            RuntimeOptionsConfig::effective_ss_tcp_replay_max_salts,
            500_000,
            usize::MAX - 1,
        ),
        (
            "vmess_grpc_max_originals",
            RuntimeOptionsConfig::effective_vmess_grpc_max_originals,
            4096,
            4096,
        ),
        (
            "native_stream_max_originals",
            RuntimeOptionsConfig::effective_native_stream_max_originals,
            4,
            4096,
        ),
        (
            "mieru_max_carriers",
            RuntimeOptionsConfig::effective_mieru_max_carriers,
            4,
            256,
        ),
        (
            "hysteria2_max_entries",
            RuntimeOptionsConfig::effective_hysteria2_max_entries,
            256,
            256,
        ),
        (
            "hysteria2_max_udp_sessions",
            RuntimeOptionsConfig::effective_hysteria2_max_udp_sessions,
            4096,
            4096,
        ),
        (
            "hysteria2_udp_pending_bytes",
            RuntimeOptionsConfig::effective_hysteria2_udp_pending_bytes,
            4 * 1024 * 1024,
            64 * 1024 * 1024,
        ),
        (
            "control_max_connections",
            RuntimeOptionsConfig::effective_control_max_connections,
            256,
            4096,
        ),
        (
            "control_max_subscriptions",
            RuntimeOptionsConfig::effective_control_max_subscriptions,
            256,
            4096,
        ),
        (
            "native_dns_max_originals",
            RuntimeOptionsConfig::effective_native_dns_max_originals,
            16,
            4096,
        ),
        (
            "native_dns_max_queries",
            RuntimeOptionsConfig::effective_native_dns_max_queries,
            16,
            4096,
        ),
        (
            "native_dns_max_blocking_jobs",
            RuntimeOptionsConfig::effective_native_dns_max_blocking_jobs,
            16,
            4096,
        ),
        (
            "native_dns_max_h2_drivers",
            RuntimeOptionsConfig::effective_native_dns_max_h2_drivers,
            16,
            4096,
        ),
    ];
    for (name, effective, default, maximum) in fields {
        assert!(
            exported.get(name).is_none(),
            "{name} must not change old exported configuration"
        );
        assert_eq!(effective(&defaults), default);
        for value in [1, default, maximum] {
            let config = parse(name, value).unwrap();
            assert_eq!(effective(&config.runtime), value);
            let encoded = serde_json::to_value(config).unwrap();
            assert_eq!(encoded["runtime"][name], value);
            let restored = RuntimeConfig::parse(&encoded.to_string()).unwrap();
            assert_eq!(effective(&restored.runtime), value);
        }
        for value in [0, maximum + 1, usize::MAX] {
            let error = parse(name, value).unwrap_err().to_string();
            assert!(error.contains(name), "{error}");
        }
    }
}

#[test]
fn control_event_payload_budget_preserves_omission_and_checks_real_byte_boundaries() {
    let defaults = RuntimeOptionsConfig::default();
    assert_eq!(
        defaults.effective_control_max_queued_event_bytes(),
        1024 * 1024
    );
    assert!(serde_json::to_value(&defaults)
        .unwrap()
        .get("control_max_queued_event_bytes")
        .is_none());
    for bytes in [1024, 1024 * 1024, 64 * 1024 * 1024] {
        let config = parse("control_max_queued_event_bytes", bytes).unwrap();
        assert_eq!(
            config.runtime.effective_control_max_queued_event_bytes(),
            bytes
        );
        let exported = serde_json::to_string(&config).unwrap();
        assert_eq!(
            RuntimeConfig::parse(&exported)
                .unwrap()
                .runtime
                .effective_control_max_queued_event_bytes(),
            bytes
        );
    }
    for bytes in [0, 1023, 64 * 1024 * 1024 + 1, usize::MAX] {
        assert!(parse("control_max_queued_event_bytes", bytes)
            .unwrap_err()
            .to_string()
            .contains("control_max_queued_event_bytes"));
    }
}
