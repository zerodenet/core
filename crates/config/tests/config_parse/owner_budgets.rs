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
    let fields: [BudgetField; 75] = [
        (
            "packet_route_max_routes",
            RuntimeOptionsConfig::effective_packet_route_max_routes,
            4096,
            4096,
        ),
        (
            "control_pipe_max_workspace_bytes",
            RuntimeOptionsConfig::effective_control_pipe_max_workspace_bytes,
            2 * 1024 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "direct_max_originals",
            RuntimeOptionsConfig::effective_direct_max_originals,
            4096,
            4096,
        ),
        (
            "direct_max_tasks",
            RuntimeOptionsConfig::effective_direct_max_tasks,
            256,
            4096,
        ),
        (
            "inbound_max_accepted_sockets",
            RuntimeOptionsConfig::effective_inbound_max_accepted_sockets,
            4096,
            65536,
        ),
        (
            "inbound_max_datagram_associations",
            RuntimeOptionsConfig::effective_inbound_max_datagram_associations,
            4096,
            65536,
        ),
        (
            "inbound_max_datagram_peers",
            |r| r.effective_inbound_max_datagram_peers(128),
            128,
            65536,
        ),
        (
            "inbound_max_packets_per_peer",
            |r| r.effective_inbound_max_packets_per_peer(256),
            256,
            65536,
        ),
        (
            "inbound_max_pending_sends",
            |r| r.effective_inbound_max_pending_sends(32),
            32,
            65536,
        ),
        (
            "inbound_max_bind_attempts",
            RuntimeOptionsConfig::effective_inbound_max_bind_attempts,
            50,
            4096,
        ),
        (
            "inbound_max_target_sockets",
            RuntimeOptionsConfig::effective_inbound_max_target_sockets,
            4096,
            65536,
        ),
        (
            "inbound_max_file_sessions",
            RuntimeOptionsConfig::effective_inbound_max_file_sessions,
            16,
            4096,
        ),
        (
            "inbound_max_handoff_probe_attempts",
            RuntimeOptionsConfig::effective_inbound_max_handoff_probe_attempts,
            1000,
            65536,
        ),
        (
            "hysteria2_h3_max_pending_classifications",
            RuntimeOptionsConfig::effective_hysteria2_h3_max_pending_classifications,
            128,
            4096,
        ),
        (
            "hysteria2_h3_max_pending_tcp_streams",
            RuntimeOptionsConfig::effective_hysteria2_h3_max_pending_tcp_streams,
            128,
            4096,
        ),
        (
            "hysteria2_h3_max_pending_web_streams",
            RuntimeOptionsConfig::effective_hysteria2_h3_max_pending_web_streams,
            128,
            4096,
        ),
        (
            "hysteria2_h3_max_pending_web_requests",
            RuntimeOptionsConfig::effective_hysteria2_h3_max_pending_web_requests,
            64,
            4096,
        ),
        (
            "inbound_max_tasks_per_carrier",
            |r| r.effective_inbound_max_tasks_per_carrier(520),
            520,
            4096,
        ),
        (
            "inbound_max_in_flight_requests",
            |r| r.effective_inbound_max_in_flight_requests(256),
            256,
            65536,
        ),
        (
            "metadata_max_probe_sockets",
            |runtime| runtime.effective_metadata_max_probe_sockets(2),
            2,
            256,
        ),
        (
            "metadata_max_probe_attempts",
            |runtime| runtime.effective_metadata_max_probe_attempts(8),
            8,
            4096,
        ),
        (
            "metadata_max_send_calls",
            |runtime| runtime.effective_metadata_max_send_calls(2),
            2,
            4096,
        ),
        (
            "quic_source_max_connections",
            RuntimeOptionsConfig::effective_quic_source_max_connections,
            8192,
            65536,
        ),
        (
            "quic_shared_endpoint_max_users",
            RuntimeOptionsConfig::effective_quic_shared_endpoint_max_users,
            65536,
            65536,
        ),
        (
            "quic_max_queue_packets",
            RuntimeOptionsConfig::effective_quic_max_queue_packets,
            256,
            256,
        ),
        (
            "component_total_max_tasks",
            RuntimeOptionsConfig::effective_component_total_max_tasks,
            4096,
            65536,
        ),
        (
            "quic_total_max_buffered_bytes",
            RuntimeOptionsConfig::effective_quic_total_max_buffered_bytes,
            64 * 1024 * 1024,
            1_073_741_824,
        ),
        (
            "event_log_stats_max_bytes",
            RuntimeOptionsConfig::effective_event_log_stats_max_bytes,
            8 * 1024 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "event_subscriber_max_pending_events",
            RuntimeOptionsConfig::effective_event_subscriber_max_pending_events,
            1024,
            1024,
        ),
        (
            "event_subscriber_total_max_pending_events",
            RuntimeOptionsConfig::effective_event_subscriber_total_max_pending_events,
            65536,
            1048576,
        ),
        (
            "event_subscriber_max_registrations",
            RuntimeOptionsConfig::effective_event_subscriber_max_registrations,
            4096,
            65536,
        ),
        (
            "event_subscriber_total_stats_max_bytes",
            RuntimeOptionsConfig::effective_event_subscriber_total_stats_max_bytes,
            67108864,
            268435456,
        ),
        (
            "event_subscriber_max_filter_bytes",
            RuntimeOptionsConfig::effective_event_subscriber_max_filter_bytes,
            65536,
            1048576,
        ),
        (
            "event_subscriber_total_max_storage_bytes",
            RuntimeOptionsConfig::effective_event_subscriber_total_max_storage_bytes,
            8388608,
            268435456,
        ),
        (
            "event_subscriber_stats_max_bytes",
            RuntimeOptionsConfig::effective_event_subscriber_stats_max_bytes,
            1024 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "event_network_max_tasks",
            RuntimeOptionsConfig::effective_event_network_max_tasks,
            1024,
            4096,
        ),
        (
            "event_network_max_originals",
            RuntimeOptionsConfig::effective_event_network_max_originals,
            256,
            4096,
        ),
        (
            "event_network_max_lookups",
            RuntimeOptionsConfig::effective_event_network_max_lookups,
            256,
            4096,
        ),
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
            "navigation_max_jobs",
            RuntimeOptionsConfig::effective_navigation_max_jobs,
            64,
            64,
        ),
        (
            "navigation_max_requests",
            RuntimeOptionsConfig::effective_navigation_max_requests,
            32,
            32,
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
            "packet_transmit_total_max_frames",
            RuntimeOptionsConfig::effective_packet_transmit_total_max_frames,
            16384,
            65536,
        ),
        (
            "packet_transmit_total_max_bytes",
            RuntimeOptionsConfig::effective_packet_transmit_total_max_bytes,
            67108864,
            1073741824,
        ),
        (
            "packet_native_read_total_max_frames",
            RuntimeOptionsConfig::effective_packet_native_read_total_max_frames,
            256,
            65536,
        ),
        (
            "packet_native_read_total_max_bytes",
            RuntimeOptionsConfig::effective_packet_native_read_total_max_bytes,
            16777216,
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

#[test]
fn omitted_metadata_limits_preserve_distinct_actual_dns_and_shared_endpoint_defaults() {
    let absent = RuntimeOptionsConfig::default();
    assert_eq!(absent.effective_metadata_max_probe_sockets(1), 1);
    assert_eq!(absent.effective_metadata_max_probe_sockets(2), 2);
    assert_eq!(absent.effective_metadata_max_probe_attempts(4), 4);
    assert_eq!(absent.effective_metadata_max_probe_attempts(8), 8);
    assert_eq!(absent.effective_metadata_max_send_calls(1), 1);
    assert_eq!(absent.effective_metadata_max_send_calls(2), 2);
    let configured = parse("metadata_max_probe_sockets", 7).unwrap();
    assert_eq!(
        configured.runtime.effective_metadata_max_probe_sockets(1),
        7
    );
    assert_eq!(
        configured.runtime.effective_metadata_max_probe_sockets(2),
        7
    );
}

#[test]
fn inbound_resource_zero_and_actual_defaults_preserve_original_admission_meaning() {
    let defaults = RuntimeOptionsConfig::default();
    for actual in [0, 2, 257] {
        assert_eq!(
            defaults.effective_inbound_max_resources_per_carrier(actual),
            actual
        );
    }
    for actual in [1, 2, 514, 516, 520] {
        assert_eq!(
            defaults.effective_inbound_max_tasks_per_carrier(actual),
            actual
        );
    }
    for actual in [1, 128, 256] {
        assert_eq!(
            defaults.effective_inbound_max_in_flight_requests(actual),
            actual
        );
    }
    assert!(serde_json::to_value(&defaults)
        .unwrap()
        .get("inbound_max_resources_per_carrier")
        .is_none());
    let empty = parse("inbound_max_resources_per_carrier", 0).unwrap();
    assert_eq!(
        empty
            .runtime
            .effective_inbound_max_resources_per_carrier(257),
        0
    );
    assert!(parse("inbound_max_resources_per_carrier", 65536).is_ok());
    assert!(parse("inbound_max_resources_per_carrier", 65537).is_err());
}

#[test]
fn windows_ring_requested_bytes_preserve_omission_and_the_original_sdk_constraints() {
    let defaults = RuntimeOptionsConfig::default();
    assert_eq!(defaults.effective_packet_windows_ring_capacity(), 67108864);
    assert_eq!(
        defaults.effective_packet_windows_ring_total_max_bytes(),
        134217728
    );
    let absent = serde_json::to_value(defaults).unwrap();
    for field in [
        "packet_windows_ring_capacity",
        "packet_windows_ring_total_max_bytes",
    ] {
        assert!(absent.get(field).is_none());
    }
    for (field, values) in [
        (
            "packet_windows_ring_capacity",
            &[131072, 262144, 67108864][..],
        ),
        (
            "packet_windows_ring_total_max_bytes",
            &[131072, 131073, 134217728, 1073741824][..],
        ),
    ] {
        for &value in values {
            let config = parse(field, value).unwrap();
            let encoded = serde_json::to_string(&config).unwrap();
            let restored = RuntimeConfig::parse(&encoded).unwrap();
            assert_eq!(
                serde_json::to_value(restored).unwrap()["runtime"][field],
                value
            );
        }
    }
    for value in [0, 1, 131071, 131073, 67108863, 67108865, usize::MAX] {
        assert!(parse("packet_windows_ring_capacity", value)
            .unwrap_err()
            .to_string()
            .contains("packet_windows_ring_capacity"));
    }
    for value in [0, 131071, 1073741825, usize::MAX] {
        assert!(parse("packet_windows_ring_total_max_bytes", value)
            .unwrap_err()
            .to_string()
            .contains("packet_windows_ring_total_max_bytes"));
    }
    // On-demand shortage is handled by the real owner before StartSession;
    // validation neither allocates a session nor rejects this legal policy.
    let config = parse("packet_windows_ring_total_max_bytes", 131072).unwrap();
    assert!(
        config
            .runtime
            .effective_packet_windows_ring_total_max_bytes()
            < config.runtime.effective_packet_windows_ring_capacity()
    );
}
