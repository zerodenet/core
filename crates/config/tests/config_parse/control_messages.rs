use zero_config::{RuntimeConfig, RuntimeOptionsConfig};
#[test]
fn control_payload_and_command_dimensions_are_optional_and_validate_the_actual_finite_boundaries() {
    let runtime = RuntimeOptionsConfig::default();
    let omitted = serde_json::to_value(&runtime).unwrap();
    type Field = (
        &'static str,
        fn(&RuntimeOptionsConfig) -> usize,
        usize,
        usize,
    );
    let fields: [Field; 13] = [
        (
            "control_sse_max_event_frames",
            RuntimeOptionsConfig::effective_control_sse_max_event_frames,
            32,
            4096,
        ),
        (
            "control_max_in_flight_request_bytes",
            RuntimeOptionsConfig::effective_control_max_in_flight_request_bytes
                as fn(&RuntimeOptionsConfig) -> usize,
            64 * 1024 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "control_max_in_flight_response_bytes",
            RuntimeOptionsConfig::effective_control_max_in_flight_response_bytes,
            64 * 1024 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "control_ipc_max_frame_bytes",
            RuntimeOptionsConfig::effective_control_ipc_max_frame_bytes,
            64 * 1024 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "control_http_max_header_bytes",
            RuntimeOptionsConfig::effective_control_http_max_header_bytes,
            8192,
            1024 * 1024,
        ),
        (
            "control_http_max_body_bytes",
            RuntimeOptionsConfig::effective_control_http_max_body_bytes,
            256 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "control_max_response_bytes",
            RuntimeOptionsConfig::effective_control_max_response_bytes,
            64 * 1024 * 1024,
            256 * 1024 * 1024,
        ),
        (
            "control_ipc_max_pending_commands",
            RuntimeOptionsConfig::effective_control_ipc_max_pending_commands,
            32,
            4096,
        ),
        (
            "control_max_pending_commands",
            RuntimeOptionsConfig::effective_control_max_pending_commands,
            1024,
            4096,
        ),
        (
            "control_ipc_max_event_frames",
            RuntimeOptionsConfig::effective_control_ipc_max_event_frames,
            32,
            4096,
        ),
        (
            "control_grpc_max_requests",
            RuntimeOptionsConfig::effective_control_grpc_max_requests,
            1024,
            4096,
        ),
        (
            "control_grpc_max_streams_per_connection",
            RuntimeOptionsConfig::effective_control_grpc_max_streams_per_connection,
            32,
            32,
        ),
        (
            "control_grpc_max_request_bytes",
            RuntimeOptionsConfig::effective_control_grpc_max_request_bytes,
            4 * 1024 * 1024,
            256 * 1024 * 1024,
        ),
    ];
    for (field, effective, default, max) in fields {
        assert!(omitted.get(field).is_none());
        assert_eq!(effective(&runtime), default);
        for value in [1, max] {
            let config = RuntimeConfig::parse(
                &serde_json::json!({"runtime":{field:value},"route":{"final":{"type":"direct"}}})
                    .to_string(),
            )
            .unwrap();
            assert_eq!(effective(&config.runtime), value);
            let round: RuntimeOptionsConfig =
                serde_json::from_value(serde_json::to_value(config.runtime).unwrap()).unwrap();
            assert_eq!(effective(&round), value);
        }
        for value in [0, max + 1, usize::MAX] {
            let rejected = RuntimeConfig::parse(
                &serde_json::json!({"runtime":{field:value},"route":{"final":{"type":"direct"}}})
                    .to_string(),
            )
            .unwrap_err();
            assert!(rejected.to_string().contains(field));
        }
    }
}
