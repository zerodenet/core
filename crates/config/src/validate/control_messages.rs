//! Exact control payload/queue dimensions, independent of native and event budgets.
use super::*;
pub(super) fn validate(runtime: &RuntimeOptionsConfig) -> Result<(), ConfigError> {
    for (name, value, max) in [
        (
            "control_max_in_flight_request_bytes",
            runtime.effective_control_max_in_flight_request_bytes(),
            268435456,
        ),
        (
            "control_max_in_flight_response_bytes",
            runtime.effective_control_max_in_flight_response_bytes(),
            268435456,
        ),
        (
            "control_ipc_max_frame_bytes",
            runtime.effective_control_ipc_max_frame_bytes(),
            268435456,
        ),
        (
            "control_http_max_header_bytes",
            runtime.effective_control_http_max_header_bytes(),
            1048576,
        ),
        (
            "control_http_max_body_bytes",
            runtime.effective_control_http_max_body_bytes(),
            268435456,
        ),
        (
            "control_max_response_bytes",
            runtime.effective_control_max_response_bytes(),
            268435456,
        ),
        (
            "control_ipc_max_pending_commands",
            runtime.effective_control_ipc_max_pending_commands(),
            4096,
        ),
        (
            "control_max_pending_commands",
            runtime.effective_control_max_pending_commands(),
            4096,
        ),
        (
            "control_ipc_max_event_frames",
            runtime.effective_control_ipc_max_event_frames(),
            4096,
        ),
    ] {
        if !(1..=max).contains(&value) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and {max}"
            )));
        }
    }
    Ok(())
}
