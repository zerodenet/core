//! Typed limits describe the existing root control owner, never an allocator.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlBudgetLimits {
    pub connections: usize,
    pub subscriptions: usize,
    pub queued_event_bytes: usize,
    pub pipe_workspace_bytes: usize,
    pub messages: ControlMessageLimits,
}
impl ControlBudgetLimits {
    pub fn from_runtime(runtime: &zero_config::RuntimeOptionsConfig) -> Self {
        Self {
            connections: runtime.effective_control_max_connections(),
            subscriptions: runtime.effective_control_max_subscriptions(),
            queued_event_bytes: runtime.effective_control_max_queued_event_bytes(),
            pipe_workspace_bytes: runtime.effective_control_pipe_max_workspace_bytes(),
            messages: ControlMessageLimits::from_runtime(runtime),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlMessageLimits {
    pub request_bytes_total: usize,
    pub response_bytes_total: usize,
    pub ipc_frame_bytes: usize,
    pub http_header_bytes: usize,
    pub http_body_bytes: usize,
    pub response_bytes: usize,
    pub ipc_pending_commands: usize,
    pub pending_commands: usize,
    pub ipc_event_frames: usize,
}
impl ControlMessageLimits {
    pub fn from_runtime(runtime: &zero_config::RuntimeOptionsConfig) -> Self {
        Self {
            request_bytes_total: runtime.effective_control_max_in_flight_request_bytes(),
            response_bytes_total: runtime.effective_control_max_in_flight_response_bytes(),
            ipc_frame_bytes: runtime.effective_control_ipc_max_frame_bytes(),
            http_header_bytes: runtime.effective_control_http_max_header_bytes(),
            http_body_bytes: runtime.effective_control_http_max_body_bytes(),
            response_bytes: runtime.effective_control_max_response_bytes(),
            ipc_pending_commands: runtime.effective_control_ipc_max_pending_commands(),
            pending_commands: runtime.effective_control_max_pending_commands(),
            ipc_event_frames: runtime.effective_control_ipc_max_event_frames(),
        }
    }
}
impl Default for ControlMessageLimits {
    fn default() -> Self {
        Self::from_runtime(&zero_config::RuntimeOptionsConfig::default())
    }
}
