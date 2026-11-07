use zero_engine::EngineError;

use super::outbound::UdpFlowOutbound;

/// Result of establishing a new persistent UDP flow.
pub(crate) enum FlowStartResult {
    Flow {
        outbound: Box<UdpFlowOutbound>,
        tx_bytes: u64,
    },
    Blocked {
        tag: String,
    },
}

/// Failure details produced while establishing or forwarding a UDP flow.
pub(crate) struct FlowFailure {
    pub(crate) stage: &'static str,
    pub(crate) error: EngineError,
    pub(crate) upstream: Option<(String, u16)>,
}

/// Read only the existing explicit ChannelError-to-Engine bridge. Never infer
/// packet disposition from an io kind, message or a diagnostic source chain.
pub(crate) fn packet_rejected_before_submission(error: &EngineError) -> bool {
    use zero_platform_tokio::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
    let EngineError::Io(error) = error else {
        return false;
    };
    error
        .get_ref()
        .and_then(|error| error.downcast_ref::<ChannelError>())
        .is_some_and(|error| {
            error.stage == Stage::Send
                && error.delivery == DeliveryState::NotSubmitted
                && error.impact == Impact::Datagram
                && matches!(error.reason, Reason::Protocol | Reason::Unsupported)
                && error.cleanup_failure_receipt().is_none()
        })
}
#[cfg(test)]
#[path = "result/tests.rs"]
mod tests;
