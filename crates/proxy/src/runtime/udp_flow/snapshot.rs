use zero_core::Session;
use zero_engine::PassiveRelaySelection;

use super::outbound::UdpFlowOutbound;
use super::rate_limit::UdpFlowRateLimiters;

#[derive(Debug, Clone)]
pub(crate) struct UdpFlowSnapshot {
    pub(crate) session: Session,
    pub(crate) outbound: UdpFlowOutbound,
    /// Client session isolation key (SIP022 3.2.4).
    pub(crate) client_session_id: Option<u64>,
    pub(crate) passive_relay_selections: Vec<PassiveRelaySelection>,
    pub(crate) rate_limiters: UdpFlowRateLimiters,
}
