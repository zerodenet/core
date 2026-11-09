use zero_engine::PassiveRelaySelection;

use super::outbound::UdpFlowOutbound;
use super::rate_limit::UdpFlowRateLimiters;
use super::sessions::UdpFlowKey;

#[derive(Debug, Clone)]
pub(crate) struct UdpFlowSnapshot {
    /// Stable identity from the inbound packet before Fake-IP restoration.
    pub(crate) key: UdpFlowKey,
    pub(crate) session: super::sessions::UdpFlowSession,
    pub(crate) outbound: UdpFlowOutbound,
    pub(crate) passive_relay_selections: Vec<PassiveRelaySelection>,
    pub(crate) rate_limiters: UdpFlowRateLimiters,
}
