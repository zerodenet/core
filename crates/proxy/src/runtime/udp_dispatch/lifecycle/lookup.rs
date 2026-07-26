use std::net::SocketAddr;

#[cfg(feature = "upstream-association-runtime")]
use zero_core::Address;

use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;

impl UdpDispatch {
    pub(crate) fn rate_limiters_by_session_id(
        &self,
        session_id: Option<u64>,
    ) -> UdpFlowRateLimiters {
        self.flows.rate_limiters_by_session_id(session_id)
    }

    pub(crate) fn confirm_passive_health(&self, session_id: Option<u64>) {
        let Some(session_id) = session_id else {
            return;
        };
        if let Some((session, selections)) = self.flows.confirm_passive_health(session_id) {
            self.runtime.record_passive_relay_outcome(
                &selections,
                &session,
                zero_engine::PassiveRelayOutcome::Success,
            );
        }
    }

    /// Look up the session ID for a direct response sender.
    pub(crate) fn direct_response_session_id(&self, sender: SocketAddr) -> Option<u64> {
        self.flows.direct_response_session_id(sender)
    }

    /// Look up a session ID by target+port only, regardless of outbound type.
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn session_id_by_target(
        &self,
        target: &Address,
        port: u16,
        client_session_id: Option<u64>,
    ) -> Option<u64> {
        self.flows
            .session_id_by_target(target, port, client_session_id)
    }

    /// Look up the session ID for an upstream response (requires outbound tag).
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn upstream_response_session_id(
        &self,
        outbound_tag: &str,
        target: &Address,
        port: u16,
    ) -> Option<u64> {
        self.flows
            .upstream_response_session_id(outbound_tag, target, port)
    }
}
