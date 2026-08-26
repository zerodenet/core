use zero_core::{Address, Session};
use zero_engine::{FlowFailureObservation, PassiveRelaySelection, SessionHandle};

use crate::runtime::udp_flow::outbound::UdpFlowOutbound;
use crate::runtime::udp_flow::rate_limit::UdpFlowRateLimiters;
use crate::runtime::udp_flow::snapshot::UdpFlowSnapshot;

use super::model::{CompletedUdpFlow, UdpFlow, UdpFlowKey, UdpSessionFlows};

impl UdpSessionFlows {
    pub(crate) fn snapshot(
        &self,
        target: &Address,
        port: u16,
        client_session_id: Option<u64>,
    ) -> Option<UdpFlowSnapshot> {
        self.flows
            .get_key_value(&UdpFlowKey::new(target, port, client_session_id))
            .map(|(key, flow)| flow.snapshot(key.clone()))
    }

    /// Look up a session ID by target+port only, regardless of outbound type.
    ///
    /// Used for chain-outbound response metering where the outbound tag
    /// may not be known at the call site.
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn session_id_by_target(
        &self,
        target: &Address,
        port: u16,
        client_session_id: Option<u64>,
    ) -> Option<u64> {
        self.flows
            .get(&UdpFlowKey::new(target, port, client_session_id))
            .map(|flow| flow.session.id)
    }

    pub(crate) fn insert(
        &mut self,
        key: UdpFlowKey,
        session: Session,
        handle: SessionHandle,
        outbound: UdpFlowOutbound,
        passive_relay_selections: Vec<PassiveRelaySelection>,
        rate_limiters: UdpFlowRateLimiters,
    ) {
        self.index_flow(&key, &session.target, &outbound);
        self.flows.insert(
            key,
            UdpFlow {
                session,
                handle,
                outbound,
                passive_relay_selections,
                passive_health_confirmed: std::sync::atomic::AtomicBool::new(false),
                rate_limiters,
            },
        );
    }

    pub(crate) fn rate_limiters_by_session_id(
        &self,
        session_id: Option<u64>,
    ) -> UdpFlowRateLimiters {
        let Some(session_id) = session_id else {
            return UdpFlowRateLimiters::default();
        };
        self.flows
            .values()
            .find(|flow| flow.session.id == session_id)
            .map(|flow| flow.rate_limiters.clone())
            .unwrap_or_default()
    }

    pub(crate) fn confirm_passive_health(
        &self,
        session_id: u64,
    ) -> Option<(Session, Vec<PassiveRelaySelection>)> {
        let flow = self
            .flows
            .values()
            .find(|flow| flow.session.id == session_id)?;
        if flow
            .passive_health_confirmed
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return None;
        }
        Some((flow.session.clone(), flow.passive_relay_selections.clone()))
    }

    pub(crate) fn finish_with_failure(
        &mut self,
        key: &UdpFlowKey,
        failure: FlowFailureObservation,
    ) -> Option<CompletedUdpFlow> {
        let flow = self.flows.remove(key)?;
        self.unindex_flow(key, &flow.session.target, &flow.outbound);
        Some(flow.finish_with_failure(failure))
    }

    pub(crate) fn finish_cancelled(&mut self, session_id: u64) -> Option<CompletedUdpFlow> {
        let key = self
            .flows
            .iter()
            .find_map(|(key, flow)| (flow.session.id == session_id).then(|| key.clone()))?;
        let flow = self.flows.remove(&key)?;
        self.unindex_flow(&key, &flow.session.target, &flow.outbound);
        Some(flow.finish_cancelled())
    }

    pub(crate) fn finish_all(&mut self) -> Vec<CompletedUdpFlow> {
        self.direct_by_sender.clear();
        self.upstream_by_response.clear();

        self.flows
            .drain()
            .map(|(_, flow)| flow.finish_success())
            .collect()
    }
}
