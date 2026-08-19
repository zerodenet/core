use std::time::Instant;

use zero_engine::EngineError;

use super::UdpDispatch;
use crate::logging::{log_session_failed, session_failure_observation};
use crate::runtime::passive_relay_health::classify_relay_outcome;
use crate::runtime::udp_flow::sessions::UdpFlowKey;
use crate::runtime::udp_flow::snapshot::UdpFlowSnapshot;

impl UdpDispatch {
    pub(super) fn fail_flow(
        &mut self,
        flow: &UdpFlowSnapshot,
        started_at: Instant,
        stage: &'static str,
        error: &EngineError,
    ) {
        self.flow_start_backoff.record_failure(
            UdpFlowKey::new(
                &flow.session.target,
                flow.session.port,
                flow.client_session_id,
            ),
            Instant::now(),
        );
        if let Some(completed) = self.flows.finish_with_failure(
            &flow.session.target,
            flow.session.port,
            flow.client_session_id,
            session_failure_observation(stage, error, None),
        ) {
            self.runtime.record_passive_relay_outcome(
                &flow.passive_relay_selections,
                &flow.session,
                classify_relay_outcome(&completed.record, Some(error)),
            );
            log_session_failed(
                &flow.session,
                Some(&completed.record),
                stage,
                started_at.elapsed(),
                error,
                None,
            );
        } else {
            log_session_failed(
                &flow.session,
                None,
                stage,
                started_at.elapsed(),
                error,
                None,
            );
        }
    }
}
