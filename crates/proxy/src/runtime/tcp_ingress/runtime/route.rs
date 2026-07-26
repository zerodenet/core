use zero_core::Session;
use zero_engine::{EngineError, PassiveRelaySelection, ResolvedOutbound, RouteDecision};

use crate::logging::log_session_accepted;

use super::model::TcpIngressRuntime;

impl TcpIngressRuntime {
    pub(crate) fn select_http_redirect(&self, session: &Session) -> Option<(u16, String)> {
        crate::runtime::http_redirect::select_redirect_target(
            &self.services.config().route.url_rewrite,
            session,
        )
    }

    pub(crate) async fn route_decision(&self, session: &Session) -> RouteDecision {
        let trace =
            crate::runtime::route_runtime::route_trace_for_session(&self.services, session).await;
        self.services
            .engine()
            .record_session_route(session.id, &trace);
        trace.decision
    }

    pub(crate) fn resolve_outbound(
        &self,
        action: &RouteDecision,
        session: &Session,
    ) -> Result<(ResolvedOutbound<'static>, Vec<PassiveRelaySelection>), EngineError> {
        self.services
            .engine()
            .resolve_route_decision_for_flow(action.clone(), &session.target, session.port)
            .map(|(resolved, _, selections)| (resolved, selections))
    }

    pub(crate) fn log_session_accepted(&self, session: &Session, action: &RouteDecision) {
        log_session_accepted(session, action, self.services.config().mode.kind());
    }

    pub(crate) fn set_session_outbound(
        &self,
        session: &Session,
        remote: Option<&(String, u16)>,
        relay_chain: Vec<(String, String)>,
    ) {
        self.services.engine().set_session_outbound_with_path(
            session,
            remote.map(|(host, port)| (host.as_str(), *port)),
            relay_chain,
        );
    }

    pub(crate) fn record_passive_relay_outcome(
        &self,
        selections: &[PassiveRelaySelection],
        session: &Session,
        outcome: zero_engine::PassiveRelayOutcome,
    ) {
        for selection in selections {
            self.services.engine().record_passive_relay_outcome(
                selection,
                &session.target,
                session.port,
                outcome,
            );
        }
    }
}
