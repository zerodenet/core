use zero_core::Session;
use zero_engine::{
    EngineError, PassiveRelayOutcome, PassiveRelaySelection, ResolvedOutbound, RouteDecision,
};

use super::model::UdpIngressRuntime;
use crate::logging::log_session_accepted;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch};

impl UdpIngressRuntime {
    pub(crate) async fn route_decision(&self, session: &Session) -> RouteDecision {
        let trace =
            crate::runtime::route_runtime::route_trace_for_session(&self.tcp_services, session)
                .await;
        self.tcp_services
            .engine()
            .record_session_route(session.id, &trace);
        trace.decision
    }

    pub(crate) fn resolve_outbound(
        &self,
        action: &RouteDecision,
        session: &Session,
    ) -> Result<(ResolvedOutbound<'static>, Vec<PassiveRelaySelection>), EngineError> {
        self.tcp_services
            .engine()
            .resolve_route_decision_for_flow_in_snapshot(
                self.tcp_services.snapshot(),
                action.clone(),
                &session.target,
                session.port,
            )
            .map(|(resolved, _, selections)| (resolved, selections))
    }

    pub(crate) fn log_session_accepted(&self, session: &Session, action: &RouteDecision) {
        log_session_accepted(session, action, self.tcp_services.config().mode.kind());
    }

    pub(crate) fn set_session_outbound(&self, session: &Session, remote: Option<&(String, u16)>) {
        self.tcp_services.engine().set_session_outbound_with_path(
            session,
            remote.map(|(host, port)| (host.as_str(), *port)),
            Vec::new(),
        );
    }

    pub(crate) fn record_passive_relay_outcome(
        &self,
        selections: &[PassiveRelaySelection],
        session: &Session,
        outcome: PassiveRelayOutcome,
    ) {
        self.record_passive_relay_target_outcome(
            selections,
            &session.target,
            session.port,
            outcome,
        );
    }

    pub(crate) fn record_passive_relay_target_outcome(
        &self,
        selections: &[PassiveRelaySelection],
        target: &zero_core::Address,
        port: u16,
        outcome: PassiveRelayOutcome,
    ) {
        for selection in selections {
            self.tcp_services
                .engine()
                .record_passive_relay_outcome(selection, target, port, outcome);
        }
    }

    pub(crate) async fn start_udp_resolved_outbound(
        &self,
        dispatch: &mut UdpDispatch,
        session: &Session,
        resolved: ResolvedOutbound<'_>,
        payload: &[u8],
    ) -> Result<FlowStartResult, FlowFailure> {
        crate::runtime::udp_dispatch::start_udp_resolved_outbound(
            dispatch,
            UdpAdapterContext::new(self.tcp_services.config(), self.runtime_services()),
            session,
            resolved,
            payload,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use zero_config::RuntimeConfig;
    use zero_core::{Address, Network, ProtocolType, Session};
    use zero_engine::{PassiveRelayOutcome, RouteDecision};

    use super::*;

    fn runtime() -> UdpIngressRuntime {
        let config = RuntimeConfig::parse(
            r#"{
                "outbounds": [
                    {"tag":"primary","protocol":{"type":"socks5","server":"primary.test","port":1080}},
                    {"tag":"alternate","protocol":{"type":"socks5","server":"alternate.test","port":1080}}
                ],
                "outbound_groups": [{"tag":"auto","type":"url_test","outbounds":["primary","alternate"],"url":"http://probe.test/","interval_seconds":60}],
                "mode":{"type":"global","outbound":"auto"},
                "route":{"rules":[],"final":{"type":"route","outbound":"auto"}}
            }"#,
        )
        .expect("udp passive health config");
        let proxy = crate::runtime::Proxy::new(config).expect("udp passive health proxy");
        UdpIngressRuntime::new(proxy.tcp_runtime_services())
    }

    fn selected_member(
        runtime: &UdpIngressRuntime,
        session: &Session,
    ) -> (String, Vec<PassiveRelaySelection>) {
        let (_resolved, selections) = runtime
            .resolve_outbound(&RouteDecision::Route("auto".to_owned()), session)
            .expect("resolve UDP outbound");
        let member = selections
            .first()
            .expect("url-test selection")
            .member_tag
            .clone();
        (member, selections)
    }

    #[test]
    fn udp_resolution_quarantines_only_the_failing_target_port() {
        let runtime = runtime();
        let affected = Session::new(
            1,
            Address::Domain("landing.test".to_owned()),
            14788,
            Network::Udp,
            ProtocolType::UNKNOWN,
        );
        let unaffected = Session::new(
            2,
            affected.target.clone(),
            14688,
            Network::Udp,
            ProtocolType::UNKNOWN,
        );
        let (member, selections) = selected_member(&runtime, &affected);
        assert_eq!(member, "primary");

        for _ in 0..3 {
            runtime.record_passive_relay_outcome(
                &selections,
                &affected,
                PassiveRelayOutcome::Failure,
            );
        }

        assert_eq!(selected_member(&runtime, &affected).0, "alternate");
        assert_eq!(selected_member(&runtime, &unaffected).0, "primary");
    }
}
