use std::io;

use crate::runtime::tcp_ingress::TcpIngressRuntime;
use crate::transport::{extract_tcp_stream, TcpRouteResult};
use zero_core::Session;
use zero_engine::EngineError;

use crate::runtime::passive_relay_health::classify_outbound_establishment_failure;

/// Execute the unified routing and outbound establishment pipeline.
///
/// Caller MUST call `prepare_session` before this to assign a session ID.
pub(crate) async fn dispatch_tcp(
    runtime: &TcpIngressRuntime,
    session: &mut Session,
) -> Result<TcpRouteResult, EngineError> {
    let trace = runtime.route_trace(session).await;
    let action = trace.decision;
    let (mut resolved, mut passive_relay_selections) =
        runtime.resolve_outbound(&action, session)?;
    let mut retried_health_race = false;
    let outbound = loop {
        match super::dispatch_tcp_outbound(
            runtime.runtime_services(),
            session,
            resolved,
            trace.route_mode,
            super::TcpDispatchIntent::Traffic,
        )
        .await
        {
            Ok(outbound) => break outbound,
            Err(failure)
                if !retried_health_race
                    && !passive_relay_selections.is_empty()
                    && matches!(&failure.error, EngineError::UnhealthyOutbound { .. }) =>
            {
                // The leaf can be quarantined between URLTest selection and
                // admission. Release any passive half-open slot, then resolve
                // once more against the updated shared health state.
                runtime.record_passive_relay_outcome(
                    &passive_relay_selections,
                    session,
                    zero_engine::PassiveRelayOutcome::Neutral,
                );
                retried_health_race = true;
                (resolved, passive_relay_selections) =
                    runtime.resolve_outbound(&action, session)?;
            }
            Err(failure) => {
                let health_outcome = classify_outbound_establishment_failure(
                    &failure.error,
                    failure.network.as_deref(),
                );
                if let Some(network) = failure.network {
                    runtime.record_session_network(session.id, *network);
                }
                runtime.record_passive_relay_outcome(
                    &passive_relay_selections,
                    session,
                    health_outcome,
                );
                return Err(EngineError::Io(io::Error::other(failure.error)));
            }
        }
    };
    let mut result = extract_tcp_stream(outbound)?;
    result.route_action = action;
    result.passive_relay_selections = passive_relay_selections;
    Ok(result)
}
