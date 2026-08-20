use std::io;

use crate::runtime::tcp_ingress::TcpIngressRuntime;
use crate::transport::{extract_tcp_stream, TcpRouteResult};
use zero_core::Session;
use zero_engine::EngineError;

/// Execute the unified routing and outbound establishment pipeline.
///
/// Caller MUST call `prepare_session` before this to assign a session ID.
pub(crate) async fn dispatch_tcp(
    runtime: &TcpIngressRuntime,
    session: &mut Session,
) -> Result<TcpRouteResult, EngineError> {
    let action = runtime.route_decision(session).await;
    let (resolved, passive_relay_selections) = runtime.resolve_outbound(&action, session)?;
    let outbound =
        match super::dispatch_tcp_outbound(runtime.runtime_services(), session, resolved).await {
            Ok(outbound) => outbound,
            Err(failure) => {
                if let Some(network) = failure.network {
                    runtime.record_session_network(session.id, *network);
                }
                runtime.record_passive_relay_outcome(
                    &passive_relay_selections,
                    session,
                    zero_engine::PassiveRelayOutcome::Failure,
                );
                return Err(EngineError::Io(io::Error::other(failure.error)));
            }
        };
    let mut result = extract_tcp_stream(outbound)?;
    result.route_action = action;
    result.passive_relay_selections = passive_relay_selections;
    Ok(result)
}
