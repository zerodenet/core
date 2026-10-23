use zero_core::Session;

use crate::inventory::{PreparedTcpCandidate, PreparedTcpCandidateExecution};
use crate::protocol_registry::TcpExecutionServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

use super::TcpDispatchIntent;
use crate::runtime::passive_relay_health::classify_outbound_establishment_failure;
use zero_engine::PassiveRelayOutcome;

pub(crate) async fn dispatch_prepared_tcp_candidate(
    services: TcpExecutionServices,
    session: &Session,
    prepared: PreparedTcpCandidate,
    intent: TcpDispatchIntent,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let health_tag = prepared.health_tag.clone();
    let trace = crate::transport::establishment::DialTrace::default();
    let services = services.with_dial_trace(trace.clone());
    let result = match &prepared.execution {
        PreparedTcpCandidateExecution::Native(slot) => {
            crate::component::stream_bridge::connect(&services, session, slot, &prepared).await
        }
        PreparedTcpCandidateExecution::Block { tag } => {
            Ok(EstablishedTcpOutbound::block(tag.clone()))
        }
        PreparedTcpCandidateExecution::Connect(operation) => {
            operation.execute(services.clone(), session).await
        }
    };

    if intent.records_outbound_health() {
        if let Some(tag) = health_tag.as_deref() {
            match &result {
                Ok(_) => services.record_outbound_success(tag),
                Err(failure) => {
                    if classify_outbound_establishment_failure(
                        &failure.error,
                        failure.network.as_deref(),
                    ) == PassiveRelayOutcome::Failure
                    {
                        services.record_outbound_failure(tag);
                    }
                }
            }
        }
    }

    result.map_err(|mut failure| {
        let dial = trace.snapshot().or_else(|| {
            let network = failure.network.as_ref()?;
            let socket = |endpoint: &zero_engine::FlowRemoteEndpoint| {
                endpoint
                    .host
                    .parse()
                    .ok()
                    .map(|ip| std::net::SocketAddr::new(ip, endpoint.port))
            };
            Some(crate::transport::establishment::DialObservation {
                stage: failure.stage,
                remote: matches!(
                    failure.stage,
                    "connect_socket" | "connect_timeout" | "configure_socket"
                )
                .then(|| network.remote_address.as_ref().and_then(socket))
                .flatten(),
                local: network.local_address.as_ref().and_then(socket),
                candidates: network
                    .resolved_candidates
                    .iter()
                    .filter_map(socket)
                    .collect(),
                attempts: network.connection_attempts.clone(),
                os_error: network
                    .connection_attempts
                    .iter()
                    .find(|attempt| {
                        Some(&attempt.remote_address) == network.remote_address.as_ref()
                            && attempt.stage == failure.stage
                    })
                    .and_then(|attempt| attempt.os_error),
            })
        });
        failure.error = crate::transport::establishment::EstablishmentFailure::wrap(
            failure.error,
            prepared.tag,
            prepared.protocol,
            prepared.endpoint,
            failure.stage,
            dial,
        );
        failure
    })
}

#[cfg(test)]
mod tests;
