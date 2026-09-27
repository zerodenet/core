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
    let attempt = if intent.checks_outbound_health() {
        health_tag
            .as_deref()
            .map(|tag| services.begin_outbound_attempt(tag))
            .transpose()
            .map_err(|error| TcpOutboundFailure {
                stage: "health_check",
                error,
                upstream_endpoint: None,
                network: None,
            })?
    } else {
        None
    };

    let result = match prepared.execution {
        PreparedTcpCandidateExecution::Block { tag } => Ok(EstablishedTcpOutbound::block(tag)),
        PreparedTcpCandidateExecution::Connect(operation) => {
            operation.execute(services.clone(), session).await
        }
    };

    if intent.records_outbound_health() {
        if let Some(attempt) = attempt {
            match &result {
                Ok(_) => attempt.succeeded(),
                Err(failure)
                    if classify_outbound_establishment_failure(
                        &failure.error,
                        failure.network.as_deref(),
                    ) == PassiveRelayOutcome::Failure =>
                {
                    attempt.failed();
                }
                Err(_) => attempt.neutral(),
            }
        }
    }

    result
}

#[cfg(test)]
mod tests;
