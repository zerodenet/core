use zero_core::Session;

use crate::inventory::{PreparedTcpCandidate, PreparedTcpCandidateExecution};
use crate::protocol_registry::TcpRuntimeServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

use super::TcpDispatchIntent;

pub(crate) async fn dispatch_prepared_tcp_candidate(
    services: TcpRuntimeServices,
    session: &Session,
    prepared: PreparedTcpCandidate<'_>,
    intent: TcpDispatchIntent,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let health_tag = prepared.health_tag.clone();
    if intent.checks_outbound_health() {
        if let Some(tag) = health_tag.as_deref() {
            if let Err(error) = services.check_outbound_health(tag) {
                return Err(TcpOutboundFailure {
                    stage: "health_check",
                    error,
                    upstream_endpoint: None,
                    network: None,
                });
            }
        }
    }

    let result = match prepared.execution {
        PreparedTcpCandidateExecution::Block { tag } => Ok(EstablishedTcpOutbound::block(tag)),
        PreparedTcpCandidateExecution::Connect(operation) => {
            operation.execute(services.clone(), session).await
        }
    };

    if intent.records_outbound_health() {
        if let Some(tag) = health_tag.as_deref() {
            match &result {
                Ok(_) => services.record_outbound_success(tag),
                Err(_) => services.record_outbound_failure(tag),
            }
        }
    }

    result
}

#[cfg(test)]
mod tests;
