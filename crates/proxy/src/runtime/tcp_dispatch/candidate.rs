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
    let trace = crate::transport::establishment::DialTrace::default();
    let services = services.with_dial_trace(trace.clone());
    #[cfg(feature = "raw-ip-runtime")]
    if let Some(caller) = services
        .invocation
        .as_ref()
        .and_then(|invocation| invocation.caller_native.as_ref())
    {
        let covered = match &prepared.execution {
            PreparedTcpCandidateExecution::Native(selected) => {
                // TUN sniffing retains a domain for routing/accounting while
                // Direct still opens the original literal IP. Qualify that
                // actual gateway, not the descriptive sniffed target. A real
                // domain Direct lookup remains a separate DNS obligation.
                // The selected Direct provider also installs each actual
                // domain lookup/refresh in this Source's finite DNS ledger.
                selected.slot().is_ok_and(|slot| {
                    use zero_platform_tokio::contract::component::ResourceDeclaration;
                    slot.recipe
                        .spec()
                        .resources
                        .iter()
                        .any(|declaration| match declaration {
                            ResourceDeclaration::ControlledDirectStream { .. }
                            | ResourceDeclaration::ControlledDirectCarrier { .. } => true,
                            // This actual standard Node grant binds the same raw
                            // Business caller and installs its original adoption.
                            // Domain Node preparation still needs its DNS owner.
                            ResourceDeclaration::ControlledNodeCarrier { endpoint, .. }
                            | ResourceDeclaration::ControlledNodeStream { endpoint, .. }
                            | ResourceDeclaration::ControlledAssociationPool { endpoint, .. } => {
                                matches!(
                                    endpoint.address,
                                    zero_core::Address::Ipv4(_) | zero_core::Address::Ipv6(_)
                                )
                            }
                            _ => false,
                        })
                })
            }
            PreparedTcpCandidateExecution::Block { .. } => !matches!(
                session.effective_direct_target(),
                zero_core::Address::Domain(_)
            ),
        };
        caller
            .declare_gateway(covered)
            .map_err(|source| TcpOutboundFailure {
                stage: "native_gateway_admission",
                error: std::io::Error::other(source).into(),
                upstream_endpoint: None,
                network: None,
            })?;
    }
    let result = match &prepared.execution {
        PreparedTcpCandidateExecution::Native(selected) => match selected.slot() {
            Ok(slot) => {
                crate::component::stream_bridge::connect(&services, session, &slot, &prepared).await
            }
            Err(source) => Err(TcpOutboundFailure {
                stage: "outbound_leaf_runtime",
                error: std::io::Error::other(source).into(),
                upstream_endpoint: prepared.endpoint.clone().map(Box::new),
                network: None,
            }),
        },
        PreparedTcpCandidateExecution::Block { tag } => {
            Ok(EstablishedTcpOutbound::block(tag.clone()))
        }
    };

    finish_candidate_attempt(
        &services,
        TcpAttemptFacts {
            health_tag: prepared.health_tag,
            tag: prepared.tag,
            protocol: prepared.protocol,
            endpoint: prepared.endpoint,
        },
        intent,
        &trace,
        result,
    )
}

// Completion facts only, after the actual native role or kernel Block executes.
// This carries no role, provider, native grant or alternate connect operation.
struct TcpAttemptFacts {
    health_tag: Option<String>,
    tag: Option<String>,
    protocol: String,
    endpoint: Option<(String, u16)>,
}
fn finish_candidate_attempt(
    services: &TcpExecutionServices,
    facts: TcpAttemptFacts,
    intent: TcpDispatchIntent,
    trace: &crate::transport::establishment::DialTrace,
    result: Result<EstablishedTcpOutbound, TcpOutboundFailure>,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let health_tag = facts.health_tag.as_deref();
    if intent.records_outbound_health() {
        if let Some(tag) = health_tag {
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
            facts.tag,
            facts.protocol,
            facts.endpoint,
            failure.stage,
            dial,
        );
        failure
    })
}

#[cfg(test)]
mod tests;
