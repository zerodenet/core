//! One temporary boundary from existing probe callers to standard Stream roles.
mod network;
use super::{
    instance::{invalid, retained},
    legacy_stream::LegacyStream,
};
use crate::{
    protocol_registry::TcpExecutionServices,
    transport::{EstablishedTcpOutbound, TcpOutboundFailure},
};
use std::sync::Arc;
use tokio::time::Instant;
use zero_core::{
    component::NetworkGeneration,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Session,
};
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    network::{UpstreamBinding, UpstreamTransport},
    stream::ConnectRequest,
    CancellationToken, ChannelError, Reason,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ServiceInvocation {
    pub(crate) deadline: Instant,
    pub(crate) purpose: ServicePurpose,
}

#[derive(Clone)]
pub(crate) struct Invocation {
    pub(crate) identity: RequestIdentity,
    pub(crate) deadline: Option<Instant>,
}
impl From<ServiceInvocation> for Invocation {
    fn from(invocation: ServiceInvocation) -> Self {
        Self {
            identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: invocation.purpose,
            })),
            deadline: Some(invocation.deadline),
        }
    }
}

pub(crate) async fn connect(
    services: &TcpExecutionServices,
    session: &Session,
    tag: &str,
    configured_tag: Option<&str>,
    policy: &zero_traits::DialPolicy,
    invocation: Invocation,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let plan = services
        .components
        .as_ref()
        .map_err(|error| failure("prepare_component", retained(error.clone())))?;
    let slot = plan
        .direct(configured_tag)
        .map_err(|error| failure("prepare_component", error))?;
    let role = slot
        .stream(
            invocation
                .deadline
                .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(5)),
        )
        .await
        .map_err(|error| failure("start_component", error))?;
    let opener = Arc::new(network::DirectOpener::new(
        services.upstream(),
        session.clone(),
    ));
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: policy.clone(),
        network_generation: NetworkGeneration(opener.generation()),
    };
    let context = OpenContext {
        identity: invocation.identity,
        deadline: invocation.deadline,
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(binding, opener.clone()),
    };
    let destination = Destination::new(session.target.clone(), session.port);
    let connected = role.connect(ConnectRequest { destination }, context).await;
    let evidence = opener.take_evidence();
    match connected {
        Ok(connected) => {
            let Some(Ok(evidence)) = evidence else {
                return Err(failure(
                    "connect_component",
                    invalid(Reason::Internal, "missing Direct network observation"),
                ));
            };
            Ok(EstablishedTcpOutbound::direct(
                tag,
                (evidence.remote.ip().to_string(), evidence.remote.port()),
                zero_platform_tokio::TcpRelayStream::from_client(LegacyStream::new(
                    connected.io,
                    evidence.local,
                    evidence.remote,
                )),
                evidence.network,
            ))
        }
        Err(mut error) => {
            error.component = Some(slot.role_id().component);
            if let Some(Err(evidence)) = evidence {
                // Preserve existing V1 probe codes/messages at the remaining
                // legacy facade. Standard channel callers keep native sources.
                return Err(TcpOutboundFailure {
                    stage: evidence.stage,
                    error: evidence.legacy_error.into(),
                    upstream_endpoint: None,
                    network: Some(evidence.network),
                });
            }
            let stage = if error.reason == Reason::Timeout {
                "connect_timeout"
            } else {
                "connect_component"
            };
            Err(TcpOutboundFailure {
                stage,
                error: zero_engine::EngineError::Io(std::io::Error::other(error)),
                upstream_endpoint: None,
                network: None,
            })
        }
    }
}
fn failure(stage: &'static str, error: ChannelError) -> TcpOutboundFailure {
    TcpOutboundFailure {
        stage,
        error: zero_engine::EngineError::Io(std::io::Error::other(error)),
        upstream_endpoint: None,
        network: None,
    }
}
