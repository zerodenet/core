//! One temporary boundary from existing probe callers to standard Stream roles.
pub(super) mod controlled;
pub(super) mod network;
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
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) caller_native:
        Option<crate::runtime::inbound_operation::raw_ip::native_obligations::NativeRegistration>,
    pub(crate) admitted: Option<super::business_demand::AdmittedTcpDemand>,
    pub(crate) caller_accepted: Option<zero_platform_tokio::AcceptedTcpCaller>,
    pub(crate) identity: RequestIdentity,
    pub(crate) deadline: Option<Instant>,
}
impl Invocation {
    pub(crate) fn cancellation(&self) -> CancellationToken {
        let cancel = self
            .admitted
            .as_ref()
            .map_or_else(CancellationToken::new, |demand| demand.cancellation());
        if self
            .caller_accepted
            .as_ref()
            .is_some_and(|caller| caller.check_open().is_err())
        {
            cancel.cancel();
        }
        #[cfg(feature = "raw-ip-runtime")]
        if self
            .caller_native
            .as_ref()
            .is_some_and(|caller| caller.check_open().is_err())
        {
            // Same original request token. A stopped/joined caller cannot
            // acquire a new carrier; this is no native closure confirmation.
            cancel.cancel();
        }
        cancel
    }
}
impl From<ServiceInvocation> for Invocation {
    fn from(invocation: ServiceInvocation) -> Self {
        Self {
            #[cfg(feature = "raw-ip-runtime")]
            caller_native: None,
            admitted: None,
            caller_accepted: None,
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
    tracing::trace!(
        session = session.id,
        "Direct business waiting for component role"
    );
    let role = slot
        .stream(
            invocation
                .deadline
                .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(5)),
        )
        .await
        .map_err(|mut error| {
            error.component.get_or_insert(slot.id);
            error.session = error.session.or(invocation.identity.session());
            failure("start_component", error)
        })?;
    tracing::trace!(
        session = session.id,
        "Direct business acquired component role"
    );
    let opener = Arc::new(
        network::DirectOpener::new(services.upstream(), session.clone()).with_business(
            super::business_carrier::BusinessCarrierRequest::new(
                services.engine(),
                &slot,
                &invocation,
            )
            .map_err(|error| failure("prepare_component", error))?,
        ),
    );
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: policy.clone(),
        network_generation: NetworkGeneration(opener.generation()),
    };
    let cancel = invocation.cancellation();
    let context = OpenContext {
        identity: invocation.identity,
        deadline: invocation.deadline,
        cancel,
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
