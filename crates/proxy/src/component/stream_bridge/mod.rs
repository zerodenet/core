//! The single temporary conversion from native proxy roles to legacy relay I/O.
use super::{
    instance::{invalid, ComponentSlot},
    legacy_stream::LegacyStream,
};
use crate::{
    inventory::PreparedTcpCandidate,
    protocol_registry::TcpExecutionServices,
    transport::{EstablishedTcpOutbound, TcpOutboundFailure},
};
use std::{error::Error, fmt, io, sync::Arc};
use zero_core::{component::NetworkGeneration, request::Destination, Address, Session};
use zero_platform_tokio::contract::{
    context::OpenContext,
    network::{UpstreamBinding, UpstreamTransport},
    stream::ConnectRequest,
    CancellationToken, ChannelError, Reason,
};
mod network;
mod relay;
mod traffic;
pub(crate) use relay::connect as connect_relay;

pub(crate) async fn connect(
    services: &TcpExecutionServices,
    session: &Session,
    slot: &Arc<ComponentSlot>,
    prepared: &PreparedTcpCandidate,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let endpoint = prepared
        .endpoint
        .clone()
        .expect("native node candidate has an endpoint");
    let stage = slot.recipe.legacy_connect_stage();
    let failure = |error| failure(stage, error, &endpoint);
    let invocation = services.invocation.clone().ok_or_else(|| {
        failure(invalid(
            Reason::Internal,
            "native Stream requires explicit request identity",
        ))
    })?;
    let role = slot
        .stream(
            invocation
                .deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
        )
        .await
        .map_err(|mut error| {
            error.component.get_or_insert(slot.id);
            error.session = error.session.or(invocation.identity.session());
            failure(error)
        })?;
    let upstream =
        services.upstream_observed(prepared.tag.as_deref().expect("native node has a tag"));
    let traffic = traffic::EstablishmentTraffic::new(upstream.observer());
    let address = match zero_core::address::parse_ip_literal(&endpoint.0) {
        Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
        Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
        None => Address::Domain(endpoint.0.clone()),
    };
    let opener = Arc::new(network::NodeOpener::new(
        upstream.with_observer(Some(traffic.clone())),
        Destination::new(address, endpoint.1),
    ));
    let context = OpenContext {
        identity: invocation.identity,
        deadline: invocation.deadline,
        cancel: CancellationToken::new(),
        observer: None,
        upstream: UpstreamTransport::new(
            UpstreamBinding {
                prefix: vec![],
                dial: slot.dial.clone(),
                network_generation: NetworkGeneration(opener.generation()),
            },
            opener.clone(),
        ),
    };
    let connected = role
        .connect(
            ConnectRequest {
                destination: Destination::new(session.target.clone(), session.port),
            },
            context,
        )
        .await
        .map_err(failure)?;
    let evidence = opener.take_evidence().ok_or_else(|| {
        failure(invalid(
            Reason::Internal,
            "native Stream did not retain its selected carrier facts",
        ))
    })?;
    services.record_control_traffic(session.id, traffic.finish());
    Ok(EstablishedTcpOutbound::proxied(
        prepared.tag.clone().expect("native node has a tag"),
        endpoint.0,
        endpoint.1,
        zero_platform_tokio::TcpRelayStream::from_client(LegacyStream::new(
            connected.io,
            evidence.local,
            evidence.remote,
        )),
    ))
}

#[derive(Debug)]
struct LegacyError(ChannelError);
impl fmt::Display for LegacyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.source() {
            Some(source) => fmt::Display::fmt(source, f),
            None => self.0.fmt(f),
        }
    }
}
impl Error for LegacyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
fn failure(
    stage: &'static str,
    error: ChannelError,
    endpoint: &(String, u16),
) -> TcpOutboundFailure {
    let kind = match error.reason {
        Reason::Timeout => io::ErrorKind::TimedOut,
        Reason::Cancelled | Reason::Revoked | Reason::StalePlan => io::ErrorKind::Interrupted,
        Reason::InvalidConfig => io::ErrorKind::InvalidInput,
        Reason::Capacity => io::ErrorKind::WouldBlock,
        _ => io::ErrorKind::Other,
    };
    TcpOutboundFailure {
        stage,
        error: zero_engine::EngineError::Io(io::Error::new(kind, LegacyError(error))),
        upstream_endpoint: Some(Box::new(endpoint.clone())),
        network: None,
    }
}
