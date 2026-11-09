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
    ChannelError, Reason,
};
#[cfg(any(
    feature = "upstream-association-runtime",
    feature = "managed-stream-runtime"
))]
pub(crate) mod datagram;
pub(in crate::component) mod network;
mod packet_network;
mod pending;
pub(crate) use pending::actual_protocol_caller;
#[cfg(test)]
pub(crate) mod capacity_debug;
pub(crate) mod relay;
mod traffic;
pub(crate) use relay::connect as connect_relay;

pub(crate) async fn connect(
    services: &TcpExecutionServices,
    session: &Session,
    slot: &Arc<ComponentSlot>,
    prepared: &PreparedTcpCandidate,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    if prepared.endpoint.is_none() {
        return packet_network::connect(services, session, slot, prepared).await;
    }
    let endpoint = prepared
        .endpoint
        .clone()
        .expect("native node candidate has an endpoint");
    let stage = slot.legacy_connect_stage();
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
    let business =
        super::business_carrier::BusinessCarrierRequest::new(services.engine(), slot, &invocation)
            .map_err(failure)?;
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: slot.dial.clone(),
        network_generation: NetworkGeneration(upstream.network_generation()),
    };
    let waiting =
        pending::PendingCaller::new(services, &invocation, vec![slot.role_id()], binding.clone())
            .map_err(failure)?;
    let opener = Arc::new(
        network::NodeOpener::new(
            upstream.with_observer(Some(traffic.clone())),
            Destination::new(address, endpoint.1),
        )
        .with_plan(services.components.as_ref().ok().map(Arc::downgrade))
        .with_business(business)
        .with_pending(
            binding.clone(),
            waiting.as_ref().map(pending::PendingCaller::observer),
        )
        .with_service_call(zero_platform_tokio::contract::network::CarrierContext {
            session: invocation.identity.session(),
            deadline: invocation.deadline,
            cancel: invocation.cancellation(),
        }),
    );
    let cancel = invocation.cancellation();
    let context = OpenContext {
        identity: invocation.identity,
        deadline: invocation.deadline,
        cancel,
        observer: Some(traffic.clone()),
        upstream: UpstreamTransport::new(binding, opener.clone()),
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
pub(crate) fn failure(
    stage: &'static str,
    error: ChannelError,
    endpoint: &(String, u16),
) -> TcpOutboundFailure {
    failure_optional(stage, error, Some(endpoint))
}
pub(super) fn failure_optional(
    stage: &'static str,
    error: ChannelError,
    endpoint: Option<&(String, u16)>,
) -> TcpOutboundFailure {
    #[cfg(test)]
    {
        if error.reason == Reason::Capacity {
            capacity_debug::source("actual native Stream before legacy projection", &error);
        }
        // Read only the retained typed facts before legacy display projection.
        // These facts grant no IO, admission, retry or cleanup authority.
        let mut pending: Vec<&(dyn std::error::Error + 'static)> = vec![&error];
        let mut visited = Vec::new();
        for depth in 0..64 {
            let Some(source) = pending.pop() else { break };
            if visited.iter().any(|actual| std::ptr::eq(*actual, source)) {
                continue;
            }
            visited.push(source);
            if let Some(channel) = source.downcast_ref::<ChannelError>() {
                eprintln!("actual native Stream failure stage={stage} depth={depth} component={:?} session={:?} operation={:?} reason={:?} impact={:?} delivery={:?}",channel.component,channel.session,channel.stage,channel.reason,channel.impact,channel.delivery);
            }
            // Transparent enum/Arc Error implementations delegate source()
            // through their contained error, so visit the actual payload too.
            if let Some(retained) = source.downcast_ref::<std::sync::Arc<ChannelError>>() {
                pending.push(retained.as_ref());
            }
            if let Some(runtime) = source.downcast_ref::<zero_transport::RuntimeError>() {
                match runtime {
                    zero_transport::RuntimeError::Io(error) => pending.push(error),
                    zero_transport::RuntimeError::Core(error) => pending.push(error),
                }
            }
            if let Some(core) = source.downcast_ref::<zero_core::Error>() {
                // Every Core error payload is a protocol-owned &'static str.
                eprintln!("actual native Stream static core source depth={depth} error={core:?}");
            }
            if let Some(native) = source.downcast_ref::<io::Error>() {
                // Only these known static lifecycle diagnostics are emitted;
                // no arbitrary profile, credential or payload is formatted.
                let message = native.to_string();
                if matches!(
                    message.as_str(),
                    "component was stopped"
                        | "component task registration is closed"
                        | "actual VMess MUX recipe no longer retained by its owner"
                        | "actual VMess MUX role owner no longer retained"
                        | "actual VMess MUX role was revoked before this open"
                        | "native pool generation differs from the actual dependency"
                        | "native carrier pool is unavailable"
                        | "native carrier pool is retired"
                        | "native carrier pool generation is unavailable"
                        | "native pool view is unscoped"
                ) {
                    eprintln!("actual native Stream lifecycle source depth={depth} kind={:?} message={message}",native.kind());
                }
                if let Some(inner) = native.get_ref() {
                    pending.push(inner)
                }
            }
            if let Some(next) = source.source() {
                pending.push(next)
            }
        }
    }
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
        upstream_endpoint: endpoint.map(|endpoint| Box::new(endpoint.clone())),
        network: None,
    }
}
