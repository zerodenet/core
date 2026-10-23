//! Active TCP connects through the common Flow -> Packet adapter.

use std::{future::Future, pin::Pin};

use zero_core::Session;
use zero_engine::EngineError;
use zero_stack::ClientTcpStackError;

use crate::{
    protocol_registry::TcpExecutionServices,
    runtime::{
        packet_flow::PacketFlowAdapter, tcp_dispatch::operation::PreparedTcpConnectOperation,
    },
    transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream},
};

pub(super) struct PacketTcpOperation(pub(super) PacketFlowAdapter);

impl PreparedTcpConnectOperation for PacketTcpOperation {
    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let targets = services
                .resolve_packet_targets(session)
                .await
                .map_err(|error| failure("raw_ip_resolve_target", error))?;
            let network = services.upstream();
            let generation = network.egress_generation();
            let (target, binding) = self
                .0
                .select(&targets, generation)
                .map_err(|error| failure("raw_ip_peer", error))?;
            let device = &binding.endpoint;
            if network.egress_generation() != generation {
                return Err(failure(
                    "raw_ip_device",
                    invalid("network changed during connect admission"),
                ));
            }
            let users = device.flow_users();
            let user = match &users {
                Some(users) => Some(
                    users
                        .acquire(
                            super::users::FlowCaller::from_tcp(&services, session)
                                .map_err(|error| failure("raw_ip_user", error))?,
                        )
                        .map_err(|error| {
                            failure("raw_ip_user", std::io::Error::other(error).into())
                        })?,
                ),
                None => None,
            };
            if let Some(peer_id) = &binding.peer_id {
                services
                    .engine()
                    .bind_session_peer(session.id, &self.0.tag, true, peer_id);
            }
            let opening = device.open_tcp_tracked(
                binding.local_ip,
                target,
                services.outbound_io(&self.0.tag, zero_api::TrafficPlane::Inner),
                user.as_ref(),
            );
            let stream = if let Some(users) = &users {
                tokio::select! {
                    biased;
                    result = opening => result.map_err(|error| failure("raw_ip_tcp_connect", stack_error(error)))?,
                    _ = users.cancellation().cancelled() => return Err(failure("raw_ip_user", invalid("packet endpoint retired during connect"))),
                }
            } else {
                opening
                    .await
                    .map_err(|error| failure("raw_ip_tcp_connect", stack_error(error)))?
            };
            if network.egress_generation() != generation || !device.is_current() {
                return Err(failure(
                    "raw_ip_device",
                    invalid("peer device changed during connect"),
                ));
            }
            let stream = match user {
                Some(user) => TcpRelayStream::new(super::users::TrackedStream::new(stream, user)),
                None => TcpRelayStream::new(stream),
            };
            Ok(EstablishedTcpOutbound::relay(
                self.0.tag.as_ref(),
                None,
                Vec::new(),
                stream,
            ))
        })
    }
}

fn stack_error(error: ClientTcpStackError) -> EngineError {
    use ClientTcpStackError as Error;
    let kind = match error {
        Error::MissingLocalAddress
        | Error::InvalidMtu
        | Error::UnknownLocalAddress
        | Error::AddressFamilyMismatch => std::io::ErrorKind::InvalidInput,
        Error::ConnectionLimit => std::io::ErrorKind::WouldBlock,
        Error::OutboundClosed => std::io::ErrorKind::BrokenPipe,
        Error::ConnectTimeout => std::io::ErrorKind::TimedOut,
        Error::ConnectionReset => std::io::ErrorKind::ConnectionReset,
        Error::DestinationUnreachable => std::io::ErrorKind::NetworkUnreachable,
    };
    EngineError::Io(std::io::Error::new(
        kind,
        format!("raw-IP TCP stack: {error:?}"),
    ))
}

fn invalid(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.to_string(),
    ))
}

fn failure(stage: &'static str, error: EngineError) -> TcpOutboundFailure {
    TcpOutboundFailure {
        stage,
        error,
        upstream_endpoint: None,
        network: None,
    }
}
