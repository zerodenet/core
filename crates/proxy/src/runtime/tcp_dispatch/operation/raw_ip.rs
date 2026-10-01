//! Active TCP connects over the same authenticated raw-IP device as UDP.

use std::{future::Future, pin::Pin, sync::Arc};

use zero_core::Session;
use zero_engine::EngineError;
use zero_stack::ClientTcpStackError;

use crate::{
    protocol_registry::TcpExecutionServices,
    runtime::{
        raw_ip::{RawIpDevicePool, RawIpOutboundPlan},
        tcp_dispatch::operation::PreparedTcpConnectOperation,
    },
    transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream},
};

pub(crate) struct RawIpTcpOperation {
    pub(crate) tag: String,
    pub(crate) identity: [u8; 32],
    pub(crate) plan: Arc<dyn RawIpOutboundPlan>,
    pub(crate) pool: Arc<RawIpDevicePool>,
}

impl PreparedTcpConnectOperation for RawIpTcpOperation {
    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let resolution = services
                .resolve_direct_targets(session)
                .await
                .map_err(|error| failure("raw_ip_resolve_target", error))?;
            let mut selected = None;
            let mut route_error = None;
            for target in resolution.candidates {
                match self.plan.peer_for_target(target.ip()) {
                    Ok(peer) => {
                        selected = Some((target, peer));
                        break;
                    }
                    Err(error) => route_error = Some(error),
                }
            }
            let (target, peer) = selected.ok_or_else(|| {
                failure(
                    "raw_ip_peer",
                    route_error.unwrap_or_else(|| invalid("no target IP")),
                )
            })?;
            let upstream = services.upstream();
            let cell = self
                .pool
                .cell_for(
                    &self.tag,
                    peer.peer_index,
                    self.identity,
                    upstream.egress_generation(),
                )
                .map_err(|error| failure("raw_ip_device", error))?;
            let local_ip = peer.local_ip;
            if let Some(peer_id) = self.plan.peer_identity(peer.peer_index) {
                services
                    .engine()
                    .bind_session_peer(session.id, &self.tag, true, &peer_id);
            }
            let device = cell
                .get()
                .ok_or_else(|| failure("raw_ip_device", invalid("peer device not initialized")))?;
            if !self.pool.is_current(
                &self.tag,
                peer.peer_index,
                self.identity,
                upstream.egress_generation(),
                device,
            ) {
                return Err(failure(
                    "raw_ip_device",
                    invalid("peer device changed during connect"),
                ));
            }
            let stream = device
                .open_tcp(local_ip, target)
                .await
                .map_err(|error| failure("raw_ip_tcp_connect", stack_error(error)))?;
            if !self.pool.is_current(
                &self.tag,
                peer.peer_index,
                self.identity,
                upstream.egress_generation(),
                device,
            ) {
                return Err(failure(
                    "raw_ip_device",
                    invalid("peer device changed during connect"),
                ));
            }
            Ok(EstablishedTcpOutbound::relay(
                &self.tag,
                None,
                Vec::new(),
                TcpRelayStream::new(stream),
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
