use crate::contract::{
    context::with_deadline,
    network::CarrierContext,
    service::{NetworkConnection, NetworkFailure, NetworkService, TcpNetworkRequest},
    BoxFuture, DeliveryState, Reason, Stage,
};
use crate::{EgressInterfaceControl, TokioSocket};
use std::{io, time::Duration};
use zero_core::component::NetworkGeneration;

impl NetworkService for EgressInterfaceControl {
    type Stream = TokioSocket;
    fn generation(&self) -> NetworkGeneration {
        NetworkGeneration(EgressInterfaceControl::generation(self))
    }
    fn open_tcp(
        &self,
        request: TcpNetworkRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<NetworkConnection<Self::Stream>, NetworkFailure>> {
        Box::pin(async move {
            let fail = |reason, operation, source| NetworkFailure {
                source,
                operation,
                stage: Stage::Prepare,
                reason,
                delivery: DeliveryState::NotSubmitted,
                local: None,
                interface_bound: false,
            };
            if context.cancel.is_cancelled() {
                return Err(fail(
                    Reason::Cancelled,
                    "cancelled",
                    io::Error::new(io::ErrorKind::Interrupted, "TCP open cancelled"),
                ));
            }
            if request.generation != NetworkService::generation(self) {
                return Err(fail(
                    Reason::StalePlan,
                    "select_egress",
                    io::Error::new(
                        io::ErrorKind::NotConnected,
                        "TCP network generation is stale",
                    ),
                ));
            }
            if context
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            {
                return Err(fail(
                    Reason::Timeout,
                    "connect_timeout",
                    io::Error::new(io::ErrorKind::TimedOut, "TCP caller deadline elapsed"),
                ));
            }
            if request.peer.port() == 0 {
                return Err(fail(
                    Reason::InvalidConfig,
                    "validate_dial_policy",
                    io::Error::new(io::ErrorKind::InvalidInput, "TCP target port is required"),
                ));
            }
            let selection = self
                .select_for_peer_with_policy(request.peer, &request.dial)
                .and_then(|selection| {
                    selection.ensure_connectable()?;
                    Ok(selection)
                })
                .map_err(|source| fail(Reason::Io, "select_egress", source))?;
            // Keep the existing ten-second candidate budget in addition to any
            // caller deadline. Neither a missing caller bound nor retries remove it.
            let end = tokio::time::Instant::now() + Duration::from_secs(10);
            let end = context.deadline.map_or(end, |caller| caller.min(end));
            let opened = tokio::select! { biased;
                _=context.cancel.cancelled()=>return Err(NetworkFailure {stage:Stage::Dial,delivery:DeliveryState::Unknown,..fail(Reason::Cancelled,"cancelled",io::Error::new(io::ErrorKind::Interrupted,"TCP open cancelled"))}),
                result=with_deadline(Some(end),TokioSocket::connect_addr_with_policy_observed(request.peer,&request.dial,&selection))=>result,
            };
            let socket = opened
                .map_err(|source| NetworkFailure {
                    stage: Stage::Dial,
                    delivery: DeliveryState::Unknown,
                    ..fail(
                        Reason::Timeout,
                        "connect_timeout",
                        io::Error::new(io::ErrorKind::TimedOut, source),
                    )
                })?
                .map_err(|source| NetworkFailure {
                    operation: source.stage(),
                    local: source.local_addr(),
                    interface_bound: source.interface_bound(),
                    stage: Stage::Dial,
                    reason: Reason::Io,
                    delivery: DeliveryState::Unknown,
                    source: source.into_inner(),
                })?;
            if context.cancel.is_cancelled() {
                return Err(NetworkFailure {
                    stage: Stage::Dial,
                    delivery: DeliveryState::Unknown,
                    ..fail(
                        Reason::Cancelled,
                        "cancelled",
                        io::Error::new(io::ErrorKind::Interrupted, "TCP open cancelled"),
                    )
                });
            }
            if request.generation != NetworkService::generation(self)
                || selection.generation() != request.generation.0
            {
                return Err(NetworkFailure {
                    stage: Stage::Dial,
                    delivery: DeliveryState::Unknown,
                    ..fail(
                        Reason::StalePlan,
                        "select_egress",
                        io::Error::new(
                            io::ErrorKind::NotConnected,
                            "TCP egress changed during open",
                        ),
                    )
                });
            }
            let local = socket.local_addr().map_err(|source| NetworkFailure {
                stage: Stage::Dial,
                delivery: DeliveryState::Unknown,
                ..fail(Reason::Io, "local_addr", source)
            })?;
            let remote = socket.peer_addr().map_err(|source| NetworkFailure {
                stage: Stage::Dial,
                delivery: DeliveryState::Unknown,
                ..fail(Reason::Io, "peer_addr", source)
            })?;
            let interface_bound = socket.egress_interface().is_some();
            Ok(NetworkConnection {
                io: socket,
                local,
                remote,
                generation: request.generation,
                interface_bound,
            })
        })
    }
}

#[cfg(test)]
mod tests;
