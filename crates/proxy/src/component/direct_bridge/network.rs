//! Host-owned Direct network implementation and bounded legacy observation handoff.
use super::super::instance::invalid;
use crate::protocol_registry::UpstreamConnectServices;
use std::{net::SocketAddr, sync::Mutex};
use zero_core::{request::Destination, Session};
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    stream::BoxStream,
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(super) struct Evidence {
    pub(super) remote: SocketAddr,
    pub(super) local: Option<SocketAddr>,
    pub(super) network: zero_engine::FlowNetworkObservation,
}
pub(super) struct FailureEvidence {
    pub(super) stage: &'static str,
    pub(super) network: Box<zero_engine::FlowNetworkObservation>,
    pub(super) legacy_error: zero_core::Error,
}
pub(super) struct DirectOpener {
    services: UpstreamConnectServices,
    session: Session,
    evidence: Mutex<Option<Result<Evidence, FailureEvidence>>>,
}
impl DirectOpener {
    pub(super) fn new(services: UpstreamConnectServices, session: Session) -> Self {
        Self {
            services,
            session,
            evidence: Mutex::new(None),
        }
    }
    pub(super) fn generation(&self) -> u64 {
        self.services.network_generation()
    }
    pub(super) fn take_evidence(&self) -> Option<Result<Evidence, FailureEvidence>> {
        self.evidence
            .lock()
            .expect("Direct observation poisoned")
            .take()
    }
}
impl UpstreamOpener for DirectOpener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if !binding.prefix.is_empty() || binding.network_generation.0 != self.generation() {
                return Err(invalid(
                    Reason::StalePlan,
                    "root Direct opener cannot execute a prefix or stale network generation",
                ));
            }
            if destination.address != self.session.target || destination.port != self.session.port {
                return Err(invalid(
                    Reason::StalePlan,
                    "Direct destination differs from the prepared network request",
                ));
            }
            match self
                .services
                .connect_direct(&self.session, &binding.dial)
                .await
            {
                Ok(connection) => {
                    if binding.network_generation.0 != self.generation() {
                        // No business bytes have been submitted. Drop this stale socket.
                        return Err(invalid(
                            Reason::StalePlan,
                            "network changed during Direct establishment",
                        ));
                    }
                    let local = connection.socket.local_addr().ok();
                    *self.evidence.lock().expect("Direct observation poisoned") =
                        Some(Ok(Evidence {
                            remote: connection.remote,
                            local,
                            network: connection.network,
                        }));
                    Ok(Box::pin(connection.socket) as BoxStream)
                }
                Err(failure) => {
                    *self.evidence.lock().expect("Direct observation poisoned") =
                        Some(Err(FailureEvidence {
                            stage: failure.stage,
                            network: failure.network,
                            legacy_error: failure.error.clone(),
                        }));
                    let reason = if failure
                        .source
                        .as_ref()
                        .is_some_and(|source| source.kind() == std::io::ErrorKind::TimedOut)
                    {
                        Reason::Timeout
                    } else {
                        Reason::Io
                    };
                    let error = ChannelError::new(Stage::Dial, reason, Impact::Session);
                    let mut error = match failure.source {
                        Some(source) => error.with_source(source),
                        None => error.with_source(failure.error),
                    };
                    error.delivery = DeliveryState::NotSubmitted;
                    Err(error)
                }
            }
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Internal,
                "Direct Stream grant has no datagram role",
            ))
        })
    }
}

#[cfg(test)]
#[path = "tests/network.rs"]
mod tests;
