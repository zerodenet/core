use std::{future::Future, sync::Arc};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zero_core::{
    component::{RoleInstanceId, SessionId},
    packet::PacketFacts,
    request::{RequestFacts, ServiceRequestFacts},
};

use super::network::UpstreamTransport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectFact {
    CarrierReady,
    RequestSubmitted,
    PeerConfirmed,
    PeerRejected,
}

/// Implementations must record bounded facts synchronously, without blocking I/O.
pub trait ConnectObserver: Send + Sync {
    fn record(&self, session: Option<SessionId>, fact: ConnectFact);
    /// One cumulative protocol-handshake result for this open operation. This
    /// excludes TLS/carrier framing; explicit zero is distinct from no report.
    fn protocol_traffic(&self, _: Option<SessionId>, _: u64, _: u64) {}
}

#[derive(Debug, Clone)]
pub enum RequestIdentity {
    Business(Arc<RequestFacts>),
    Packet(Arc<PacketFacts>),
    Service(Arc<ServiceRequestFacts>),
}

impl RequestIdentity {
    pub fn session(&self) -> Option<SessionId> {
        match self {
            Self::Business(facts) => Some(facts.session),
            Self::Packet(_) | Self::Service(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct OpenContext {
    pub identity: RequestIdentity,
    /// A caller's absolute bound, when it has one. None preserves the existing
    /// per-operation timers; it never disables the opener's own DNS/dial bounds.
    pub deadline: Option<Instant>,
    pub cancel: CancellationToken,
    pub upstream: UpstreamTransport,
    pub observer: Option<Arc<dyn ConnectObserver>>,
}

impl OpenContext {
    pub fn deadline_elapsed(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }
    /// Stream/Datagram service requests carry no synthetic business SessionId.
    pub fn session_identity(&self) -> Result<Option<SessionId>, super::ChannelError> {
        match &self.identity {
            RequestIdentity::Business(facts) => Ok(Some(facts.session)),
            RequestIdentity::Service(_) => Ok(None),
            RequestIdentity::Packet(_) => Err(identity_error(super::Impact::Session)),
        }
    }
    pub fn business_facts(&self) -> Result<&RequestFacts, super::ChannelError> {
        match &self.identity {
            RequestIdentity::Business(facts) => Ok(facts),
            RequestIdentity::Packet(_) | RequestIdentity::Service(_) => {
                Err(identity_error(super::Impact::Session))
            }
        }
    }
    pub fn packet_facts(&self) -> Result<&PacketFacts, super::ChannelError> {
        match &self.identity {
            RequestIdentity::Packet(facts) => Ok(facts),
            RequestIdentity::Business(_) | RequestIdentity::Service(_) => {
                Err(identity_error(super::Impact::Packet))
            }
        }
    }
}

pub async fn with_deadline<T>(
    deadline: Option<Instant>,
    future: impl Future<Output = T>,
) -> Result<T, tokio::time::error::Elapsed> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, future).await,
        None => Ok(future.await),
    }
}
fn identity_error(impact: super::Impact) -> super::ChannelError {
    let mut error =
        super::ChannelError::new(super::Stage::Prepare, super::Reason::Internal, impact);
    error.delivery = super::DeliveryState::NotSubmitted;
    error
}

#[derive(Clone)]
pub struct AcceptContext {
    pub deadline: Instant,
    pub cancel: CancellationToken,
}

#[derive(Clone)]
pub struct HandshakeContext {
    pub inbound: RoleInstanceId,
    /// Allocate only after recognizing a business request. Control and website
    /// traffic must not allocate a synthetic proxy session.
    pub identities: Arc<dyn SessionIdentitySource>,
    pub deadline: Instant,
    pub cancel: CancellationToken,
}

pub trait SessionIdentitySource: Send + Sync {
    fn allocate(&self) -> Result<SessionId, super::ChannelError>;
}
