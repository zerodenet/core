use std::sync::Arc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zero_core::{
    component::{RoleInstanceId, SessionId},
    packet::PacketFacts,
    request::RequestFacts,
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
}

#[derive(Debug, Clone)]
pub enum RequestIdentity {
    Business(Arc<RequestFacts>),
    Packet(Arc<PacketFacts>),
}

impl RequestIdentity {
    pub fn session(&self) -> Option<SessionId> {
        match self {
            Self::Business(facts) => Some(facts.session),
            Self::Packet(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct OpenContext {
    pub identity: RequestIdentity,
    pub deadline: Instant,
    pub cancel: CancellationToken,
    pub upstream: UpstreamTransport,
    pub observer: Option<Arc<dyn ConnectObserver>>,
}

impl OpenContext {
    pub fn business_facts(&self) -> Result<&RequestFacts, super::ChannelError> {
        match &self.identity {
            RequestIdentity::Business(facts) => Ok(facts),
            RequestIdentity::Packet(_) => Err(identity_error(super::Impact::Session)),
        }
    }
    pub fn packet_facts(&self) -> Result<&PacketFacts, super::ChannelError> {
        match &self.identity {
            RequestIdentity::Packet(facts) => Ok(facts),
            RequestIdentity::Business(_) => Err(identity_error(super::Impact::Packet)),
        }
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
