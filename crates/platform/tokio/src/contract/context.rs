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

/// Neutral actual Packet-plane outcomes. Delivery and native-close proofs remain
/// separate; these facts carry no synthetic business SessionId or task ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketIoFact {
    Sent {
        bytes: usize,
    },
    Received {
        bytes: usize,
    },
    Dropped {
        bytes: usize,
        reason: zero_traits::PacketDropReason,
    },
    Error,
    ReceiveCoverageLost,
}

/// Implementations must record bounded facts synchronously, without blocking I/O.
pub trait ConnectObserver: Send + Sync {
    fn record(&self, session: Option<SessionId>, fact: ConnectFact);
    /// Native Packet driver facts for the original actual return association.
    /// Production Packet assembly adapts its existing Inner observer here.
    fn packet_io(&self, _association: &zero_core::packet::ReturnAssociation, _fact: PacketIoFact) {}

    /// Optional locally accepted protocol-framed datagram bytes for compatibility Flow
    /// accounting. This is separate from business payload receipts and physical
    /// carrier traffic, and does not prove peer delivery.
    fn datagram_sent(&self, _session: Option<SessionId>, _encoded_bytes: u64) {}

    /// One cumulative protocol-handshake result for this open operation. This
    /// excludes TLS/carrier framing; explicit zero is distinct from no report.
    fn protocol_traffic(&self, _session: Option<SessionId>, _read_bytes: u64, _written_bytes: u64) {
    }
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
    /// Optional bound for waiting for a new request. None preserves an idle
    /// listener/keep-alive wait; HandshakeContext still has a finite deadline.
    pub deadline: Option<Instant>,
    pub cancel: CancellationToken,
}

#[derive(Clone)]
pub struct HandshakeContext {
    /// Real accepted-task origin. No arbitrary caller or native authority.
    pub accepted: Option<crate::AcceptedCaller>,
    /// One-time facts produced by a configured trusted carrier prelude. The
    /// host validates the unchanged physical origin before applying them to
    /// business/control admission. This grants no native or task authority.
    pub connection: Option<Arc<std::sync::OnceLock<InboundConnectionFacts>>>,
    pub inbound: RoleInstanceId,
    /// Allocate only after recognizing a business request. Control and website
    /// traffic must not allocate a synthetic proxy session.
    pub identities: Arc<dyn SessionIdentitySource>,
    pub deadline: Instant,
    pub cancel: CancellationToken,
}

pub trait SessionIdentitySource: Send + Sync {
    fn allocate(&self) -> Result<SessionId, super::ChannelError>;
    /// The actual host keeps the full authenticated facts next to its sealed
    /// identity ticket, including anonymous per-session rates. No credential
    /// secret or configuration snapshot is passed through this boundary.
    fn allocate_authenticated(
        &self,
        auth: Option<zero_core::SessionAuth>,
    ) -> Result<SessionId, super::ChannelError> {
        if auth.is_some() {
            // Never silently lose anonymous rates or authentication facts when
            // an embedding host has not installed an authenticated allocator.
            return Err(super::ChannelError::new(
                super::Stage::Handshake,
                super::Reason::Unsupported,
                super::Impact::Session,
            ));
        }
        self.allocate()
    }
}

/// Captured from the actual accepted carrier and its trusted prelude, never
/// reconstructed from an outbound target or a later configuration snapshot.
#[derive(Debug, Clone, Default)]
pub struct InboundConnectionFacts {
    pub original_source: Option<std::net::SocketAddr>,
    pub effective_source: Option<std::net::SocketAddr>,
    pub local_endpoint: Option<std::net::SocketAddr>,
    pub sni: Option<String>,
}
impl InboundConnectionFacts {
    pub fn from_stream(stream: &impl crate::ClientStream) -> Self {
        let peer = stream.peer_addr().ok();
        Self {
            original_source: peer,
            effective_source: peer,
            local_endpoint: stream.local_addr().ok(),
            sni: None,
        }
    }
    pub fn request(
        &self,
        context: &HandshakeContext,
        protocol: zero_core::ProtocolType,
        target: Option<zero_core::request::Destination>,
        authentication: Option<zero_core::SessionAuth>,
    ) -> Result<RequestFacts, super::ChannelError> {
        let subject = authentication
            .as_ref()
            .and_then(|auth| auth.principal_key.as_ref())
            .map(|key| zero_core::component::AuthSubject {
                owner: context.inbound.component,
                principal: zero_core::component::PrincipalId(Arc::from(key.as_str())),
            });
        Ok(RequestFacts {
            session: context.identities.allocate_authenticated(authentication)?,
            inbound: context.inbound.role,
            original_source: self.original_source,
            effective_source: self.effective_source,
            local_endpoint: self.local_endpoint,
            original_target: None,
            target,
            target_source: None,
            subject,
            peer: None,
            protocol,
            sni: self.sni.clone(),
            process: None,
        })
    }
}
