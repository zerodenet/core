//! A host-authorized Direct binding, scoped to one persistent resource owner.
mod socket;
use super::super::ChannelError;
use crate::{controlled_datagram::ControlledDatagramSocket, EgressSelection};
pub use socket::{BoundDatagramSocket, DatagramBindingGuard};
use std::{fmt, net::SocketAddr, sync::Arc};
use zero_core::component::{ComponentInstanceId, NetworkGeneration};
use zero_traits::DialPolicy;

struct IssuerIdentity {
    owner: ComponentInstanceId,
}
/// Private provider namespace. Requests carry IDs, never a policy or selection.
pub struct DatagramBindingIssuer(Arc<IssuerIdentity>);
#[derive(Clone)]
pub struct DatagramBindingId {
    issuer: Arc<IssuerIdentity>,
    sequence: u64,
}
impl DatagramBindingIssuer {
    pub fn new(owner: ComponentInstanceId) -> Self {
        Self(Arc::new(IssuerIdentity { owner }))
    }
    /// The host provider must allocate sequence numbers with checked arithmetic.
    pub fn issue(&self, sequence: u64) -> DatagramBindingId {
        DatagramBindingId {
            issuer: self.0.clone(),
            sequence,
        }
    }
    pub fn owns(&self, id: &DatagramBindingId) -> bool {
        Arc::ptr_eq(&self.0, &id.issuer)
    }
}
impl DatagramBindingId {
    pub fn owner(&self) -> ComponentInstanceId {
        self.issuer.owner
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}
impl PartialEq for DatagramBindingId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.issuer, &other.issuer) && self.sequence == other.sequence
    }
}
impl Eq for DatagramBindingId {}
impl fmt::Debug for DatagramBindingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatagramBindingId")
            .field("owner", &self.owner())
            .field("sequence", &self.sequence)
            .finish()
    }
}
#[derive(Clone, Debug)]
pub struct BoundDatagramRequest {
    pub binding: DatagramBindingId,
    pub peer: SocketAddr,
    pub expected_network_generation: NetworkGeneration,
}
/// Installed only by the actual host Direct operation, never by the protocol.
/// Implementations retain a lightweight current-policy fence and the actual
/// egress control, not Engine, Config, a resolver or global NetworkService.
pub trait DatagramBindingAuthorization: Send + Sync {
    fn policy(&self) -> &DialPolicy;
    fn preferred_port(&self) -> Option<u16>;
    fn check_current(&self, expected: NetworkGeneration) -> Result<(), ChannelError>;
    fn select(
        &self,
        peer: SocketAddr,
        expected: NetworkGeneration,
    ) -> Result<EgressSelection, ChannelError>;
}
#[derive(Debug)]
pub struct GrantedBoundDatagram {
    pub socket: BoundDatagramSocket,
    pub selection: EgressSelection,
}
impl GrantedBoundDatagram {
    /// Host construction after the original is already in its persistent owner.
    pub fn from_host(
        socket: ControlledDatagramSocket,
        binding: DatagramBindingId,
        selection: EgressSelection,
        authorization: Arc<dyn DatagramBindingAuthorization>,
    ) -> Self {
        Self {
            socket: BoundDatagramSocket::new(socket, binding, selection.clone(), authorization),
            selection,
        }
    }
}
