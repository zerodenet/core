//! Logical borrowing is separate from the donor's native retirement.
mod adopt;
mod business;
pub(in crate::controlled_carrier) mod datagram;
mod dependency;
mod host_node;
mod io;
mod logical;
pub use host_node::HostNodeDnsSource;
mod native;
mod native_packet;
mod owner;
pub use logical::LogicalLoanCloseParticipant;
pub(crate) mod packet_driver;
mod receiver;
mod related;
use super::CarrierOrigin;
use crate::contract::{stream::BoxStream, CancellationToken};
pub use datagram::{
    BorrowedDatagram, BorrowedDatagramCandidates, CarrierEndpointClaim, CarrierEndpointUserGraph,
    CarrierEndpointUsersClosed, CarrierEndpointUsersCompletion, DatagramLoan, LoanDatagramIo,
    ServiceDatagramTarget, ServiceDatagramTargetIssuer, ServiceDatagramTargetLoan,
};
pub use dependency::CarrierDependencyLoanIssuer;
pub use io::LoanIo;
pub use owner::{LoanClosed, LoanOwner};
pub use receiver::CarrierLoanReceiver;
pub(super) use related::RelatedControl;
use std::sync::{atomic::AtomicBool, Arc};

struct State {
    returned: AtomicBool,
    changed: tokio::sync::Notify,
    cancel: CancellationToken,
}
pub struct StreamLoan {
    pub io: LoanIo,
    pub owner: LoanOwner,
}
pub type PrepaidStreamLoanResult<S> = Result<
    StreamLoan,
    (
        zero_traits::packet_metadata::PacketMemoryError,
        S,
        crate::contract::lifecycle::BoxChannelControl,
        CarrierOrigin,
        zero_traits::packet_metadata::PacketMetadataCredit,
    ),
>;
impl StreamLoan {
    /// Same logical loan constructor with its concrete erased IO Box paid
    /// BEFORE allocation. This adds no native/controller/Scope authority and
    /// does not claim the legacy common LoanState headers are prepaid here.
    pub fn from_prepaid_io<S: crate::contract::stream::StreamIo + 'static>(
        io: S,
        control: crate::contract::lifecycle::BoxChannelControl,
        original: CarrierOrigin,
        credit: zero_traits::packet_metadata::PacketMetadataCredit,
    ) -> PrepaidStreamLoanResult<S> {
        let needed = zero_traits::packet_metadata::PacketMetadataLayout::boxed::<S>();
        let paid = credit.layout();
        if paid.allocations() < needed.allocations() || paid.bytes() < needed.bytes() {
            return Err((
                zero_traits::packet_metadata::PacketMemoryError::Capacity,
                io,
                control,
                original,
                credit,
            ));
        }
        let mut loan = Self::new(Box::pin(io), control, original);
        loan.io.retain_boxing(credit);
        Ok(loan)
    }
    /// The protocol supplies its real logical-session control, never an FD
    /// completion callback. The sealed original lineage has no public constructor.
    pub fn new(
        io: BoxStream,
        control: crate::contract::lifecycle::BoxChannelControl,
        original: CarrierOrigin,
    ) -> Self {
        let state = Arc::new(State {
            returned: AtomicBool::new(false),
            changed: Default::default(),
            cancel: CancellationToken::new(),
        });
        Self {
            io: LoanIo::new(io, state.clone()),
            owner: LoanOwner::new(control, original, state),
        }
    }
}
