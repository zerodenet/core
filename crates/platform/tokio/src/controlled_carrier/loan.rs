//! Logical borrowing is separate from the donor's native retirement.
mod adopt;
mod business;
mod datagram;
mod dependency;
mod io;
mod native;
mod native_packet;
mod owner;
mod receiver;
mod related;
use super::CarrierOrigin;
use crate::contract::{lifecycle::DynChannelControl, stream::BoxStream, CancellationToken};
pub use dependency::CarrierDependencyLoanIssuer;
pub use datagram::{BorrowedDatagram, DatagramLoan, LoanDatagramIo};
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
impl StreamLoan {
    /// The protocol supplies its real logical-session control, never an FD
    /// completion callback. The sealed original lineage has no public constructor.
    pub fn new(
        io: BoxStream,
        control: Box<dyn DynChannelControl>,
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
