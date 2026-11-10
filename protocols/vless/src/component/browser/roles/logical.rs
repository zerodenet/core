//! The original role LoanOwner stays beside its transformed protocol stream.
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl},
        BoxFuture, ChannelError, CloseReason,
    },
    controlled_carrier::loan::LoanOwner,
};
pub(in crate::component::browser) enum Logical {
    Loan(LoanOwner),
    Driver(zero_platform_tokio::controlled_carrier::CarrierAdoption),
}
impl DynChannelControl for Logical {
    fn cancel(&self, reason: CloseReason) {
        // The real unique logical IO must return before requesting physical
        // retirement, exactly like the existing taskless NativeControl.
        if let Self::Loan(owner) = self {
            owner.cancel(reason)
        }
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        match self {
            Self::Loan(owner) => owner.try_close(),
            Self::Driver(actual) => actual.try_retire(),
        }
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        match self {
            Self::Loan(owner) => Box::pin(owner.close(end)),
            Self::Driver(actual) => Box::pin(actual.retire(end)),
        }
    }
}
