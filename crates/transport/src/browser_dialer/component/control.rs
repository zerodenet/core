//! The real lower loan stays owned until the transformed upper view returns.
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl},
        BoxFuture, ChannelError, CloseReason,
    },
    controlled_carrier::loan::LoanOwner,
};
#[cfg_attr(
    feature = "split_http",
    expect(
        clippy::large_enum_variant,
        reason = "both variants already use one prepaid BoxChannelControl sized for Logical; a nested LoanOwner box would add an uncharged allocation"
    )
)]
pub(super) enum Logical {
    Loan(LoanOwner),
    #[cfg(feature = "split_http")]
    Driver(zero_platform_tokio::controlled_carrier::CarrierAdoption),
}
impl DynChannelControl for Logical {
    fn cancel(&self, reason: CloseReason) {
        match self {
            Self::Loan(owner) => owner.cancel(reason),
            #[cfg(feature = "split_http")]
            Self::Driver(_) => {}
        }
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        match self {
            Self::Loan(owner) => owner.try_close(),
            #[cfg(feature = "split_http")]
            Self::Driver(actual) => actual.try_retire(),
        }
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        match self {
            Self::Loan(owner) => Box::pin(owner.close(end)),
            #[cfg(feature = "split_http")]
            Self::Driver(actual) => Box::pin(actual.retire(end)),
        }
    }
}
