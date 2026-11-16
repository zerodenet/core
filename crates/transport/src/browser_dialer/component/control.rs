//! The real lower loan stays owned until the transformed upper view returns.
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl},
        BoxFuture, ChannelError, CloseReason,
    },
    controlled_carrier::loan::LoanOwner,
};
pub(super) struct Logical(pub LoanOwner);
impl DynChannelControl for Logical {
    fn cancel(&self, reason: CloseReason) {
        self.0.cancel(reason);
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.0.try_close()
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.0.close(end))
    }
}
