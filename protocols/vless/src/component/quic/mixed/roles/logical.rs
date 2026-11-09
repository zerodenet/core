//! Same accepted XMUX pool; the original H3 driver owns every HTTP work item.
use super::*;
use zero_platform_tokio::contract::lifecycle::{ChannelControl, DynChannelControl};
use zero_transport::split_http::OwnedLogicalReturn;
pub(super) struct Logical(pub(super) OwnedLogicalReturn);
impl DynChannelControl for Logical {
    fn cancel(&self, _: zero_platform_tokio::contract::CloseReason) {
        self.0.request_return();
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError>,
    > {
        Box::pin(ChannelControl::wait_closed(self, end))
    }
}
impl ChannelControl for Logical {
    fn cancel(&self, _: zero_platform_tokio::contract::CloseReason) {
        self.0.request_return();
    }
    async fn wait_closed(
        &mut self,
        _: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        self.0.request_return();
        self.0.wait_returned().await;
        Ok(Default::default())
    }
}
