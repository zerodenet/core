//! Return the exact packet views before their actual existing logical owner.
use super::*;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    CloseReason,
};
pub struct Control(pub(super) Arc<Shared>);
impl Control {
    fn pending() -> CloseReport {
        CloseReport {
            unfinished: vec![Unfinished::Io(
                "Trojan UDP original packet views or controller remain retained".into(),
            )],
            ..Default::default()
        }
    }
}
impl ChannelControl for Control {
    fn revoked(&self) -> zero_platform_tokio::contract::BoxFuture<'static, ()> {
        self.0.cancellation()
    }
    fn cancel(&self, reason: CloseReason) {
        self.0.stop(reason);
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        if !self.0.return_stream() {
            return Ok(Self::pending());
        }
        Ok(CloseReport::default())
    }
    async fn wait_closed(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.0.return_stream() {
                break;
            }
            if tokio::time::timeout_at(end, changed).await.is_err() {
                return Ok(Self::pending());
            }
        }
        Ok(CloseReport::default())
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.0.stop(CloseReason::Shutdown);
    }
}
