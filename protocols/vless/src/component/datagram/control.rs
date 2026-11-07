//! Return the exact packet views before their actual existing logical owner.
use super::*;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    CloseReason,
};
pub struct Control(pub(super) Arc<Shared>);
impl Control {
    pub(in crate::component) fn original(&self) -> Option<&CarrierOrigin> {
        self.0.original.as_ref()
    }
    fn pending() -> CloseReport {
        CloseReport {
            unfinished: vec![Unfinished::Io(
                "VLESS UDP original packet views or controller remain retained".into(),
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
        let Ok(mut owner) = self.0.owner.try_lock() else {
            return Ok(Self::pending());
        };
        match owner.as_mut() {
            Some(owner) => owner.try_close(),
            None => Ok(CloseReport::default()),
        }
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
        let mut owner = match tokio::time::timeout_at(end, self.0.owner.lock()).await {
            Ok(owner) => owner,
            Err(_) => return Ok(Self::pending()),
        };
        match owner.as_mut() {
            Some(owner) => owner.close(end).await,
            None => Ok(CloseReport::default()),
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.0.stop(CloseReason::Shutdown);
    }
}
