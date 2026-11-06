//! Same original pending send and logical close remain cancellation-safe.
use super::*;
use std::future::poll_fn;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport},
    CloseReason,
};
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.stop();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.stop();
        let closed = self
            .0
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
            .clone()
            .expect("same logical socket explicitly closed");
        tokio::time::timeout_at(deadline, async {
            // Drive the original pending send to its actual accepted-prefix
            // result; cancellation of this wait leaves it in the same owner.
            let _ = poll_fn(|cx| self.0.poll_pending(cx)).await;
            closed.wait_io_quiescent().await;
        })
        .await
        .map_err(|source| failure(Stage::Close, Reason::Timeout).with_source(source))?;
        self.0.user.lock().unwrap().take();
        if let Some(source) = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .first
            .clone()
        {
            return Err(crate::component::instance::retained(source));
        }
        // Logical socket only. No wire/native/Scope-close fact is issued here.
        Ok(CloseReport::default())
    }
}
