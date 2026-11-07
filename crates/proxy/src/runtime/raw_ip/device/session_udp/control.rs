//! Same original pending send and logical close remain cancellation-safe.
use super::*;
use std::future::poll_fn;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport},
    CloseReason,
};
impl ChannelControl for Control {
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.shared.stop();
        self.shared.completion.continuation_started();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        if self.shared.poll_pending(&mut cx).is_pending()
            || !self
                .shared
                .original
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed
                .as_ref()
                .expect("same original close")
                .io_quiescent()
        {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "same logical UDP send continuation remains pending".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        self.finish()
    }

    fn cancel(&self, _: CloseReason) {
        self.shared.stop();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        let end = *self.deadline.get_or_insert(deadline);
        self.shared.stop();
        let closed = self
            .shared
            .original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
            .clone()
            .expect("same logical socket explicitly closed");
        self.shared.completion.continuation_started();
        tokio::time::timeout_at(end, async {
            // Drive the original pending send to its actual accepted-prefix
            // result; cancellation of this wait leaves it in the same owner.
            let _ = poll_fn(|cx| self.shared.poll_pending(cx)).await;
            closed.wait_io_quiescent().await;
        })
        .await
        .map_err(|source| self.shared.completion.deadline_failure(source))?;
        self.finish()
    }
}
impl Control {
    fn finish(&self) -> Result<CloseReport, ChannelError> {
        // The exact receipt was stored from this original socket's close,
        // and its one admitted send continuation really returned above.
        self.shared.completion.confirm();
        let user = self.shared.user.lock().unwrap().take();
        if let Some(user) = &user {
            user.mark_logical_closed();
        }
        drop(user);
        if let Some(source) = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .first
            .clone()
        {
            return Err(self.shared.completion.terminal_failure(source));
        }
        // Logical socket only. No wire/native/Scope-close fact is issued here.
        Ok(CloseReport::default())
    }
}
