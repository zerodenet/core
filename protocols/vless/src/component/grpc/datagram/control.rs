//! Return both real packet views and then consume the same logical controller.
use super::*;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    CloseReason,
};
pub struct Control(pub(super) Arc<Shared>);
impl ChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.0.stop(reason);
    }
    async fn wait_closed(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        let returned = async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let stream = {
                    let mut state = self.0.state.lock().unwrap();
                    if state.views == 0 {
                        state.stream.take()
                    } else {
                        None
                    }
                };
                if let Some(stream) = stream {
                    drop(stream);
                    return;
                }
                if self.0.state.lock().unwrap().stream.is_none() {
                    return;
                }
                changed.await;
            }
        };
        if tokio::time::timeout_at(end, returned).await.is_err() {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Io(
                    "VLESS UDP logical packet views remain retained".into(),
                )],
                ..Default::default()
            });
        }
        let mut owner = tokio::time::timeout_at(end, self.0.owner.lock())
            .await
            .map_err(|source| {
                self.0
                    .error(Stage::Close, Reason::Timeout, DeliveryState::Unknown)
                    .with_source(source)
            })?;
        owner.close(end).await
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.0.stop(CloseReason::Shutdown);
    }
}
