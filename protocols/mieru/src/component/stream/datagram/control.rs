use super::shared::Shared;
use std::sync::Arc;
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::{
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    ChannelError, CloseReason,
};

pub struct Control {
    shared: Arc<Shared>,
    closed: bool,
}
impl Control {
    pub(super) fn original(&self) -> &zero_platform_tokio::controlled_carrier::CarrierOrigin {
        self.shared
            .original
            .as_ref()
            .expect("native session original")
    }
    pub(super) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            closed: false,
        }
    }
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.shared.stop.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        let close = async {
            loop {
                let changed = self.shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.shared.state.lock().unwrap().views == 0 {
                    break;
                }
                changed.await;
            }
            if let Some(owner) = &self.shared.owner {
                let returned = self.shared.state.lock().unwrap().stream.take();
                drop(returned);
                return owner.lock().await.close(deadline).await;
            }
            #[cfg(test)]
            {
                std::future::poll_fn(|cx| {
                    let mut state = self.shared.state.lock().unwrap();
                    if let Some(stream) = state.stream.as_mut() {
                        std::task::ready!(stream.as_mut().poll_shutdown(cx)).map_err(|source| {
                            self.shared
                                .io_error(zero_platform_tokio::contract::Stage::Close, source)
                        })?;
                    }
                    let returned = state.stream.take();
                    drop(state);
                    drop(returned);
                    std::task::Poll::Ready(Ok(CloseReport::default()))
                })
                .await
            }
            #[cfg(not(test))]
            {
                Err(self.shared.error(
                    zero_platform_tokio::contract::Stage::Close,
                    zero_platform_tokio::contract::Reason::Unsupported,
                    zero_platform_tokio::contract::DeliveryState::Unknown,
                ))
            }
        };
        match timeout_at(deadline, close).await {
            Ok(Ok(mut report)) => {
                if !report.is_complete() {
                    return Ok(report);
                }
                if !self.closed {
                    report
                        .released_resources
                        .push("Mieru UDP logical stream and transferred views".into());
                    self.closed = true;
                }
                // The pool's original provider, not this logical Control,
                // still owns and must prove physical closure and driver joins.
                Ok(report)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "Mieru UDP transferred views or logical stream shutdown".into(),
                )],
                ..Default::default()
            }),
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}
