use super::*;
use zero_platform_tokio::contract::lifecycle::{ChannelControl, CloseReport, Unfinished};
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.session.cancel();
    }
    async fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.session.cancel();
        loop {
            let changed = self.session.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let unknown = self.session.unknown.load(Ordering::Acquire);
            let original = self
                .session
                .parent
                .upgrade()
                .and_then(|parent| parent.driver.get().cloned());
            if self.session.close_ack.load(Ordering::Acquire)
                && (!unknown || original.as_ref().is_some_and(|driver| driver.is_joined()))
            {
                return Ok(CloseReport::default());
            }
            if unknown {
                if let Some(original) = original.filter(|driver| !driver.is_joined()) {
                    if tokio::time::timeout_at(deadline, original.wait())
                        .await
                        .is_err()
                    {
                        return Ok(CloseReport {
                            unfinished: vec![Unfinished::Resource(
                                "Unknown native Packet write still awaits its original driver join"
                                    .into(),
                            )],
                            ..Default::default()
                        });
                    }
                    continue;
                }
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "same original driver has not acknowledged Packet session closure".into(),
                    )],
                    ..Default::default()
                });
            }
        }
    }
}
