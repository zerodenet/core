//! Normal UDP drivers use the same retained consumer without stopping admission.
use super::*;
use tokio::time::{timeout_at, Instant};
impl RetiredDatagrams {
    pub(crate) async fn progress_retired(
        &self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let available = self.entries().iter().any(|(_, entry)| {
                let state = entry.lock().expect("Datagram entry poisoned");
                !state.reserved && !state.active
            });
            if available {
                break;
            }
            if timeout_at(deadline, changed).await.is_err() {
                return Ok(CloseReport::default());
            }
        }
        let Ok(_waiter) = timeout_at(deadline, self.waiter.lock()).await else {
            return Ok(self.snapshot_retired(false));
        };
        let target = Arc::new(WaitTarget::default());
        match timeout_at(
            deadline,
            std::future::poll_fn(|cx| self.poll_entries(cx, &target, deadline, true)),
        )
        .await
        {
            Ok(Err(source)) => Err(super::super::instance::retained(source)),
            Ok(Ok(())) | Err(_) => Ok(self.snapshot_retired(true)),
        }
    }
}
