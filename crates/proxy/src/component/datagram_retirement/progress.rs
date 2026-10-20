//! Normal UDP drivers advance retirement without stopping new admissions.
use super::RetiredDatagrams;
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tokio::time::{timeout_at, Instant};
use zero_platform_tokio::contract::{lifecycle::CloseReport, ChannelError};
impl RetiredDatagrams {
    pub(crate) async fn progress_retired(
        &self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        loop {
            // Register before checking state so retirement cannot be missed
            // when there are no upstream datagrams to wake the actual driver.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let available = {
                let state = self.state.lock().expect("Datagram owner poisoned");
                state.entries.values().any(|entry| {
                    let entry = entry.lock().expect("Datagram entry poisoned");
                    !entry.reserved && !entry.active
                })
            };
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
        match timeout_at(
            deadline,
            std::future::poll_fn(|cx| self.poll_retired(cx, deadline)),
        )
        .await
        {
            Ok(Err(source)) => Err(super::super::instance::retained(source)),
            Ok(Ok(())) | Err(_) => Ok(self.snapshot_retired(true)),
        }
    }
    fn poll_retired(
        &self,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> Poll<Result<(), Arc<ChannelError>>> {
        let mut state = self.state.lock().expect("Datagram owner poisoned");
        state.waker = Some(cx.waker().clone());
        let mut complete = true;
        let mut failure = None;
        for entry in state.entries.values() {
            let mut entry = entry.lock().expect("Datagram entry poisoned");
            if entry.reserved || entry.active {
                continue;
            }
            match entry.poll_closed(cx, deadline) {
                Poll::Pending => complete = false,
                Poll::Ready(Err(error)) => {
                    complete = false;
                    failure.get_or_insert(error);
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if let Some(error) = failure {
            return Poll::Ready(Err(error));
        }
        if complete {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn snapshot_retired(&self, take: bool) -> CloseReport {
        let mut state = self.state.lock().expect("Datagram owner poisoned");
        let mut report = CloseReport::default();
        let mut completed = Vec::new();
        for (id, entry) in &state.entries {
            let mut entry = entry.lock().expect("Datagram entry poisoned");
            if entry.reserved || entry.active {
                continue;
            }
            report.append(entry.snapshot(*id, take));
            if take && entry.closed {
                completed.push(*id);
            }
        }
        for id in completed {
            state.entries.remove(&id);
        }
        report
    }
}
