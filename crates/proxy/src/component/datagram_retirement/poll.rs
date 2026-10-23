//! The existing waiter is the sole poll consumer; no parent lock spans a callout.
use super::*;
use std::task::{Context, Poll, Waker};
use tokio::time::{timeout_at, Instant};
impl RetiredDatagrams {
    pub(crate) async fn wait_closed(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.retire_all();
        let Ok(_waiter) = timeout_at(deadline, self.waiter.lock()).await else {
            return Ok(self.snapshot_all(false));
        };
        let target = Arc::new(WaitTarget::default());
        match timeout_at(
            deadline,
            std::future::poll_fn(|cx| self.poll_entries(cx, &target, deadline, false)),
        )
        .await
        {
            Ok(Err(source)) => Err(super::super::instance::retained(source)),
            Ok(Ok(())) | Err(_) => Ok(self.snapshot_all(true)),
        }
    }
    pub(super) async fn wait_one(
        &self,
        id: u64,
        entry: &Arc<Mutex<Entry>>,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.retire(id, entry, CloseReason::Shutdown);
        let Ok(_waiter) = timeout_at(deadline, self.waiter.lock()).await else {
            return Ok(entry
                .lock()
                .expect("Datagram entry poisoned")
                .snapshot(id, false));
        };
        let target = Arc::new(WaitTarget::default());
        let result = timeout_at(
            deadline,
            std::future::poll_fn(|cx| {
                target.register(cx);
                self.wake.bind(&target);
                let _callbacks = self.wake.enter();
                let waker = Waker::from(self.wake.clone());
                Entry::poll_closed(entry, &mut Context::from_waker(&waker), deadline)
            }),
        )
        .await;
        if let Ok(Err(source)) = result {
            entry
                .lock()
                .expect("Datagram entry poisoned")
                .acknowledge_failure(&source);
            return Err(super::super::instance::retained(source));
        }
        let (report, closed) = {
            let mut actual = entry.lock().expect("Datagram entry poisoned");
            (actual.snapshot(id, true), actual.closed)
        };
        if closed {
            self.remove_closed(&[(id, entry.clone())])
        }
        Ok(report)
    }
    pub(super) fn poll_entries(
        &self,
        cx: &mut Context<'_>,
        target: &Arc<WaitTarget>,
        deadline: Instant,
        retired_only: bool,
    ) -> Poll<Result<(), Arc<ChannelError>>> {
        target.register(cx);
        self.wake.bind(target);
        let _callbacks = self.wake.enter();
        let waker = Waker::from(self.wake.clone());
        let mut internal = Context::from_waker(&waker);
        let entries = self.entries();
        let mut complete = true;
        let mut receipts = false;
        let mut failure = None;
        for (_, entry) in &entries {
            if retired_only {
                let state = entry.lock().expect("Datagram entry poisoned");
                if state.reserved || state.active {
                    continue;
                }
            }
            match Entry::poll_closed(entry, &mut internal, deadline) {
                Poll::Pending => complete = false,
                Poll::Ready(Err(source)) => {
                    complete = false;
                    failure.get_or_insert(source);
                }
                Poll::Ready(Ok(())) => {}
            }
            let state = entry.lock().expect("Datagram entry poisoned");
            receipts |= state.closed || state.has_receipts();
        }
        if retired_only && receipts {
            return Poll::Ready(Ok(()));
        }
        if let Some(source) = failure {
            for (_, entry) in entries {
                entry
                    .lock()
                    .expect("Datagram entry poisoned")
                    .acknowledge_failure(&source)
            }
            return Poll::Ready(Err(source));
        }
        if complete {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
