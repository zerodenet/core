//! One paid row per actual retained RX future, polled by the existing driver.
use super::*;
use std::task::{Context, Poll};
pub(super) struct ReceiveRow {
    ordinal: u64,
    work: Option<PaidReceive>,
}
impl ReceiveRow {
    pub(super) fn new(ordinal: u64, work: PaidReceive) -> Self {
        Self {
            ordinal,
            work: Some(work),
        }
    }
}
struct Polling<'a> {
    shared: &'a Shared,
    ordinal: u64,
    work: Option<PaidReceive>,
    finished: bool,
}
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let work = self.work.take();
        // A real poll panic must not destroy that original RX Box before the
        // owning Critical/Raw task's actual Scope receiver observes its exit.
        let old = {
            let mut rows = self
                .shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rows.iter_mut()
                .find(|row| row.ordinal == self.ordinal)
                .expect("original retained RX row survives polling")
                .work
                .replace(work.expect("same original receive future"))
        };
        assert!(
            old.is_none(),
            "only the original response driver polls this row"
        );
        drop(old);
    }
}
fn poll_rows(shared: &Shared, cx: &mut Context<'_>) -> Poll<Result<(), ChannelError>> {
    let maximum = shared.next.load(std::sync::atomic::Ordering::Acquire);
    let mut next = 0;
    let mut completed = false;
    loop {
        let candidate = {
            let mut rows = shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            rows.iter_mut()
                .find(|row| row.ordinal >= next && row.ordinal < maximum)
                .map(|row| (row.ordinal, row.work.take()))
        };
        let Some((ordinal, work)) = candidate else {
            break;
        };
        next = ordinal + 1; // captured next counter proves this cannot overflow
        let Some(work) = work else {
            continue;
        };
        let mut polling = Polling {
            shared,
            ordinal,
            work: Some(work),
            finished: false,
        };
        if let Poll::Ready(result) = Pin::new(polling.work.as_mut().unwrap()).poll(cx) {
            polling.finished = true;
            let row = {
                let mut rows = shared
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let position = rows
                    .iter()
                    .position(|row| row.ordinal == ordinal)
                    .expect("same completed RX row");
                rows.remove(position)
            };
            drop(row);
            drop(polling.work.take());
            result?;
            completed = true;
        }
    }
    if completed {
        Poll::Ready(Ok(()))
    } else {
        Poll::Pending
    }
}
pub(super) async fn run(shared: PacketMetadataArc<Shared>) -> Result<(), ChannelError> {
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        tokio::select! { biased;
            _=shared.stop.cancelled()=>break,
            _=changed=>{},
            result=std::future::poll_fn(|cx| poll_rows(&shared,cx))=>{ result?; }
        }
    }
    // Original bodies/views are destroyed outside the row lock; their actual
    // logical controllers remain separately installed in RetiredDatagrams.
    let pending = std::mem::take(
        &mut *shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    drop(pending);
    if shared.failures.first.get().is_some() {
        Err(
            ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                .with_metadata_source(shared.failures.clone().into_error()),
        )
    } else {
        Ok(())
    }
}
