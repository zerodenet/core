//! A single poll lease borrows the retained future, never its actual controller.
use super::*;
use std::{
    sync::Mutex,
    task::{Context, Poll},
};
use tokio::time::{sleep_until, Duration, Instant};
struct PollLease {
    entry: Arc<Mutex<Entry>>,
    closing: Option<Closing>,
    retry: Option<Pin<Box<Sleep>>>,
}
impl Drop for PollLease {
    fn drop(&mut self) {
        let mut entry = self
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entry.poll_panicked |= std::thread::panicking();
        entry.closing = self.closing.take();
        entry.retry = self.retry.take();
        entry.polling = false;
    }
}
impl Entry {
    pub(in crate::component::datagram_retirement) fn poll_closed(
        entry: &Arc<Mutex<Self>>,
        cx: &mut Context<'_>,
        deadline: Instant,
    ) -> Poll<Result<(), Arc<ChannelError>>> {
        Self::cancel_pending(entry);
        let mut lease = {
            let mut state = entry.lock().expect("Datagram entry poisoned");
            if state.closed {
                return Poll::Ready(Ok(()));
            }
            if state.reserved || state.active || state.polling || state.poll_panicked {
                return Poll::Pending;
            }
            if state.failure_pending {
                return Poll::Ready(Err(state
                    .failure
                    .as_ref()
                    .expect("actual pending failure")
                    .clone()));
            }
            if !state.protocol_closed && state.closing.is_none() {
                let actual = state
                    .control
                    .as_ref()
                    .expect("retained actual controller")
                    .clone();
                state.closing = Some(Box::pin(async move {
                    let mut control = actual.lock().await;
                    control
                        .as_mut()
                        .expect("retained actual controller")
                        .wait_closed(deadline)
                        .await
                }));
            }
            state.polling = true;
            PollLease {
                entry: entry.clone(),
                closing: state.closing.take(),
                retry: state.retry.take(),
            }
        };
        if let Some(retry) = &mut lease.retry {
            if retry.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            lease.retry = None;
        }
        if lease.closing.is_none() {
            return finish_controller(entry, &mut lease, cx, deadline);
        }
        let Poll::Ready(result) = lease.closing.as_mut().unwrap().as_mut().poll(cx) else {
            return Poll::Pending;
        };
        // Drop the completed protocol future outside metadata locks. Its actual
        // controller continues to live in the same Entry's independent cell.
        lease.closing = None;
        let mut retry = None;
        let (result, previous) = {
            let mut state = entry.lock().expect("Datagram entry poisoned");
            match result {
                Ok(mut report) => {
                    let previous = state.failure.take();
                    state.failure_pending = false;
                    state.protocol_closed = report.is_complete();
                    state.unfinished = std::mem::take(&mut report.unfinished);
                    state.receipts.append(report);
                    if state.protocol_closed {
                        (Poll::Ready(Ok(())), previous)
                    } else {
                        retry = Some(Box::pin(sleep_until(
                            deadline.min(Instant::now() + Duration::from_millis(10)),
                        )));
                        (Poll::Pending, previous)
                    }
                }
                Err(error) => {
                    let source = Arc::new(error);
                    let previous = state.failure.replace(source.clone());
                    state.failure_pending = true;
                    retry = Some(Box::pin(sleep_until(
                        Instant::now() + Duration::from_millis(10),
                    )));
                    (Poll::Ready(Err(source)), previous)
                }
            }
        };
        // An original error source may run arbitrary last-reference Drop code.
        drop(previous);
        if let Some(mut retry) = retry {
            let _ = retry.as_mut().poll(cx);
            lease.retry = Some(retry);
        }
        if matches!(&result, Poll::Ready(Ok(()))) {
            return finish_controller(entry, &mut lease, cx, deadline);
        }
        result
    }
}
fn finish_controller(
    entry: &Arc<Mutex<Entry>>,
    lease: &mut PollLease,
    cx: &mut Context<'_>,
    deadline: Instant,
) -> Poll<Result<(), Arc<ChannelError>>> {
    let cell = entry
        .lock()
        .expect("Datagram entry poisoned")
        .control
        .as_ref()
        .expect("installed original controller")
        .clone();
    let Ok(mut control) = cell.try_lock() else {
        let mut retry = Box::pin(sleep_until(
            deadline.min(Instant::now() + Duration::from_millis(10)),
        ));
        let _ = retry.as_mut().poll(cx);
        lease.retry = Some(retry);
        return Poll::Pending;
    };
    let actual = control.take();
    drop(control);
    // Actual completion is already durable, but a last protocol/control Drop
    // may call arbitrary code or panic. Run it before removing the entry or
    // handing its receipts to a caller. Unwind keeps those receipts charged.
    drop(actual);
    entry.lock().expect("Datagram entry poisoned").closed = true;
    Poll::Ready(Ok(()))
}
