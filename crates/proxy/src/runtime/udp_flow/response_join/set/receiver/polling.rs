//! Restore the same operation on unwind before the original driver reports it.
use super::*;
use std::task::Poll;
struct Polling<'a> {
    receiver: &'a PacketReceiver,
    work: Option<Work>,
    finished: bool,
}
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let work = self.work.take();
        if std::thread::panicking() {
            self.receiver.panicked.store(true, Ordering::Release);
        }
        let _notifications = self.receiver.metadata.notifications();
        let previous = {
            self.receiver
                .work
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(work.expect("same RX Box remains held during polling"))
        };
        assert!(
            previous.is_none(),
            "the original response lease is the sole poller"
        );
        drop(previous);
    }
}
impl PacketReceiver {
    pub(in crate::runtime::udp_flow::response_join::set) fn poll(&self, cx: &mut Context<'_>) {
        if self.is_complete() || self.panicked.load(Ordering::Acquire) {
            return;
        }
        let work = self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(work) = work else {
            return;
        };
        let mut polling = Polling {
            receiver: self,
            work: Some(work),
            finished: false,
        };
        let result = polling.work.as_mut().unwrap().body.as_mut().poll(cx);
        if let Poll::Ready(result) = result {
            if result.is_err() {
                self.receive_failure();
            }
            // Actual source ownership precedes destroying the completed future.
            polling.finished = true;
            drop(polling.work.take());
            self.complete.store(true, Ordering::Release);
        }
    }
}
