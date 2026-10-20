//! One fixed event slot per serialized native poll; no external callbacks here.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use zero_traits::IoObserver;
#[derive(Debug, Default)]
pub(super) struct Capture {
    read: AtomicBool,
    read_bytes: AtomicUsize,
    write: AtomicBool,
    write_bytes: AtomicUsize,
    error: AtomicBool,
}
pub(super) struct Observed {
    read: Option<usize>,
    write: Option<usize>,
    error: bool,
}
impl Capture {
    /// Drain before leaving the native guard: a later poll cannot merge, steal
    /// or duplicate this poll's original event, including zero-byte callbacks.
    pub(super) fn take(&self) -> Observed {
        Observed {
            read: self
                .read
                .swap(false, Ordering::Relaxed)
                .then(|| self.read_bytes.load(Ordering::Relaxed)),
            write: self
                .write
                .swap(false, Ordering::Relaxed)
                .then(|| self.write_bytes.load(Ordering::Relaxed)),
            error: self.error.swap(false, Ordering::Relaxed),
        }
    }
}
impl IoObserver for Capture {
    fn stream_boundary(&self) {} // real boundary is reported by persistent owner
    fn received(&self, bytes: usize) {
        self.read_bytes.store(bytes, Ordering::Relaxed);
        self.read.store(true, Ordering::Relaxed);
    }
    fn sent(&self, bytes: usize) {
        self.write_bytes.store(bytes, Ordering::Relaxed);
        self.write.store(true, Ordering::Relaxed);
    }
    fn error(&self) {
        self.error.store(true, Ordering::Relaxed);
    }
    fn dropped(&self) {}
}
impl Observed {
    pub(super) fn dispatch(self, observer: Option<&dyn IoObserver>) {
        if let Some(observer) = observer {
            if let Some(bytes) = self.read {
                observer.received(bytes);
            }
            if let Some(bytes) = self.write {
                observer.sent(bytes);
            }
            if self.error {
                observer.error();
            }
        }
    }
}
