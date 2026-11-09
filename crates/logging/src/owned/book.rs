//! Same concrete logger book: local chunks, global payload/storage/outputs.
use super::*;
use zero_traits::packet_metadata::{
    PacketMetadataCounter, PacketMetadataCredit, PacketMetadataLayout,
};
mod candidate;
pub use candidate::{LoggingBudgetCandidate, LoggingHandle};
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    pub pending_chunks: usize,
    pub pending_bytes: usize,
    pub storage_bytes: usize,
    pub outputs: usize,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Limits {
    chunks: usize,
    bytes: usize,
    storage: usize,
    outputs: usize,
}
impl Limits {
    pub(super) fn new(config: &LogConfig) -> io::Result<Self> {
        config.validate_owner_limits().map_err(io::Error::other)?;
        std::alloc::Layout::array::<queue::Frame>(config.effective_max_pending_chunks())
            .map_err(io::Error::other)?;
        std::alloc::Layout::array::<file::Output>(config.effective_max_outputs())
            .map_err(io::Error::other)?;
        config
            .effective_max_outputs()
            .checked_mul(config.effective_max_pending_chunks())
            .ok_or_else(capacity)?;
        Ok(Self {
            chunks: config.effective_max_pending_chunks(),
            bytes: config.effective_max_pending_bytes(),
            storage: config.effective_max_storage_bytes(),
            outputs: config.effective_max_outputs(),
        })
    }
    fn intersect(self, target: Self) -> Self {
        Self {
            chunks: self.chunks.min(target.chunks),
            bytes: self.bytes.min(target.bytes),
            storage: self.storage.min(target.storage),
            outputs: self.outputs.min(target.outputs),
        }
    }
}
struct State {
    used: Usage,
    per_output: Vec<usize>,
    limits: Limits,
    pending: Option<(u64, Limits)>,
    serial: u64,
    stopped: bool,
    preparing: usize,
}
pub(super) struct Book {
    state: Mutex<State>,
    metadata: Arc<PacketMetadataCounter>,
    pub(super) wake: Arc<wake::Wake>,
    // Counter array is freed before its actual allocation credit.
    _counts: Storage,
}
impl Book {
    pub(super) fn new(limits: Limits, outputs: usize) -> io::Result<Arc<Self>> {
        if outputs > limits.outputs {
            return Err(capacity());
        }
        let metadata = PacketMetadataCounter::new_owned(PacketMetadataLayout::new(
            limits.storage,
            limits.storage,
        ))
        .map_err(io::Error::other)?;
        let layout = PacketMetadataLayout::array::<usize>(outputs)
            .and_then(|n| n.checked_add(PacketMetadataLayout::arc::<Book>()?))
            .and_then(|n| n.checked_add(PacketMetadataLayout::arc::<Shared>()?))
            .and_then(|n| n.checked_add(PacketMetadataLayout::arc::<wake::Wake>()?))
            .map_err(io::Error::other)?;
        // The one cold book, relay and fixed output array are prepaid before
        // construction. Their final retirement has no remaining Root waiter.
        let credit = metadata
            .reserve(layout, std::task::Waker::noop().clone())
            .map_err(io::Error::other)?;
        let wake = wake::Wake::new();
        let mut counts = Vec::new();
        counts
            .try_reserve_exact(outputs)
            .map_err(io::Error::other)?;
        counts.resize(outputs, 0);
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                used: Usage {
                    outputs,
                    ..Usage::default()
                },
                per_output: counts,
                limits,
                pending: None,
                serial: 0,
                stopped: false,
                preparing: 0,
            }),
            metadata,
            wake,
            _counts: credit,
        }))
    }
    fn effective(state: &State) -> Limits {
        state
            .pending
            .map_or(state.limits, |(_, target)| state.limits.intersect(target))
    }
    pub(super) fn usage(&self) -> Usage {
        let mut usage = self.state.lock().unwrap_or_else(|e| e.into_inner()).used;
        usage.storage_bytes = self.metadata.snapshot().occupied.bytes();
        usage
    }
    pub(super) fn payload(self: &Arc<Self>, output: usize, bytes: usize) -> io::Result<Credit> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.stopped {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "logging admission stopped",
            ));
        }
        let limits = Self::effective(&state);
        let local = state.per_output.get(output).copied().ok_or_else(|| {
            io::Error::new(io::ErrorKind::PermissionDenied, "foreign logging output")
        })?;
        let pending = add(state.used.pending_bytes, bytes)?;
        if local >= limits.chunks || pending > limits.bytes {
            return Err(capacity());
        }
        state.used.pending_chunks = add(state.used.pending_chunks, 1)?;
        state.used.pending_bytes = pending;
        state.preparing = add(state.preparing, 1)?;
        state.per_output[output] = local + 1;
        Ok(Credit {
            book: self.clone(),
            output,
            bytes,
            preparing: true,
        })
    }
    pub(super) fn storage(&self, bytes: usize) -> io::Result<Storage> {
        self.layout(PacketMetadataLayout::new(usize::from(bytes != 0), bytes))
    }
    pub(super) fn layout(&self, layout: PacketMetadataLayout) -> io::Result<Storage> {
        let wake = std::task::Waker::from(self.wake.clone());
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let maximum = Self::effective(&state).storage;
        if add(self.metadata.snapshot().occupied.bytes(), layout.bytes())? > maximum {
            return Err(capacity());
        }
        let result = self
            .metadata
            .reserve(layout, wake)
            .map_err(io::Error::other);
        drop(state);
        result
    }
    pub(super) fn preparing(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .preparing
    }
    pub(super) fn seal(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).stopped = true;
        self.wake.notify();
    }
    pub(super) fn release_outputs(&self) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .used
            .outputs = 0;
        self.wake.notify();
    }
}
pub(super) type Storage = PacketMetadataCredit;
pub(super) struct Credit {
    book: Arc<Book>,
    output: usize,
    bytes: usize,
    preparing: bool,
}
impl Credit {
    pub(super) fn queued(&mut self) {
        if self.preparing {
            self.book
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .preparing -= 1;
            self.preparing = false;
        }
    }
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut state = self.book.state.lock().unwrap_or_else(|e| e.into_inner());
        state.used.pending_chunks -= 1;
        if self.preparing {
            state.preparing -= 1;
        }
        state.used.pending_bytes -= self.bytes;
        state.per_output[self.output] -= 1;
        drop(state);
        self.book.wake.notify();
    }
}
