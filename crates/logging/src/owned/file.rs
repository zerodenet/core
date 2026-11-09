//! One declared output graph; rotation never overlaps its native originals.
use super::*;
use zero_platform_tokio::file::{OriginalFile, OriginalFileClosed};
mod close;
mod rotate;
mod state;
pub(super) type Output = zero_traits::packet_metadata::PacketMetadataArc<Entry>;
pub(super) struct Entry {
    pub(super) output: usize,
    path: PathBuf,
    max_bytes: u64,
    max_files: usize,
    pub(super) book: Arc<book::Book>,
    native: Mutex<Native>,
    faults: Mutex<Faults>,
    pub(super) queue: Mutex<queue::Ring>,
    pub(super) working: Mutex<Option<queue::Frame>>,
    pub(super) gate: Gate,
    phase: std::sync::atomic::AtomicU8,
    stopped: std::sync::atomic::AtomicBool,
    unknown: std::sync::atomic::AtomicBool,
    native_closed: std::sync::atomic::AtomicBool,
    reopen: std::sync::atomic::AtomicBool,
    pub(super) started: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) panic_next_work: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    pub(super) panic_after_write_failure: std::sync::atomic::AtomicBool,
    _storage: book::Storage,
    _native_headers: book::Storage,
}
struct Native {
    original: Option<OriginalFile>,
    closed: Option<OriginalFileClosed>,
    acquiring: bool,
    written: u64,
    rotating: bool,
    unknown: Option<fault::IoFailure>,
}
struct Faults {
    pending: [Option<fault::IoFailure>; 4],
    slots: [Option<fault::Slot>; 4],
}
pub(super) struct Gate {
    ready: Mutex<bool>,
    changed: std::sync::Condvar,
}
impl Gate {
    fn new() -> Self {
        Self {
            ready: Mutex::new(false),
            changed: std::sync::Condvar::new(),
        }
    }
    pub(super) fn reset(&self) {
        *self.ready.lock().unwrap_or_else(|e| e.into_inner()) = false;
    }
    pub(super) fn open(&self) {
        *self.ready.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
    pub(super) fn wait(&self) {
        let mut ready = self.ready.lock().unwrap_or_else(|e| e.into_inner());
        while !*ready {
            ready = self.changed.wait(ready).unwrap_or_else(|e| e.into_inner());
        }
    }
}
impl Entry {
    pub(super) fn prepare(
        output: usize,
        config: &zero_config::LogFileConfig,
        book: &Arc<book::Book>,
    ) -> io::Result<Output> {
        let header = book.layout(Output::layout().map_err(io::Error::other)?)?;
        // Includes two transient rotation paths and their decimal suffixes.
        // Prepaid before any IO, so no metadata refund/wake runs under native.
        let path_envelope = config
            .path
            .len()
            .checked_mul(4)
            .and_then(|n| n.checked_add(128))
            .ok_or_else(capacity)?;
        let storage = book.storage(path_envelope)?;
        let native_headers =
            book.layout(OriginalFile::retained_header_layout().map_err(io::Error::other)?)?;
        let write = fault::Slot::reserve(book)?;
        let flush = fault::Slot::reserve(book)?;
        let close = fault::Slot::reserve(book)?;
        let open = fault::Slot::reserve(book)?;
        let value = Self {
            output,
            path: PathBuf::from(&config.path),
            max_bytes: config.max_bytes,
            max_files: config.max_files,
            book: book.clone(),
            native: Mutex::new(Native {
                original: None,
                closed: None,
                acquiring: false,
                written: 0,
                rotating: false,
                unknown: None,
            }),
            faults: Mutex::new(Faults {
                pending: [None, None, None, None],
                slots: [Some(write), Some(flush), Some(close), Some(open)],
            }),
            queue: Mutex::new(queue::Ring::empty()),
            working: Mutex::new(None),
            gate: Gate::new(),
            phase: 0.into(),
            stopped: false.into(),
            unknown: false.into(),
            native_closed: true.into(),
            reopen: false.into(),
            started: false.into(),
            #[cfg(test)]
            panic_next_work: false.into(),
            #[cfg(test)]
            panic_after_write_failure: false.into(),
            _storage: storage,
            _native_headers: native_headers,
        };
        match Output::from_prepaid(value, header) {
            Ok(output) => Ok(output),
            Err(_) => unreachable!("same output layout"),
        }
    }
    /// Strong controller already exists in the caller before this first open.
    pub(super) fn open(&self) -> io::Result<()> {
        let mut state = self.native.lock().unwrap_or_else(|e| e.into_inner());
        if state.original.is_some() || state.unknown.is_some() {
            return Err(io::Error::other("logging original already installed"));
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        state.acquiring = true;
        self.native_closed
            .store(false, std::sync::atomic::Ordering::Release);
        let file = match fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(file) => file,
            Err(source) => {
                state.acquiring = false;
                self.native_closed
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(source);
            }
        };
        state.original = Some(OriginalFile::adopt(file));
        state.acquiring = false;
        state.written = state.original.as_ref().unwrap().metadata()?.len();
        state.closed = None;
        Ok(())
    }
    fn source(
        &self,
        index: usize,
        phase: fault::IoPhase,
        error: io::Error,
        frame: Option<queue::Frame>,
    ) -> fault::IoFailure {
        let source = {
            let mut faults = self.faults.lock().unwrap_or_else(|e| e.into_inner());
            let source = faults.slots[index]
                .take()
                .expect("actual IO fault prepaid")
                .receive(self.output, phase, error, frame);
            faults.pending[index] = Some(source.clone());
            source
        };
        source
    }
    pub(super) fn receive_open_failure(&self, error: io::Error) -> fault::IoFailure {
        let frame = self.queue.lock().unwrap_or_else(|e| e.into_inner()).pop();
        let source = self.source(3, fault::IoPhase::Open, error, frame);
        self.require_reopen();
        self.mark_initialization_failed();
        source
    }
    pub(super) fn ensure_slot(&self, index: usize) -> bool {
        if self.faults.lock().unwrap_or_else(|e| e.into_inner()).slots[index].is_some() {
            return true;
        }
        let mut replacement = match fault::Slot::reserve(&self.book) {
            Ok(value) => Some(value),
            Err(_) => return false,
        };
        {
            let mut faults = self.faults.lock().unwrap_or_else(|e| e.into_inner());
            if faults.slots[index].is_none() {
                faults.slots[index] = replacement.take();
            }
        }
        drop(replacement);
        true
    }
    pub(super) fn write_working(&self) -> bool {
        if self.has_pending_io() || self.is_unknown() || !self.ensure_slot(0) {
            return false;
        }
        let mut working = self.working.lock().unwrap_or_else(|e| e.into_inner());
        let Some(frame) = working.as_ref() else {
            return true;
        };
        let result = {
            let mut native = self.native.lock().unwrap_or_else(|e| e.into_inner());
            self.write_locked(&mut native, &frame.bytes)
        };
        match result {
            Ok(()) => true,
            Err((phase, error)) => {
                let frame = working.take();
                drop(working);
                // Strongly retain the actual failed data before clearing its
                // local operation. A partial write never authorizes replay.
                let source = self.source(0, phase, error, frame);
                self.require_reopen();
                if phase == fault::IoPhase::Close {
                    self.native
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .unknown = Some(source);
                    self.unknown
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                false
            }
        }
    }
    fn write_locked(
        &self,
        state: &mut Native,
        bytes: &[u8],
    ) -> Result<(), (fault::IoPhase, io::Error)> {
        if state.rotating
            || (state.written > 0
                && state.written.saturating_add(bytes.len() as u64) > self.max_bytes)
        {
            self.rotate_locked(state)?;
        }
        state
            .original
            .as_mut()
            .ok_or_else(|| {
                (
                    fault::IoPhase::Open,
                    io::Error::other("logging original not open"),
                )
            })?
            .write_all(bytes)
            .map_err(|error| (fault::IoPhase::Write, error))?;
        state.written = state.written.saturating_add(bytes.len() as u64);
        Ok(())
    }
    pub(super) fn flush(&self) -> bool {
        if self.is_unknown() || self.has_pending_io() {
            return true;
        }
        if self
            .faults
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pending[1]
            .is_some()
        {
            return true;
        }
        if !self.ensure_slot(1) {
            return false;
        }
        let result = {
            let mut native = self.native.lock().unwrap_or_else(|e| e.into_inner());
            if native.closed.is_some() {
                Ok(())
            } else {
                match native.original.as_mut() {
                    Some(original) => original.flush(),
                    None => Ok(()),
                }
            }
        };
        if let Err(error) = result {
            self.source(1, fault::IoPhase::Flush, error, None);
            self.require_reopen();
        }
        self.book.wake.notify();
        true
    }
}
