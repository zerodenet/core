use super::*;
#[derive(Clone)]
pub struct LoggingHandle(pub(in crate::owned) Arc<Book>);
pub struct LoggingBudgetCandidate {
    book: Arc<Book>,
    serial: u64,
    target: Limits,
    committed: bool,
}
impl LoggingHandle {
    pub(in crate::owned) fn new(book: Arc<Book>) -> Self {
        Self(book)
    }

    pub fn limits_match(&self, config: &LogConfig) -> io::Result<bool> {
        let target = Limits::new(config)?;
        Ok(self
            .0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .limits
            == target)
    }
    pub fn validate_limits(&self, config: &LogConfig) -> io::Result<()> {
        let target = Limits::new(config)?;
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.used.pending_bytes > target.bytes
            || state.used.outputs > target.outputs
            || state.per_output.iter().any(|count| *count > target.chunks)
        {
            return Err(capacity());
        }
        self.0
            .metadata
            .check_limit(PacketMetadataLayout::new(target.storage, target.storage))
            .map_err(io::Error::other)
    }
    pub fn prepare_limits(&self, config: &LogConfig) -> io::Result<LoggingBudgetCandidate> {
        let target = Limits::new(config)?;
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.pending.is_some() {
            return Err(capacity());
        }
        if state.used.pending_bytes > target.bytes
            || state.used.outputs > target.outputs
            || state.per_output.iter().any(|count| *count > target.chunks)
        {
            return Err(capacity());
        }
        self.0
            .metadata
            .check_limit(PacketMetadataLayout::new(target.storage, target.storage))
            .map_err(io::Error::other)?;
        let serial = state
            .serial
            .checked_add(1)
            .ok_or_else(|| io::Error::other("logging budget identity exhausted"))?;
        state.serial = serial;
        state.pending = Some((serial, target));
        Ok(LoggingBudgetCandidate {
            book: self.0.clone(),
            serial,
            target,
            committed: false,
        })
    }
    pub fn usage(&self) -> Usage {
        self.0.usage()
    }
}
impl LoggingBudgetCandidate {
    pub fn commit_deferred(&mut self) {
        let mut state = self.book.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            state
                .pending
                .is_some_and(|(serial, target)| serial == self.serial && target == self.target),
            "same logging candidate"
        );
        // Every payload and metadata growth has used the pending intersection
        // since prepare, so publication cannot newly fail or evict old data.
        self.book
            .metadata
            .install_limit(PacketMetadataLayout::new(
                self.target.storage,
                self.target.storage,
            ))
            .expect("same held logging census fenced by original candidate");
        state.limits = self.target;
        state.pending = None;
        self.committed = true;
        drop(state);
    }
    /// Call only after the real Engine/Host/application publication guards drop.
    pub fn publish_committed(&self) {
        assert!(self.committed, "logging metadata actually committed");
        self.book.wake.notify();
    }
}
impl Drop for LoggingBudgetCandidate {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = self.book.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .pending
            .is_some_and(|(serial, _)| serial == self.serial)
        {
            state.pending = None;
        }
        drop(state);
        self.book.wake.notify();
    }
}
