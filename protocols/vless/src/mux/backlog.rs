use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) const MUX_RESPONSE_QUEUE_CAPACITY: usize =
    crate::validation::DEFAULT_MUX_RESPONSE_BACKLOG_FRAMES as usize;
pub(crate) const DEFAULT_MUX_RESPONSE_BACKLOG_BYTES: usize =
    crate::validation::DEFAULT_MUX_RESPONSE_BACKLOG_BYTES as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MuxResponseBacklogPolicy {
    frames: usize,
    bytes: usize,
}

#[derive(Clone)]
pub(crate) struct MuxResponseBacklog {
    inner: Arc<MuxResponseBacklogInner>,
}

struct MuxResponseBacklogInner {
    limit: usize,
    used: AtomicUsize,
}

pub(crate) struct BufferedMuxResponse<T> {
    value: Option<T>,
    backlog: MuxResponseBacklog,
    bytes: usize,
}

impl MuxResponseBacklog {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(MuxResponseBacklogInner {
                limit,
                used: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn from_policy(policy: MuxResponseBacklogPolicy) -> Self {
        Self::new(policy.bytes())
    }

    pub(crate) fn try_buffer<T>(
        &self,
        bytes: usize,
        value: T,
    ) -> Result<BufferedMuxResponse<T>, T> {
        let mut used = self.inner.used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(bytes) else {
                return Err(value);
            };
            if next > self.inner.limit {
                return Err(value);
            }
            match self.inner.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(BufferedMuxResponse {
                        value: Some(value),
                        backlog: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.inner.used.load(Ordering::Acquire)
    }
}

impl MuxResponseBacklogPolicy {
    pub(crate) fn from_config(
        frames: Option<u32>,
        bytes: Option<u64>,
    ) -> Result<Self, zero_core::Error> {
        crate::validation::validate_mux_response_backlog(frames, bytes)
            .map_err(zero_core::Error::Config)?;
        let frames = frames.unwrap_or(MUX_RESPONSE_QUEUE_CAPACITY as u32);
        let bytes = bytes.unwrap_or(DEFAULT_MUX_RESPONSE_BACKLOG_BYTES as u64);
        Ok(Self {
            frames: frames as usize,
            bytes: bytes as usize,
        })
    }

    pub(crate) const fn frames(self) -> usize {
        self.frames
    }

    pub(crate) const fn bytes(self) -> usize {
        self.bytes
    }
}

impl Default for MuxResponseBacklogPolicy {
    fn default() -> Self {
        Self {
            frames: MUX_RESPONSE_QUEUE_CAPACITY,
            bytes: DEFAULT_MUX_RESPONSE_BACKLOG_BYTES,
        }
    }
}

impl<T> BufferedMuxResponse<T> {
    pub(crate) fn into_inner(mut self) -> T {
        self.value
            .take()
            .expect("buffered MUX response value is present")
    }
}

impl<T> Drop for BufferedMuxResponse<T> {
    fn drop(&mut self) {
        self.backlog
            .inner
            .used
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
