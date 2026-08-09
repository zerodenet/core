use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) use crate::validation::MuxResponseBacklogPolicy;

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
