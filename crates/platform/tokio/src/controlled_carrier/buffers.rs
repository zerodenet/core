//! The existing carrier root's pure byte book. No I/O or retirement authority.
use std::{
    io,
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CarrierBufferLimits {
    pub max_queue_packets: usize,
    pub total_max_bytes: usize,
}
impl Default for CarrierBufferLimits {
    fn default() -> Self {
        Self {
            max_queue_packets: 256,
            total_max_bytes: 64 * 1024 * 1024,
        }
    }
}
impl CarrierBufferLimits {
    pub fn validate(self) -> io::Result<()> {
        if !(1..=256).contains(&self.max_queue_packets)
            || !(1..=1_073_741_824).contains(&self.total_max_bytes)
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(())
    }
}
#[derive(Debug)]
struct State {
    limits: CarrierBufferLimits,
    used: usize,
    minimum_needed: usize,
}
#[derive(Debug)]
struct Book {
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}
/// Clones share the same actual root's book. This pure handle remains usable
/// by its original cleanup after business admission is cancelled; acquiring
/// bytes never authorizes any new socket, task, send or component generation.
#[derive(Clone, Debug)]
pub struct CarrierBufferBudget(Arc<Book>);
/// Move this credit with the actual backing allocations. Drop the allocations
/// before the credit, including failed/pending/retiring output. A codec must
/// prepay its complete real peak, not only its eventual wire payload length.
#[derive(Debug)]
pub struct CarrierBufferCredit {
    book: CarrierBufferBudget,
    bytes: usize,
}
impl Default for CarrierBufferBudget {
    fn default() -> Self {
        Self(Arc::new(Book {
            state: Mutex::new(State {
                limits: CarrierBufferLimits::default(),
                used: 0,
                minimum_needed: usize::MAX,
            }),
            changed: tokio::sync::Notify::new(),
        }))
    }
}
impl CarrierBufferBudget {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn limits(&self) -> CarrierBufferLimits {
        self.lock().limits
    }
    pub fn used_bytes(&self) -> usize {
        self.lock().used
    }
    /// Byte-book identity only, never same original/Scope/driver evidence.
    pub fn same_budget(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    fn acquire(&self, bytes: usize, waiting: bool) -> io::Result<CarrierBufferCredit> {
        let mut state = self.lock();
        if bytes > state.limits.total_max_bytes {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if bytes > state.limits.total_max_bytes - state.used {
            if waiting {
                state.minimum_needed = state.minimum_needed.min(bytes);
            }
            return Err(io::ErrorKind::WouldBlock.into());
        }
        state.used += bytes;
        Ok(CarrierBufferCredit {
            book: self.clone(),
            bytes,
        })
    }
    pub fn try_reserve(&self, bytes: usize) -> io::Result<CarrierBufferCredit> {
        self.acquire(bytes, false)
    }
    /// One inline future per original writer. Notify's intrusive waiters live
    /// in those actual futures: no root waiter Vec, task, pool or scheduler.
    /// Register before testing the same locked count to avoid a lost refund.
    pub fn reserve(
        &self,
        bytes: usize,
    ) -> impl std::future::Future<Output = io::Result<CarrierBufferCredit>> + Send + 'static {
        let actual = self.clone();
        async move {
            loop {
                let notified = actual.0.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                match actual.acquire(bytes, true) {
                    Ok(credit) => return Ok(credit),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => notified.await,
                    Err(error) => return Err(error),
                }
            }
        }
    }
    fn release(&self, bytes: usize) {
        let wake = {
            let mut state = self.lock();
            state.used = state
                .used
                .checked_sub(bytes)
                .expect("same live original byte credit");
            let ready = state.minimum_needed <= state.limits.total_max_bytes - state.used;
            if ready {
                state.minimum_needed = usize::MAX;
            }
            ready
        };
        // External wakers may reenter this book. Never wake under its lock.
        // No free>0 loop: wake only when some actual requested amount fits.
        if wake {
            self.0.changed.notify_waiters();
        }
    }
    pub(super) fn check_limits(&self, limits: CarrierBufferLimits) -> io::Result<()> {
        limits.validate()?;
        if self.used_bytes() > limits.total_max_bytes {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(())
    }
    pub(super) fn install_limits_deferred(&self, limits: CarrierBufferLimits) -> io::Result<bool> {
        limits.validate()?;
        let wake = {
            let mut state = self.lock();
            if state.used > limits.total_max_bytes {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            state.limits = limits;
            let ready = state.minimum_needed <= limits.total_max_bytes - state.used;
            if ready {
                state.minimum_needed = usize::MAX;
            }
            ready
        };
        Ok(wake)
    }
    pub(super) fn publish_capacity(&self, wake: bool) {
        if wake {
            self.0.changed.notify_waiters();
        }
    }
}
impl CarrierBufferCredit {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    /// Split prepaid peak storage when its actual allocations acquire separate
    /// owners. Splitting does not reserve/refund or allocate another byte book.
    pub fn split_off(&mut self, bytes: usize) -> io::Result<Self> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(io::ErrorKind::InvalidInput)?;
        Ok(Self {
            book: self.book.clone(),
            bytes,
        })
    }
    /// Only after the discarded backing capacity is actually destroyed.
    pub fn shrink_to(&mut self, bytes: usize) -> io::Result<()> {
        let released = self
            .bytes
            .checked_sub(bytes)
            .ok_or(io::ErrorKind::InvalidInput)?;
        self.bytes = bytes;
        self.book.release(released);
        Ok(())
    }
}
impl Drop for CarrierBufferCredit {
    fn drop(&mut self) {
        self.book.release(self.bytes);
    }
}

#[cfg(test)]
mod tests;
