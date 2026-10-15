//! Connector-local delivery admission, independent of authoritative counters.
pub(super) use crate::encoding::serialized_bytes;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

pub(super) struct DeliveryBudget {
    limit: u64,
    used: AtomicU64,
    paging_blocked: AtomicBool,
}
impl DeliveryBudget {
    pub(super) fn new(limit: u32) -> Arc<Self> {
        Arc::new(Self {
            limit: u64::from(limit.max(1)),
            used: AtomicU64::new(0),
            paging_blocked: AtomicBool::new(false),
        })
    }
    pub(super) fn set_paging_blocked(&self, blocked: bool) {
        self.paging_blocked.store(blocked, Ordering::Relaxed);
    }
    pub(super) fn paging_blocked(&self) -> bool {
        self.paging_blocked.load(Ordering::Relaxed)
    }
    pub(super) fn snapshot(&self) -> zero_api::DeliveryBufferStatus {
        let reserved_bytes = self.used.load(Ordering::Relaxed);
        zero_api::DeliveryBufferStatus {
            byte_budget: self.limit,
            reserved_bytes,
            over_budget: reserved_bytes > self.limit,
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.used.load(Ordering::Relaxed) == 0
    }
    pub(super) fn available(&self) -> u64 {
        self.limit.saturating_sub(self.used.load(Ordering::Relaxed))
    }
    pub(super) fn can_reserve(&self, bytes: u64) -> bool {
        self.is_empty() || bytes <= self.available()
    }
    pub(super) fn reserve(self: &Arc<Self>, bytes: u64) -> Option<DeliveryReservation> {
        let bytes = bytes.max(1);
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            if used != 0 && bytes > self.limit.saturating_sub(used) {
                return None;
            }
            let next = used.checked_add(bytes)?;
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Some(DeliveryReservation {
                        budget: self.clone(),
                        bytes,
                    })
                }
                Err(actual) => used = actual,
            }
        }
    }
    // Only pre-existing reload state or already budgeted paging uses this.
    // Retaining facts can temporarily exceed a newly reduced configuration.
    pub(super) fn inherit(self: &Arc<Self>, bytes: u64) -> DeliveryReservation {
        let bytes = bytes.max(1);
        self.used.fetch_add(bytes, Ordering::Relaxed);
        DeliveryReservation {
            budget: self.clone(),
            bytes,
        }
    }
}
pub(super) struct DeliveryReservation {
    budget: Arc<DeliveryBudget>,
    bytes: u64,
}
impl Drop for DeliveryReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
#[path = "../../tests/delivery_memory/mod.rs"]
mod tests;
