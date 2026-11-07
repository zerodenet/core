//! Count and serialized-byte admission for the live event bridge. This is
//! Connector memory control, not business accounting or an event journal.
use super::super::memory::serialized_bytes;
use super::super::*;
use super::budget::{SourceBudget, SourceReservation};
use tokio::sync::Semaphore;

pub(in crate::dispatcher) struct QueuedSourceEvent {
    pub event: Arc<RawApiEvent>,
    pub reservation: SourceReservation,
}

impl QueuedSourceEvent {
    pub(in crate::dispatcher) fn into_blocked(self) -> BlockedEvent {
        let mut blocked = BlockedEvent::from_shared(self.event);
        blocked.source_reservation = Some(self.reservation);
        blocked
    }
}

pub(in crate::dispatcher) fn spawn<S: EventStream + Send + 'static>(
    subscriber: super::original::Subscriber<S>,
    cancelled: Arc<AtomicBool>,
    capacity: usize,
    byte_limit: u32,
    budget: Arc<SourceBudget>,
) -> (
    tokio::sync::mpsc::Receiver<QueuedSourceEvent>,
    Arc<SourceBudget>,
    std::thread::JoinHandle<()>,
) {
    // Validated config uses <=256 MiB. Direct embedders also cannot exceed a
    // platform's semaphore ceiling or request an impossible reservation.
    let byte_limit = byte_limit.clamp(1, Semaphore::MAX_PERMITS.min(u32::MAX as usize) as u32);
    let capacity = capacity.clamp(1, Semaphore::MAX_PERMITS);
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    let thread_budget = budget.clone();
    let runtime = tokio::runtime::Handle::current();
    let task = std::thread::Builder::new()
        .name("zero-event-source".to_owned())
        .spawn(move || {
            let send = |event: super::original::Record| {
                // An existing oversized record is admitted alone. Do not turn
                // a new memory setting into implicit event deletion or stall
                // forever waiting for more permits than the budget contains.
                let size = serialized_bytes(&*event);
                let bytes = size.min(u64::from(byte_limit)) as u32;
                thread_budget.waiting_bytes.store(size, Ordering::Relaxed);
                let permit = runtime.block_on(thread_budget.acquire(bytes.max(1)));
                thread_budget.waiting_bytes.store(0, Ordering::Relaxed);
                let Ok(permit) = permit else {
                    return false;
                };
                let reservation = thread_budget.reserve(permit, size);
                tx.blocking_send(QueuedSourceEvent {
                    event: event.into_shared(),
                    reservation,
                })
                .is_ok()
            };
            while !cancelled.load(Ordering::Acquire) {
                match subscriber.receive(Duration::from_millis(250)) {
                    Ok(event) => {
                        if !send(event) {
                            return;
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                }
            }
            while let Some(event) = subscriber.try_receive() {
                if !send(event) {
                    return;
                }
            }
        })
        .expect("event source bridge thread");
    (rx, budget, task)
}
