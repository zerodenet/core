//! Admission on the one concrete dispatcher's existing task/resource inventories.
//! Permits never prove a task joined, a view returned, or an original closed.
use std::{io, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zero_config::{ApiConfig, EventDispatcherConfig, EventSinkConfig};

pub(crate) struct DispatcherCapacity {
    tasks: Arc<Semaphore>,
    // No original waiter exists: refusal precedes native acquisition. Returning
    // this permit cannot call an external waker under a resource-inventory lock.
    originals: Arc<Semaphore>,
}
pub(crate) struct TaskCharge(Option<OwnedSemaphorePermit>);
pub(crate) struct OriginalCharge(Option<OwnedSemaphorePermit>);
macro_rules! original_charge {
    ($kind:ty) => {
        impl $kind {
            /// Only the actual inventory's consumed join/close or known
            /// non-acquisition path may call this. Drop cannot refund Unknown.
            pub(crate) fn complete(mut self) {
                drop(self.0.take());
            }
        }
        impl Drop for $kind {
            fn drop(&mut self) {
                if let Some(unknown) = self.0.take() {
                    unknown.forget();
                }
            }
        }
    };
}
original_charge!(TaskCharge);
original_charge!(OriginalCharge);

impl Default for DispatcherCapacity {
    fn default() -> Self {
        Self::from_limits(&EventDispatcherConfig::default())
    }
}
impl DispatcherCapacity {
    pub(crate) fn from_api(api: &ApiConfig) -> Self {
        let limits = Self::effective_limits(api).unwrap_or((0, 0));
        Self {
            tasks: Arc::new(Semaphore::new(limits.0.min(Semaphore::MAX_PERMITS))),
            originals: Arc::new(Semaphore::new(limits.1.min(Semaphore::MAX_PERMITS))),
        }
    }
    fn effective_limits(api: &ApiConfig) -> io::Result<(usize, usize)> {
        let active = !api.event_sinks.is_empty() || api.dead_letter_path.is_some();
        let jsonl = api
            .event_sinks
            .iter()
            .filter(|sink| matches!(sink, EventSinkConfig::JsonLines { .. }))
            .count();
        let task_floor = if active {
            api.event_sinks
                .len()
                .checked_add(2)
                .and_then(|n| n.checked_add(usize::from(jsonl != 0)))
                .ok_or_else(|| io::Error::other("dispatcher task layout overflow"))?
        } else {
            1
        };
        // Same concrete providers: each persistent file has its lock; the
        // journal also owns one SQLite-with-scratch original and compaction
        // needs one temporary file without closing the live journal first.
        let original_floor = if active {
            jsonl
                .checked_mul(2)
                .and_then(|n| n.checked_add(2 * usize::from(api.dead_letter_path.is_some())))
                .and_then(|n| n.checked_add(4 * usize::from(api.outbox_path.is_some())))
                .ok_or_else(|| io::Error::other("dispatcher original layout overflow"))?
        } else {
            0
        };
        let tasks = api
            .dispatcher
            .max_worker_tasks
            .unwrap_or_else(|| api.dispatcher.effective_max_worker_tasks().max(task_floor));
        let originals = api.dispatcher.max_open_originals.unwrap_or_else(|| {
            api.dispatcher
                .effective_max_open_originals()
                .max(original_floor)
        });
        if tasks < task_floor || originals < original_floor {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "dispatcher limits cannot hold its actual configured receivers/providers and one progress position"));
        }
        Ok((tasks, originals))
    }
    pub(crate) fn from_limits(config: &EventDispatcherConfig) -> Self {
        // Validation runs before starting this owner's first actual job. A
        // malformed standalone input gets a sealed cold inventory, not a
        // default unlimited fallback or one uncharged initializer.
        let tasks = config.effective_max_worker_tasks();
        let originals = config.effective_max_open_originals();
        Self {
            tasks: Arc::new(Semaphore::new(if tasks <= Semaphore::MAX_PERMITS {
                tasks
            } else {
                0
            })),
            originals: Arc::new(Semaphore::new(if originals <= Semaphore::MAX_PERMITS {
                originals
            } else {
                0
            })),
        }
    }
    pub(crate) fn validate(api: &ApiConfig) -> io::Result<()> {
        let (tasks, originals) = Self::effective_limits(api)?;
        if tasks == 0
            || tasks > Semaphore::MAX_PERMITS
            || originals == 0
            || originals > Semaphore::MAX_PERMITS
        {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "dispatcher task and original limits must be positive representable semaphore counts"));
        }
        // Receivers remain live while their publishers run. Keep at least one
        // actual publisher position, so a legal finite cap cannot deadlock all
        // receiver jobs while they wait to enter the same existing inventory.
        let active = !api.event_sinks.is_empty() || api.dead_letter_path.is_some();
        let publisher = api
            .event_sinks
            .iter()
            .any(|sink| matches!(sink, EventSinkConfig::JsonLines { .. }));
        let minimum = if active {
            api.event_sinks
                .len()
                .checked_add(2)
                .and_then(|count| count.checked_add(usize::from(publisher)))
                .ok_or_else(|| io::Error::other("dispatcher task layout overflow"))?
        } else {
            1
        };
        if tasks < minimum {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                format!("api.dispatcher.max_worker_tasks must allow {minimum} actual receiver/source/initializer/publisher positions")));
        }
        Ok(())
    }
    pub(crate) fn task(&self) -> io::Result<TaskCharge> {
        self.tasks
            .clone()
            .try_acquire_owned()
            .map(|permit| TaskCharge(Some(permit)))
            .map_err(|_| capacity("dispatcher worker task"))
    }
    pub(crate) async fn publisher(&self) -> io::Result<TaskCharge> {
        self.tasks
            .clone()
            .acquire_owned()
            .await
            .map(|permit| TaskCharge(Some(permit)))
            .map_err(|_| capacity("dispatcher publisher"))
    }
    pub(crate) fn original(&self) -> io::Result<OriginalCharge> {
        self.originals
            .clone()
            .try_acquire_owned()
            .map(|permit| OriginalCharge(Some(permit)))
            .map_err(|_| capacity("dispatcher original"))
    }
}
fn capacity(kind: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("actual {kind} limit exhausted"),
    )
}

#[cfg(test)]
mod tests;
