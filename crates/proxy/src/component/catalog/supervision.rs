//! One inline join consumer, shared by runtime, maintenance and nested callers.
use super::*;
use crate::component::instance::{ComponentTaskFault, WaitTarget};
use std::{
    future::{poll_fn, Future},
    task::Poll,
};

#[derive(Default)]
pub(in crate::component) struct InlineSupervisor(tokio::sync::Mutex<()>);

impl InlineSupervisor {
    pub(in crate::component) async fn drive<T>(
        &self,
        signal: &tokio::sync::Notify,
        mut scan: impl FnMut(&Arc<WaitTarget>),
        work: impl Future<Output = T>,
    ) -> T {
        let target = Arc::new(WaitTarget::default());
        tokio::pin!(work);
        let lease = self.0.lock();
        tokio::pin!(lease);
        let mut held = None;
        let mut changed = Box::pin(signal.notified());
        poll_fn(|cx| {
            target.register(cx);
            // A nested caller must poll its real work while the outer driver
            // owns the lease. Waiting exclusively for the lease deadlocks it.
            if held.is_none() {
                if let Poll::Ready(guard) = lease.as_mut().poll(cx) {
                    held = Some(guard);
                }
            }
            if held.is_some() {
                changed.as_mut().enable();
                if changed.as_mut().poll(cx).is_ready() {
                    changed.set(signal.notified());
                    changed.as_mut().enable();
                }
                scan(&target);
            }
            work.as_mut().poll(cx)
        })
        .await
    }
}

impl ComponentCatalog {
    fn first_task_fault(&self) -> Option<Arc<ComponentTaskFault>> {
        self.supervised_slots()
            .iter()
            .find_map(|slot| slot.task_fault())
    }
    pub(crate) async fn drive_supervised<T>(&self, work: impl Future<Output = T>) -> T {
        self.supervisor
            .drive(
                self.admission.signal(),
                |target| {
                    // This census is temporary. No permanent task/slot registry or
                    // historical configuration owner is kept by the driver.
                    for slot in self.supervised_slots() {
                        slot.poll_tasks(target);
                    }
                },
                work,
            )
            .await
    }

    fn supervised_slots(&self) -> Vec<Arc<ComponentSlot>> {
        let state = self.state.lock().expect("component catalog poisoned");
        let mut slots = BTreeMap::new();
        for plan in state
            .current
            .iter()
            .chain(state.cold.iter())
            .chain(state.staged.iter())
            .filter_map(|entry| entry.plan.as_ref().ok())
            .chain(state.retired.iter())
            .chain(
                state
                    .replacement
                    .iter()
                    .flat_map(|pending| pending.owned_plans()),
            )
        {
            for slot in &plan.order {
                if let Some(previous) = slots.insert(slot.instance_id(), slot.clone()) {
                    assert!(
                        Arc::ptr_eq(&previous, slot),
                        "different owners repeated a supervised identity"
                    );
                }
            }
        }
        slots.into_values().collect()
    }

    /// Return the same typed fault, without consuming a publication flag.
    /// Cancelling this waiter cannot erase it; its actual slot and charged exit
    /// remain owned until cleanup persists the original source and ACKs it.
    pub(crate) async fn next_failure(&self) -> ChannelError {
        loop {
            let changed = self.admission.signal().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(fault) = self.first_task_fault() {
                return fault.error();
            }
            changed.await;
        }
    }

    pub(crate) async fn scan_candidate(
        &self,
        config: &Arc<RuntimeConfig>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let plan = {
            let state = self.state.lock().expect("component catalog poisoned");
            state
                .staged
                .iter()
                .chain(state.current.iter())
                .find(|entry| entry.matches(config))
                .map(|entry| entry.ready().clone())
                .ok_or_else(|| {
                    invalid(
                        Reason::StalePlan,
                        "no installed component candidate to scan",
                    )
                })?
        };
        self.drive_supervised(plan.scan_tasks(deadline)).await
    }
}

#[cfg(test)]
pub(in crate::component) mod testing;
