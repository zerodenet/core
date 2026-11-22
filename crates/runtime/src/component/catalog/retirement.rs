//! Declared completion and sticky task faults are different retained facts.
//! Native declarations remain unconfirmed without provider-level physical proof.
mod receive;
use super::*;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::collections::BTreeSet;
use zero_core::component::ComponentInstanceId;

#[cfg(feature = "raw-ip-runtime")]
impl ComponentCatalog {
    /// Published device originals can also be retained by an external
    /// Source or the device inventory. Close their catalog borrowers before
    /// those owners receive the global stop request.
    pub async fn shutdown_device_borrowers(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        self.drive_supervised(async {
            let _transition = tokio::time::timeout_at(deadline, self.transition.lock())
                .await
                .map_err(|_| invalid(Reason::Timeout, "component transition remains owned"))?;
            let plans = self.shutdown_plans();
            if plans.iter().all(|plan| plan.deferred_devices.is_empty()) {
                return Ok(());
            }
            self.admission.freeze_loans().retain();
            for plan in plans {
                plan.shutdown_device_borrowers(deadline).await?;
            }
            Ok(())
        })
        .await
    }
}

impl ComponentPlan {
    #[cfg(feature = "raw-ip-runtime")]
    async fn shutdown_device_borrowers(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let loans = self.borrowing.loan_snapshot().1;
        // A loan edge to a deferred device was recorded only after its
        // actual carrier provider admitted the generation.
        let mut consumers = loans
            .iter()
            .filter(|(_, donor)| {
                self.deferred_devices.contains(&donor.component)
                    && self.order.iter().any(|slot| slot.instance_id() == *donor)
            })
            .map(|(consumer, _)| *consumer)
            .filter(|consumer| {
                self.order
                    .iter()
                    .any(|slot| slot.instance_id() == *consumer)
            })
            .collect::<BTreeSet<_>>();
        if consumers.is_empty() {
            return Ok(());
        }
        loop {
            let before = consumers.len();
            for slot in &self.order {
                if self
                    .retirement_dependencies(slot)
                    .iter()
                    .any(|dependency| consumers.contains(dependency))
                {
                    consumers.insert(slot.instance_id());
                }
            }
            if consumers.len() == before {
                break;
            }
        }
        self.confirm_instances(&consumers, deadline).await
    }

    /// The slot combines actual protocol/task receipts with declaration-wide
    /// provider evidence before returning a complete report. Unknown native or
    /// transferred users therefore remain charged here.
    pub(super) async fn confirm_instances(
        &self,
        instances: &BTreeSet<ComponentInstanceId>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let _closing = tokio::time::timeout_at(deadline, self.closing.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "component close collection remains owned"))?;
        let nodes: BTreeMap<_, _> = self
            .order
            .iter()
            .filter(|slot| instances.contains(&slot.instance_id()))
            .map(|slot| (slot.instance_id(), slot.clone()))
            .collect();
        let ordered =
            super::replacement::order::close_order(&nodes, &self.borrowing.loan_snapshot().1)?;
        if nodes.len() != instances.len() {
            return Err(invalid(
                Reason::InvalidConfig,
                "retirement contains an unknown owner",
            ));
        }
        for slot in nodes.values() {
            slot.fence_retirement();
        }
        let mut pending = None;
        let mut blocked: BTreeSet<ComponentInstanceId> = BTreeSet::new();
        for group in ordered {
            if group.iter().any(|id| blocked.contains(id)) {
                for id in &group {
                    blocked.extend(self.retirement_dependencies(&nodes[id]));
                }
                continue;
            }
            let mut closing = FuturesUnordered::new();
            for identity in &group {
                if self
                    .confirmed_declared_close
                    .lock()
                    .expect("component close proof poisoned")
                    .contains(identity)
                {
                    continue;
                }
                let slot = nodes[identity].clone();
                closing.push(async move {
                    let result = slot.shutdown(deadline).await;
                    // Archive inside the awaited original, before it becomes a
                    // completed stream item that an observer could discard.
                    self.receive_instance_close(&slot, result)
                });
            }
            let mut incomplete = false;
            while let Some(result) = closing.next().await {
                if let Err(error) = result {
                    incomplete = true;
                    pending.get_or_insert(error);
                }
            }
            if incomplete {
                for id in &group {
                    blocked.extend(self.retirement_dependencies(&nodes[id]));
                }
            }
        }
        pending.map_or(Ok(()), Err)
    }

    pub(super) fn confirmed(&self, instances: &BTreeSet<ComponentInstanceId>) -> bool {
        let closed = self
            .confirmed_declared_close
            .lock()
            .expect("component close proof poisoned");
        instances.is_subset(&closed)
    }

    pub(super) fn retirement_faults(
        &self,
        instances: &BTreeSet<ComponentInstanceId>,
    ) -> Vec<Arc<plan::PlanFailure>> {
        self.failures
            .lock()
            .expect("component failures poisoned")
            .values()
            .filter(|failure| instances.contains(&failure.instance()))
            .cloned()
            .collect()
    }
}
