//! One retained target and exact retirement subset, installed before any await.
use super::*;
mod order;
mod publication;
use std::collections::BTreeSet;
use zero_core::component::ComponentInstanceId;

struct Part {
    plan: Arc<ComponentPlan>,
    instances: BTreeSet<ComponentInstanceId>,
}
/// This terminal contains no slots, recipes, configuration or Engine owners.
/// It confirms only task/lifecycle owners without native declarations. Native
/// declarations and transferred users require separate provider/user proofs.
#[derive(Debug)]
pub(super) struct DeclaredComponentTerminal {
    instances: Box<[ComponentInstanceId]>,
    failures: Box<[Arc<plan::PlanTaskFailure>]>,
}
impl DeclaredComponentTerminal {
    pub(super) fn failure(&self) -> Option<&Arc<plan::PlanTaskFailure>> {
        self.failures().first()
    }
    pub(super) fn instances(&self) -> &[ComponentInstanceId] {
        &self.instances
    }
    pub(super) fn failures(&self) -> &[Arc<plan::PlanTaskFailure>] {
        &self.failures
    }
}
/// Only this retirement can issue or consume its non-Clone capability. A waiter
/// may be cancelled and receive another capability for the same terminal nonce.
pub(super) struct DeclaredCloseReceipt(Arc<DeclaredComponentTerminal>);

pub(super) struct Replacement {
    pub(super) candidate: Arc<ComponentPlan>,
    // Planned only: start still does not close changed current or enable shared
    // production. Future StopOld must also confirm dependent transferred users.
    affected_current: Box<[ComponentInstanceId]>,
    parts: Vec<Part>,
    order: Vec<ComponentInstanceId>,
    terminal: OnceLock<Arc<DeclaredComponentTerminal>>,
    released: Mutex<publication::Publication>,
}
impl Replacement {
    pub(super) fn new(
        candidate: Arc<ComponentPlan>,
        current: Option<&Arc<ComponentPlan>>,
        old: impl IntoIterator<Item = Arc<ComponentPlan>>,
    ) -> Result<Self, ChannelError> {
        let affected_current = current
            .into_iter()
            .flat_map(|plan| &plan.order)
            .filter(|slot| !candidate.order.iter().any(|next| Arc::ptr_eq(slot, next)))
            .map(|slot| slot.instance_id())
            .collect();
        let mut seen: BTreeMap<ComponentInstanceId, Arc<ComponentSlot>> = BTreeMap::new();
        let mut parts = Vec::new();
        for plan in old {
            if Arc::ptr_eq(&plan, &candidate) {
                continue;
            }
            let mut instances = BTreeSet::new();
            for slot in &plan.order {
                if candidate.order.iter().any(|keep| Arc::ptr_eq(keep, slot))
                    || current.is_some_and(|active| {
                        active.order.iter().any(|keep| Arc::ptr_eq(keep, slot))
                    })
                {
                    continue;
                }
                if let Some(known) = seen.get(&slot.instance_id()) {
                    if !Arc::ptr_eq(known, slot) {
                        return Err(invalid(
                            Reason::InvalidConfig,
                            "different owners repeated a retiring instance identity",
                        ));
                    }
                    continue;
                }
                seen.insert(slot.instance_id(), slot.clone());
                instances.insert(slot.instance_id());
            }
            if !instances.is_empty() {
                parts.push(Part { plan, instances });
            }
        }
        let order = order::close_order(&seen)?;
        Ok(Self {
            candidate,
            affected_current,
            parts,
            order,
            terminal: OnceLock::new(),
            released: Mutex::new(publication::Publication::Pending),
        })
    }
    pub(super) fn owned_plans(&self) -> impl Iterator<Item = &Arc<ComponentPlan>> {
        std::iter::once(&self.candidate).chain(self.parts.iter().map(|part| &part.plan))
    }
    pub(super) fn owned_ids(&self) -> impl Iterator<Item = ComponentId> + '_ {
        self.owned_plans()
            .flat_map(|plan| plan.order.iter().map(|slot| slot.id))
    }
    pub(super) fn affected_current(&self) -> &[ComponentInstanceId] {
        &self.affected_current
    }
    pub(super) fn matches_current(&self, current: Option<&Arc<ComponentPlan>>) -> bool {
        let actual: Vec<_> = current
            .into_iter()
            .flat_map(|plan| &plan.order)
            .filter(|slot| {
                !self
                    .candidate
                    .order
                    .iter()
                    .any(|next| Arc::ptr_eq(slot, next))
            })
            .map(|slot| slot.instance_id())
            .collect();
        self.affected_current() == actual.as_slice()
    }
    pub(super) async fn confirm(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<DeclaredCloseReceipt, ChannelError> {
        // Cancel every retained old owner before any one shutdown can await.
        for part in &self.parts {
            for slot in &part.plan.order {
                if part.instances.contains(&slot.instance_id()) {
                    slot.revoke();
                }
            }
        }
        let mut pending = None;
        for identity in &self.order {
            let part = self
                .parts
                .iter()
                .find(|part| part.instances.contains(identity))
                .unwrap();
            let exact = BTreeSet::from([*identity]);
            if let Err(error) = part.plan.confirm_instances(&exact, deadline).await {
                pending.get_or_insert(error);
            }
        }
        if let Some(error) = pending {
            return Err(error);
        }
        if self
            .parts
            .iter()
            .any(|part| !part.plan.confirmed(&part.instances))
        {
            return Err(invalid(
                Reason::Timeout,
                "replacement declared closure remains unconfirmed",
            ));
        }
        let terminal = self.terminal.get_or_init(|| {
            let mut failures = BTreeMap::new();
            let mut instances = BTreeSet::new();
            for part in &self.parts {
                instances.extend(part.instances.iter().copied());
                for failure in part.plan.task_faults(&part.instances) {
                    if !failure.reported() {
                        failures.entry(failure.0.id).or_insert(failure);
                    }
                }
            }
            Arc::new(DeclaredComponentTerminal {
                instances: instances.into_iter().collect(),
                failures: failures.into_values().collect(),
            })
        });
        Ok(DeclaredCloseReceipt(terminal.clone()))
    }
}

#[cfg(test)]
#[path = "../tests/replacement.rs"]
mod tests;
