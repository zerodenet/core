//! One retained target and exact retirement subset, installed before any await.
use super::*;
pub(super) mod order;
mod publication;
use std::collections::BTreeSet;
use zero_core::component::ComponentInstanceId;

struct Part {
    plan: Arc<ComponentPlan>,
    instances: BTreeSet<ComponentInstanceId>,
}
/// This terminal contains no slots, recipes, configuration or Engine owners.
/// Complete slot reports include lifecycle, transferred-user, task and original
/// provider proof. A missing or Unknown native proof never seals this terminal.
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
    // The whole diff seals commit ownership; the retirement parts additionally
    // contain affected shared/native owners and their transitive consumers.
    affected_current: Box<[ComponentInstanceId]>,
    parts: Vec<Part>,
    order: Vec<ComponentInstanceId>,
    dependencies: BTreeMap<ComponentInstanceId, BTreeSet<ComponentInstanceId>>,
    terminal: OnceLock<Arc<DeclaredComponentTerminal>>,
    released: Mutex<publication::Publication>,
}
impl Replacement {
    pub(super) fn new(
        candidate: Arc<ComponentPlan>,
        current: Option<&Arc<ComponentPlan>>,
        old: impl IntoIterator<Item = Arc<ComponentPlan>>,
    ) -> Result<Self, ChannelError> {
        let affected_current: Box<[ComponentInstanceId]> = current
            .into_iter()
            .flat_map(|plan| &plan.order)
            .filter(|slot| !candidate.order.iter().any(|next| Arc::ptr_eq(slot, next)))
            .map(|slot| slot.instance_id())
            .collect();
        // Resource-free protocol settings may leave their transferred sessions
        // with the session owner. Rebuilding shared/native state must first
        // close every dependent user, even if that consumer owns no native I/O.
        let mut stop_current = BTreeSet::new();
        if let Some(current) = current {
            loop {
                let before = stop_current.len();
                for slot in &current.order {
                    let spec = slot.recipe.spec();
                    if !candidate.order.iter().any(|next| Arc::ptr_eq(slot, next))
                        && (spec.roles.is_empty()
                            || !spec.resources.is_empty()
                            || spec.task_limit.is_some()
                            || current
                                .borrowing
                                .loan_snapshot()
                                .1
                                .iter()
                                .any(|(_, donor)| *donor == slot.instance_id())
                            || current
                                .retirement_dependencies(slot)
                                .iter()
                                .any(|id| stop_current.contains(id)))
                    {
                        stop_current.insert(slot.instance_id());
                    }
                }
                if stop_current.len() == before {
                    break;
                }
            }
            if candidate.order.iter().any(|slot| {
                slot.dependency_instances()
                    .iter()
                    .any(|id| stop_current.contains(id))
            }) {
                return Err(invalid(
                    Reason::StalePlan,
                    "candidate retained a dependency selected for retirement",
                ));
            }
        }
        let mut seen: BTreeMap<ComponentInstanceId, Arc<ComponentSlot>> = BTreeMap::new();
        let mut parts = Vec::new();
        for plan in current.into_iter().cloned().chain(old) {
            if Arc::ptr_eq(&plan, &candidate) {
                continue;
            }
            let mut instances = BTreeSet::new();
            for slot in &plan.order {
                if candidate.order.iter().any(|keep| Arc::ptr_eq(keep, slot))
                    || current.is_some_and(|active| {
                        active.order.iter().any(|keep| Arc::ptr_eq(keep, slot))
                            && !stop_current.contains(&slot.instance_id())
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
        let loans = candidate.borrowing.loan_snapshot().1;
        let dependencies = seen
            .iter()
            .map(|(id, slot)| {
                (
                    *id,
                    slot.dependency_instances()
                        .iter()
                        .copied()
                        .chain(
                            loans
                                .iter()
                                .filter(|(from, _)| from == id)
                                .map(|(_, to)| *to),
                        )
                        .collect(),
                )
            })
            .collect();
        let order = order::close_order(&seen, &loans)?;
        Ok(Self {
            candidate,
            affected_current,
            parts,
            order,
            dependencies,
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
        let mut blocked: BTreeSet<ComponentInstanceId> = BTreeSet::new();
        for identity in &self.order {
            let part = self
                .parts
                .iter()
                .find(|part| part.instances.contains(identity))
                .unwrap();
            if blocked.contains(identity) {
                blocked.extend(self.dependencies[identity].iter());
                continue;
            }
            let exact = BTreeSet::from([*identity]);
            if let Err(error) = part.plan.confirm_instances(&exact, deadline).await {
                blocked.extend(self.dependencies[identity].iter());
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
