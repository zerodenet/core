//! Close real transitive borrowers before their original Source loses native IO.
use super::*;
use std::collections::BTreeSet;
use zero_core::component::ComponentInstanceId;
impl ComponentCatalog {
    fn accepted_plans(&self) -> Vec<Arc<ComponentPlan>> {
        let state = self.state.lock().expect("component catalog poisoned");
        let mut plans = Vec::new();
        for plan in state
            .current
            .iter()
            .chain(state.cold.iter())
            .chain(state.staged.iter())
            .filter_map(|entry| entry.plan.as_ref().ok())
            .chain(state.retired.iter())
        {
            if !plans.iter().any(|old| Arc::ptr_eq(old, plan)) {
                plans.push(plan.clone());
            }
        }
        plans
    }
    /// Actual sources retained by providers that this candidate replaces.
    /// No tag/config projection or cold declaration invents an original.
    pub fn retiring_accepted_sources(
        &self,
        candidate: &Arc<ComponentPlan>,
    ) -> Vec<ComponentInstanceId> {
        let mut sources = BTreeSet::new();
        for plan in self.accepted_plans() {
            if Arc::ptr_eq(&plan, candidate) {
                continue;
            }
            for slot in &plan.order {
                if candidate.order.iter().any(|next| Arc::ptr_eq(next, slot)) {
                    continue;
                }
                if let Some(provider) = slot.accepted_carrier_provider() {
                    sources.extend(provider.sources());
                }
            }
        }
        sources.into_iter().collect()
    }
    /// Existing Source main remains alive and consumes its original accepted
    /// close receipts. Called only inside a persisted StopOld transaction.
    pub async fn retire_accepted_source_originals(
        &self,
        source: ComponentInstanceId,
        end: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        self.drain_accepted_source(source, end).await?;
        loop {
            let plans = self.accepted_plans();
            if plans.iter().all(|plan| {
                plan.order.iter().all(|slot| {
                    slot.accepted_carrier_provider()
                        .is_none_or(|provider| provider.source_originals_returned(source))
                })
            }) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= end {
                return Err(invalid(
                    Reason::Timeout,
                    "actual accepted Source original return remains pending",
                ));
            }
            tokio::time::sleep_until(
                end.min(tokio::time::Instant::now() + std::time::Duration::from_millis(10)),
            )
            .await;
        }
    }
    /// Synchronous admission fence only; it never cancels a Source or certifies
    /// that a borrowed view, protocol driver, native original or Scope closed.
    pub fn fence_accepted_source(&self, source: ComponentInstanceId) -> bool {
        let mut present = false;
        for plan in self.accepted_plans() {
            for slot in &plan.order {
                if let Some(provider) = slot.accepted_carrier_provider() {
                    provider.fence_source(source);
                    present = true;
                }
            }
        }
        present
    }
    pub async fn drain_accepted_source(
        &self,
        source: ComponentInstanceId,
        end: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        self.fence_accepted_source(source);
        self.drive_supervised(async {
            for plan in self.accepted_plans() {
                plan.drain_accepted_source(source, end).await?;
            }
            Ok(())
        })
        .await
    }
    /// The external Source invokes this only after its real terminal receipt.
    pub fn release_accepted_source(&self, source: ComponentInstanceId) {
        for plan in self.accepted_plans() {
            for slot in &plan.order {
                if let Some(provider) = slot.accepted_carrier_provider() {
                    provider.release_source(source);
                }
            }
        }
    }
}
impl ComponentPlan {
    async fn drain_accepted_source(
        &self,
        source: ComponentInstanceId,
        end: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let roots = self
            .order
            .iter()
            .filter_map(|slot| {
                slot.accepted_carrier_provider()
                    .filter(|provider| provider.fence_source(source))
                    .map(|provider| (slot.clone(), provider))
            })
            .collect::<Vec<_>>();
        let root_ids = roots
            .iter()
            .map(|(slot, _)| slot.instance_id())
            .collect::<BTreeSet<_>>();
        if roots.is_empty() {
            return Ok(());
        }
        let mut affected = root_ids.clone();
        loop {
            let old = affected.len();
            for slot in &self.order {
                if self
                    .retirement_dependencies(slot)
                    .iter()
                    .any(|id| affected.contains(id))
                {
                    affected.insert(slot.instance_id());
                }
            }
            if old == affected.len() {
                break;
            }
        }
        let consumers = affected.difference(&root_ids).copied().collect();
        // An enclosing replacement may already hold closing while awaiting this
        // Source. Its exact consumer receipts avoid re-entering that same lock.
        if !self.confirmed(&consumers) {
            self.confirm_instances(&consumers, end).await?;
        }
        // Admission cancellation now fences direct logical users only. No
        // lifecycle revoke, connection.close or Source task cancellation yet.
        for (slot, _) in &roots {
            slot.revoke();
        }
        loop {
            if roots
                .iter()
                .all(|(_, provider)| provider.source_views_returned(source))
            {
                for (_, provider) in &roots {
                    provider.stop_source_originals(source, end).await?;
                }
                return Ok(());
            }
            if tokio::time::Instant::now() >= end {
                return Err(invalid(
                    Reason::Timeout,
                    "original accepted Source still has transferred logical IO",
                ));
            }
            tokio::time::sleep_until(
                end.min(tokio::time::Instant::now() + std::time::Duration::from_millis(10)),
            )
            .await;
        }
    }
}
