//! Pause actual retired user roots without revoking live I/O or claiming closure.
use super::*;

/// Retained by the real replacement transaction until commit or rollback.
/// Dropping it only restores admission; normal StopOld still proves closure.
pub struct ReplacementUsersPause {
    _roots: Vec<crate::component::instance::PausedUsers>,
}

impl ComponentCatalog {
    /// Original cleanup custody only. A staged Engine snapshot must not erase
    /// the actual retained pool's ownership proof. This grants no role/IO use.
    pub fn validate_owned_carrier_adoption(
        &self,
        role: zero_core::component::RoleInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let plans = {
            let state = self.state.lock().expect("component catalog poisoned");
            state
                .current
                .iter()
                .chain(state.staged.iter())
                .chain(state.cold.iter())
                .filter_map(|entry| entry.plan.as_ref().ok())
                .chain(state.retired.iter())
                .cloned()
                .collect::<Vec<_>>()
        };
        // Actual provider/slot/Scope inspection and final Arc destructors occur
        // outside the catalog mutex. Public IDs alone cannot match an owner.
        if plans.iter().any(|plan| {
            if plan.validate_carrier_adoption(role, adoption).is_err() {
                return false;
            }
            let Some(slot) = plan.component(adoption.instance().component) else {
                return false;
            };
            slot.instance_id() == adoption.instance()
                && slot
                    .recipe
                    .spec()
                    .resources
                    .iter()
                    .enumerate()
                    .any(|(ordinal, _)| {
                        slot.original_carrier_owner(ordinal).is_some_and(|owner| {
                            owner.matches_adoption(adoption)
                                && owner.bound().is_some_and(|bound| {
                                    slot.matches_live_scope(&bound.scope_identity())
                                })
                        })
                    })
        }) {
            Ok(())
        } else {
            Err(invalid(
                Reason::StalePlan,
                "original carrier does not belong to an actual retained catalog role",
            ))
        }
    }
    /// Exact old instances in this catalog which this owned candidate will
    /// replace. This is a read-only graph fact, never permission to close IO.
    pub fn replacement_instances(
        &self,
        candidate: &Arc<ComponentPlan>,
    ) -> Result<Vec<zero_core::component::ComponentInstanceId>, ChannelError> {
        let old = {
            let state = self.state.lock().expect("component catalog poisoned");
            let owned = state
                .current
                .iter()
                .chain(state.cold.iter())
                .chain(state.staged.iter())
                .filter_map(|entry| entry.plan.as_ref().ok())
                .any(|plan| Arc::ptr_eq(plan, candidate));
            if !owned {
                return Err(invalid(
                    Reason::StalePlan,
                    "replacement candidate is not owned by this catalog",
                ));
            }
            state
                .current
                .as_ref()
                .and_then(|entry| entry.plan.as_ref().ok())
                .cloned()
        };
        Ok(old
            .into_iter()
            .flat_map(|plan| {
                plan.order
                    .iter()
                    .filter(|slot| !candidate.order.iter().any(|next| Arc::ptr_eq(slot, next)))
                    .map(|slot| slot.instance_id())
                    .collect::<Vec<_>>()
            })
            .collect())
    }
    pub fn try_pause_replacement_users(
        &self,
        candidate: &Arc<ComponentPlan>,
    ) -> Result<ReplacementUsersPause, ChannelError> {
        let old = {
            let state = self.state.lock().expect("component catalog poisoned");
            let owned = state
                .current
                .iter()
                .chain(state.cold.iter())
                .chain(state.staged.iter())
                .filter_map(|entry| entry.plan.as_ref().ok())
                .any(|plan| Arc::ptr_eq(plan, candidate));
            if !owned {
                return Err(invalid(
                    Reason::StalePlan,
                    "replacement candidate is not owned by this catalog",
                ));
            }
            state
                .current
                .as_ref()
                .and_then(|entry| entry.plan.as_ref().ok())
                .cloned()
        };
        let mut roots = Vec::new();
        if let Some(old) = old {
            for slot in &old.order {
                if !candidate.order.iter().any(|next| Arc::ptr_eq(slot, next)) {
                    // Each transferred dependent also leases its real dependency
                    // roots. No guessed Session count or blanket host lock.
                    roots.push(slot.own_user_root().try_pause()?);
                }
            }
        }
        Ok(ReplacementUsersPause { _roots: roots })
    }
}
