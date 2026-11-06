//! Pause actual retired user roots without revoking live I/O or claiming closure.
use super::*;

/// Retained by the real replacement transaction until commit or rollback.
/// Dropping it only restores admission; normal StopOld still proves closure.
pub struct ReplacementUsersPause {
    _roots: Vec<crate::component::instance::PausedUsers>,
}

impl ComponentCatalog {
    pub fn try_pause_replacement_users(
        &self,
        candidate: &Arc<ComponentPlan>,
    ) -> Result<ReplacementUsersPause, ChannelError> {
        let old = {
            let state = self.state.lock().expect("component catalog poisoned");
            let owned = state.current.iter().chain(state.cold.iter()).chain(state.staged.iter())
                .filter_map(|entry| entry.plan.as_ref().ok())
                .any(|plan| Arc::ptr_eq(plan, candidate));
            if !owned {
                return Err(invalid(Reason::StalePlan, "replacement candidate is not owned by this catalog"));
            }
            state.current.as_ref().and_then(|entry| entry.plan.as_ref().ok()).cloned()
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
