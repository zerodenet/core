use zero_config::OutboundRuntimeKind;

use super::{Engine, EngineRuntimeSnapshot, TargetId};
use crate::plan::TargetKind;

impl Engine {
    /// Inspect candidate eligibility without resolving a path: resolution
    /// consumes load-balance rotation or reserves passive half-open attempts.
    pub(in crate::runtime) fn urltest_member_available(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        target_id: TargetId,
    ) -> bool {
        let Some(target) = snapshot.plan().target(target_id) else {
            return false;
        };
        let available = |id| self.urltest_member_available(snapshot, id);
        match target.kind() {
            TargetKind::Outbound(outbound) => {
                crate::EndpointAdmission::from_snapshot(snapshot)
                    .outbound_denial(target.tag())
                    .is_none()
                    && (outbound.runtime_kind() != OutboundRuntimeKind::Proxy
                        || self.check_outbound_health(target.tag()).is_ok())
            }
            TargetKind::Selector(selector) => available(
                snapshot
                    .outbound_group_state
                    .selector_selected_target(target_id)
                    .unwrap_or_else(|| selector.initial_member()),
            ),
            TargetKind::Fallback(fallback) => fallback.members().iter().copied().any(available),
            TargetKind::LoadBalance(balance) => balance.members().iter().copied().any(available),
            TargetKind::UrlTest(urltest) => urltest.members().iter().copied().any(available),
            // Carrier observations are attributed to the first relay hop.
            TargetKind::Relay(relay) => relay.chain().first().copied().is_some_and(available),
        }
    }
}
