//! Same actual Source record receives only its sealed ordered-graph originals.
use super::*;
use crate::controlled_carrier::ProtocolOriginal;
impl Record {
    pub(super) fn validates_protocol(&self, original: &ProtocolOriginal) -> bool {
        let Some((targets, selected)) = &self.protocol_targets else {
            return false;
        };
        if original.binding().network_generation != self.binding.network_generation {
            return false;
        }
        self.binding
            .prefix
            .iter()
            .chain(std::iter::once(selected))
            .enumerate()
            .any(|(index, role)| {
                let Some(target) = targets.iter().find(|target| target.role == *role) else {
                    return false;
                };
                if target.instance != original.instance()
                    || original.binding().dial != target.binding.dial
                    || original.binding().prefix != self.binding.prefix[..index]
                {
                    return false;
                }
                if let Some(metadata) = original.metadata() {
                    target.metadata.iter().any(|owner| {
                        owner.instance() == original.instance()
                            && owner.same_owner(metadata.owner())
                    })
                } else if let Some(adoption) = original.adoption() {
                    target.originals.iter().any(|owner| {
                        owner.instance() == original.instance()
                            && owner.matches_adoption(adoption)
                            && owner.bound().is_some_and(|bound| {
                                bound.scope_identity().same_scope(original.scope_identity())
                            })
                    })
                } else {
                    false
                }
            })
    }
    pub(super) fn receive_protocol(&self, original: ProtocolOriginal) -> Result<(), ChannelError> {
        let anchor = self
            .observer
            .get()
            .ok_or_else(|| failure(Reason::Revoked))?
            .pending_carrier_observer();
        if !original.matches_observer(&anchor)
            || !self.validates_protocol(&original)
            || self.call.cancel.is_cancelled()
            || self.caller.is_joined()
            || !self.caller.scope_identity().resource_open()
            || !self
                .caller
                .native_task_id()
                .is_some_and(|id| Some(id) == tokio::task::try_id())
        {
            return Err(failure(Reason::StalePlan));
        }
        self.protocols
            .as_ref()
            .ok_or_else(|| failure(Reason::StalePlan))?
            .retain_for_caller(original, self.caller.clone())
    }
}
