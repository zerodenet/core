//! Pure exact native-root custody, issued only after its unique driver graph.
use super::*;
use crate::{
    controlled_carrier::{CarrierProtocolRetirement, ProtocolOriginalReceiver},
    controlled_datagram::metadata::MetadataViewsRetained,
};
#[derive(Clone)]
pub struct MetadataDriverRetained {
    slot: Weak<Slot>,
    owner: MetadataPoolRegistration,
    views: MetadataViewsRetained,
    protocol: TaskJoinWitness,
}
impl MetadataDriverRetained {
    pub fn instance(&self) -> ComponentInstanceId {
        self.owner.instance()
    }
    pub fn scope_identity(&self) -> crate::contract::scope::TaskScopeIdentity {
        self.protocol.scope_identity()
    }
    pub fn packet_driver(&self) -> &TaskJoinWitness {
        self.views.driver()
    }
    pub fn protocol_driver(&self) -> &TaskJoinWitness {
        &self.protocol
    }
    pub(crate) fn names(&self, original: &MetadataPoolOrigin) -> bool {
        self.instance() == original.instance && self.slot.ptr_eq(&original.slot)
    }
}
impl MetadataPoolOrigin {
    /// Historical receipt of this exact original's actual cached receiver.
    /// It never certifies NativeClosed, user return or Scope/Host ACK.
    pub fn retained_protocol_receiver(&self) -> Option<ProtocolOriginalReceiver> {
        let slot = self.slot.upgrade()?;
        let phase = {
            slot.protocol
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }?;
        phase
            .receiver()
            .filter(|receipt| receipt.names_metadata(self))
    }
    pub fn retained_driver(&self) -> Option<MetadataDriverRetained> {
        let slot = self.slot.upgrade()?;
        let phase = {
            slot.protocol
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }?;
        phase.receiver().and_then(|proof| proof.metadata_for(self))
    }
    /// Pure handoff, never native close. Root already holds the exact control;
    /// only its actual original/Scope/claim and live driver graph may publish.
    pub fn prepare_protocol_receiver(
        &self,
        views: MetadataViewsRetained,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<crate::controlled_carrier::ProtocolReceiverPrepared, ChannelError> {
        let protocol = views.protocol_driver().clone();
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "original metadata Root disappeared"))?;
        let owner = slot
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "original metadata owner disappeared"))?;
        let phase = {
            slot.protocol
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
        .ok_or_else(|| {
            error(
                Reason::InvalidConfig,
                "original has no persistent protocol control",
            )
        })?;
        let bound = {
            owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .bound
                .clone()
        };
        let scoped = bound.as_ref().is_some_and(|bound| {
            bound.scope.same_scope(&protocol.scope_identity())
                && bound.scope.resource_open()
                && !bound.host_cancel.is_cancelled()
        });
        let live = scoped
            && !protocol.is_joined()
            && !views.driver().is_joined()
            && protocol
                .scope_identity()
                .same_scope(&views.driver().scope_identity());
        let native = slot.native()?;
        let valid = live
            && slot.root_bound
            && retirement.matches_phase(&phase)
            && slot.closed.get().is_none()
            && native.transferred
            && native.control.is_some()
            && native
                .issuer
                .as_ref()
                .is_some_and(|issuer| issuer.validates_receiver(&views));
        drop(native);
        if !valid {
            return Err(error(
                Reason::StalePlan,
                "foreign original metadata receiver/driver graph",
            ));
        }
        let retained = MetadataDriverRetained {
            slot: Arc::downgrade(&slot),
            owner: self.owner.clone(),
            views,
            protocol,
        };
        Ok(crate::controlled_carrier::ProtocolReceiverPrepared::new(
            &phase,
            ProtocolOriginalReceiver::metadata(retained),
        ))
    }
}
