//! Pure exact native-root custody, issued only after its unique driver graph.
use super::*;
use crate::{
    controlled_carrier::{CarrierProtocolRetirement, ProtocolOriginalReceiver},
    controlled_datagram::metadata::MetadataViewsRetained,
};
#[derive(Clone)]
pub struct MetadataDriverRetained {
    slot: Weak<Slot>,
    wire: Weak<Slot>,
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
        self.instance() == original.instance() && self.slot.ptr_eq(&original.slot)
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
            wire: Arc::downgrade(&slot),
            owner: self.owner().clone(),
            views,
            protocol,
        };
        Ok(crate::controlled_carrier::ProtocolReceiverPrepared::new(
            &phase,
            ProtocolOriginalReceiver::metadata(retained),
        ))
    }
}

impl MetadataDriverRetained {
    pub(crate) fn wire_is_live(&self) -> bool {
        self.wire.upgrade().is_some_and(|wire| {
            wire.native().is_ok_and(|native| {
                native.transferred
                    && native.control.is_some()
                    && !native.wire_closed
                    && native
                        .issuer
                        .as_ref()
                        .is_some_and(|issuer| issuer.validates_receiver(&self.views))
            })
        })
    }
    pub(crate) fn same_handoff(&self, other: &Self) -> bool {
        self.slot.ptr_eq(&other.slot)
            && self.wire.ptr_eq(&other.wire)
            && self.views.driver().same_registration(other.views.driver())
            && self.protocol.same_registration(&other.protocol)
    }
    pub(crate) fn continues(&self, previous: &Self) -> bool {
        self.slot.ptr_eq(&previous.slot)
            && self.views.same_protocol_family(&previous.views)
            && self.protocol.same_registration(&previous.protocol)
            && !self.protocol.is_joined()
            && !self.views.driver().is_joined()
            && previous.views.driver().is_joined()
            && self.wire_is_live()
            && previous.wire.upgrade().is_some_and(|wire| {
                wire.native().is_ok_and(|native| {
                    native.wire_closed
                        && native
                            .report
                            .as_ref()
                            .is_some_and(|report| report.is_complete())
                })
            })
    }
}

impl MetadataDriverRetained {
    pub fn view_handoff(&self) -> &MetadataViewsRetained {
        &self.views
    }
}
impl MetadataPoolOrigin {
    /// Advance only this live protocol's actual receiver after its former wire
    /// has a native close report. The next wire belongs to the very same pool.
    pub fn prepare_protocol_continuation_receiver(
        &self,
        wire: &MetadataPoolOrigin,
        views: MetadataViewsRetained,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<crate::controlled_carrier::ProtocolReceiverPrepared, ChannelError> {
        let fail = || {
            error(
                Reason::StalePlan,
                "foreign or unclosed metadata continuation",
            )
        };
        let slot = self.slot.upgrade().ok_or_else(fail)?;
        let next = wire.slot.upgrade().ok_or_else(fail)?;
        if Arc::ptr_eq(&slot, &next) || !slot.root_bound || !next.root_bound {
            return Err(fail());
        }
        let owner = slot.owner.upgrade().ok_or_else(fail)?;
        let next_owner = next.owner.upgrade().ok_or_else(fail)?;
        if !Arc::ptr_eq(&owner, &next_owner) || owner.cancel.is_cancelled() {
            return Err(fail());
        }
        let phase = slot.protocol.lock().unwrap().clone().ok_or_else(fail)?;
        if retirement.retirement_requested() || !retirement.matches_phase(&phase) {
            return Err(fail());
        }
        let previous = phase.receiver().ok_or_else(fail)?;
        let actual = previous.metadata_for(self).ok_or_else(fail)?;
        let bound = owner.state.lock().unwrap().bound.clone().ok_or_else(fail)?;
        if !bound.scope.resource_open()
            || bound.host_cancel.is_cancelled()
            || !bound
                .scope
                .same_scope(&views.protocol_driver().scope_identity())
            || !actual.views.same_protocol_family(&views)
            || self.binding_snapshot() != wire.binding_snapshot()
        {
            return Err(fail());
        }
        {
            let native = slot.native()?;
            if !native
                .issuer
                .as_ref()
                .is_some_and(|issuer| issuer.validates_protocol_receiver(&views))
            {
                return Err(fail());
            }
        }
        let receiver = MetadataDriverRetained {
            slot: Arc::downgrade(&slot),
            wire: Arc::downgrade(&next),
            owner: self.owner().clone(),
            protocol: views.protocol_driver().clone(),
            views,
        };
        if !receiver.continues(&actual) {
            return Err(fail());
        }
        Ok(
            crate::controlled_carrier::ProtocolReceiverPrepared::metadata_continuation(
                &phase,
                previous,
                ProtocolOriginalReceiver::metadata(receiver),
            ),
        )
    }
}
