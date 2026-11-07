//! Pause actual retired user roots without revoking live I/O or claiming closure.
use super::*;
mod receiver;

/// Retained by the real replacement transaction until commit or rollback.
/// Dropping it only restores admission; normal StopOld still proves closure.
pub struct ReplacementUsersPause {
    pub(super) _roots: Vec<crate::component::instance::PausedUsers>,
    pub(super) candidate: std::sync::Weak<ComponentPlan>,
}
impl ReplacementUsersPause {
    /// This exact old generation was put under THIS actual candidate's user
    /// barrier. Observation selects a real fence only; it proves no close/IO.
    pub fn stops_instance(&self, instance: zero_core::component::ComponentInstanceId) -> bool {
        self.candidate.upgrade().is_some()
            && self
                ._roots
                .iter()
                .any(|root| root.actual_instance == Some(instance))
    }

    /// A Source preflight may classify only its exact stored flow loan whose
    /// cached receiver was proved to belong to this actual StopOld set. Counts
    /// remain charged; real view return, Source checkpoints and joins follow.
    pub fn retains_source_receiver(
        &self,
        observation: &zero_platform_tokio::controlled_carrier::loan::CarrierLoanReceiver,
        source: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        let Some(loan) = observation.retained() else {
            return false;
        };
        if !loan.retains_origin(source) {
            return false;
        }
        let Some(receiver) = loan.receiver_adoption() else {
            return false;
        };
        self.candidate.upgrade().is_some()
            && self._roots.iter().any(|root| {
                root.receivers()
                    .iter()
                    .any(|actual| actual.same_slot(&receiver))
            })
    }
}

impl ComponentCatalog {
    /// Original cleanup custody only. A staged Engine snapshot must not erase
    /// the actual retained pool's ownership proof. This grants no role/IO use.
    pub fn validate_owned_carrier_adoption(
        &self,
        role: zero_core::component::RoleInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.validate_owned_carrier_with(adoption, |plan| {
            plan.validate_carrier_adoption(role, adoption)
        })
    }
    /// Actual owning instance rather than a configured role: roleless cached
    /// pools still have the same retained Catalog Slot/provider/Scope.
    pub fn validate_owned_carrier_instance(
        &self,
        consumer: zero_core::component::ComponentInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.validate_owned_carrier_with(adoption, |plan| {
            plan.validate_carrier_instance(consumer, adoption)
        })
    }
    fn validate_owned_carrier_with(
        &self,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
        validate: impl Fn(&ComponentPlan) -> Result<(), ChannelError>,
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
            if validate(plan).is_err() {
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
            let stopping = old
                .order
                .iter()
                .filter(|slot| !candidate.order.iter().any(|next| Arc::ptr_eq(slot, next)))
                .cloned()
                .collect::<Vec<_>>();
            for slot in &stopping {
                let mut root = slot
                    .own_user_root()
                    .try_pause_receivers(|lease| receiver::retained(&old, slot, &stopping, lease))
                    .map_err(|mut source| {
                        source.component.get_or_insert(slot.id);
                        source
                    })?;
                root.actual_instance = Some(slot.instance_id());
                roots.push(root);
            }
        }
        Ok(ReplacementUsersPause {
            _roots: roots,
            candidate: Arc::downgrade(candidate),
        })
    }
}
