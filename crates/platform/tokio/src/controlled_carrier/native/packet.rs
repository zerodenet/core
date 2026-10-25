use super::*;
use crate::{
    contract::component::ResourceDeclaration,
    controlled_datagram::{AcquisitionOutcome, ControlledDatagramOwner},
};
use std::{num::NonZeroUsize, sync::Mutex as SyncMutex};

impl NativeCarrierOpener {
    pub(super) fn packet(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        mut grant: CarrierGrant,
    ) -> Result<super::super::CarrierDatagram, ChannelError> {
        grant.check()?;
        let instance = grant.instance();
        let denied = |reason| {
            error(
                instance,
                Stage::Prepare,
                reason,
                DeliveryState::NotSubmitted,
            )
        };
        let registration = grant
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| denied(Reason::Revoked))?;
        let bound = registration
            .state()
            .binding
            .clone()
            .ok_or_else(|| denied(Reason::InvalidConfig))?;
        let declaration = ResourceDeclaration::ControlledDatagramCarrier {
            endpoint: destination.clone(),
            binding: binding.clone(),
            max_sockets: registration.limit,
        };
        if !bound.permits_native(binding, &destination, &context)
            || !(bound.matches(&declaration, registration.limit) || bound.pool_kind() == Some(true))
            || self.grantee != grant.instance()
            || binding != &self.binding
            || self.egress.generation() != binding.network_generation.0
            || destination != self.endpoint
            || context.session.is_some()
        {
            return Err(denied(Reason::StalePlan));
        }
        if context.cancel.is_cancelled() {
            return Err(denied(Reason::Cancelled));
        }
        if context.deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(denied(Reason::Timeout));
        }
        let selection = self
            .egress
            .select_for_peer_with_policy(self.peer, &binding.dial)
            .map_err(|source| denied(Reason::Io).with_source(source))?;
        if selection.generation() != binding.network_generation.0 {
            return Err(denied(Reason::StalePlan));
        }
        let native = Arc::new(SyncMutex::new(ControlledDatagramOwner::new(
            NonZeroUsize::new(1).unwrap(),
        )));
        {
            let mut state = grant.slot.state();
            if state.closed
                || state.opening
                || state.control.is_some()
                || state.packet_owner.is_some()
            {
                return Err(denied(Reason::Revoked));
            }
            state.packet_owner = Some(native.clone());
            state.binding = Some(binding.clone());
            state.endpoint = Some(destination.clone());
            state.unknown = Some(Arc::new(denied(Reason::Internal).with_source(
                std::io::Error::other("packet acquisition did not return an original disposition"),
            )));
        }
        // Original owner is persistent before the synchronous native boundary.
        let acquired = native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .acquire(self.peer, None, &binding.dial, &selection);
        let io = match acquired {
            Ok(io) => io,
            Err(source) => {
                let outcome = source.outcome;
                let settled = matches!(
                    source.outcome,
                    AcquisitionOutcome::KnownNoNative | AcquisitionOutcome::Closed(_)
                );
                let original = source.source.clone();
                let failure = error(
                    grant.instance(),
                    Stage::Dial,
                    Reason::Io,
                    if settled {
                        DeliveryState::NotSubmitted
                    } else {
                        DeliveryState::Unknown
                    },
                )
                .with_source(source);
                grant.slot.state().unknown = (!settled).then(|| {
                    Arc::new(
                        error(
                            grant.instance(),
                            Stage::Dial,
                            Reason::Io,
                            DeliveryState::Unknown,
                        )
                        .with_source(original),
                    )
                });
                let mut state = grant.slot.state();
                state.packet_disposition = Some(outcome);
                if settled {
                    state.closed = true;
                    state.pending_close = Some(crate::contract::lifecycle::CloseReport {
                        closed_io: if matches!(outcome, AcquisitionOutcome::Closed(_)) {
                            vec![grant.slot.label()]
                        } else {
                            vec![]
                        },
                        ..Default::default()
                    });
                }
                return Err(failure);
            }
        };
        let control = native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_control(&io)
            .expect("same sole packet acquisition");
        {
            let mut state = grant.slot.state();
            state.packet = Some(Arc::new(control));
            state.unknown = None;
            state.view_live = true;
        }
        super::super::users::acquired(&grant.slot);
        grant.transferred = true;
        let mut view = super::super::CarrierDatagram {
            io,
            slot: grant.slot.clone(),
            peer: self.peer,
        };
        let endpoints = super::super::CarrierEndpoints {
            origin: Arc::downgrade(&grant.slot),
            local: Some(
                view.local_addr()
                    .map_err(|source| denied(Reason::Io).with_source(source))?,
            ),
            peer: self.peer,
        };
        grant.slot.state().endpoints = Some(endpoints);
        view.io
            .set_observer(self.observer.clone())
            .map_err(|source| denied(Reason::Io).with_source(source))?;
        if self.egress.generation() != binding.network_generation.0 || !bound.available() {
            return Err(denied(Reason::StalePlan));
        }
        if context.cancel.is_cancelled() {
            return Err(denied(Reason::Cancelled));
        }
        Ok(view)
    }
}
