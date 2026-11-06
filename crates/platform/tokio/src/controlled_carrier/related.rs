//! A negotiated peer depends on the actual control original, not its endpoint
//! as physical encapsulation ancestry. There is no independent owner or pool.
use super::{error, CarrierAdoption, CarrierGrant};
use crate::contract::{network::UpstreamBinding, ChannelError, DeliveryState, Reason, Stage};
use std::sync::{Arc, Weak};
use zero_core::component::ComponentInstanceId;

pub(super) fn check_parent(
    parent: &CarrierAdoption,
    instance: ComponentInstanceId,
    binding: &UpstreamBinding,
) -> Result<(), ChannelError> {
    let fail = || {
        error(
            instance,
            Stage::Prepare,
            Reason::StalePlan,
            DeliveryState::NotSubmitted,
        )
    };
    let owner = parent.slot.owner.upgrade().ok_or_else(fail)?;
    let authority = owner.state().binding.clone().ok_or_else(fail)?;
    if instance != parent.instance()
        || !authority.permits_related()
        || parent.slot.cancel.is_cancelled()
    {
        return Err(fail());
    }
    let state = parent.slot.state();
    if state.closed
        || state.opening
        || !state.view_live
        || state.unknown.is_some()
        || state.binding.as_ref() != Some(binding)
        || (state.control.is_none() && state.loan.is_none())
        || state.packet.is_some()
        || state.related_parent.is_some()
    {
        return Err(fail());
    }
    Ok(())
}

pub(super) fn check_endpoint(
    parent: &CarrierAdoption,
    endpoint: &zero_core::request::Destination,
) -> Result<(), ChannelError> {
    if parent.slot.state().endpoint.as_ref() != Some(endpoint) {
        return Err(error(
            parent.instance(),
            Stage::Prepare,
            Reason::StalePlan,
            DeliveryState::NotSubmitted,
        ));
    }
    Ok(())
}

impl CarrierGrant {
    /// Bind a reserved Datagram original to its already acquired control
    /// original. This grants no native/root I/O and never changes either
    /// original's physical CarrierOrigin. Logical loans still require their
    /// actual donor and the ordinary same-owner adoption checks.
    pub fn link_related_datagram(&self, parent: &CarrierAdoption) -> Result<(), ChannelError> {
        self.check()?;
        let binding = parent.slot.state().binding.clone().ok_or_else(|| {
            error(
                self.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
        })?;
        check_parent(parent, self.instance(), &binding)?;
        let fail = || {
            error(
                self.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
        };
        if !Weak::ptr_eq(&self.slot.owner, &parent.slot.owner)
            || Arc::ptr_eq(&self.slot, &parent.slot)
        {
            return Err(fail());
        }
        // Parent first is the only nested lock order. No callback/native work
        // occurs under either lock. Retirement fences admission before taking
        // its related-child snapshot.
        let mut parent_state = parent.slot.state();
        if parent.slot.cancel.is_cancelled() || parent_state.closed || !parent_state.view_live {
            return Err(fail());
        }
        if let Some(child) = &parent_state.related_child {
            if Arc::ptr_eq(child, &self.slot) {
                return Ok(());
            }
            if !child.state().closed {
                return Err(error(
                    self.instance(),
                    Stage::Prepare,
                    Reason::Capacity,
                    DeliveryState::NotSubmitted,
                ));
            }
        }
        let mut child = self.slot.state();
        if child.related_parent.is_some()
            || child.closed
            || child.opening
            || child.control.is_some()
            || child.packet_owner.is_some()
            || child.loan.is_some()
            || child.unknown.is_some()
        {
            return Err(fail());
        }
        child.related_parent = Some(Arc::downgrade(&parent.slot));
        parent_state.related_child = Some(self.slot.clone());
        Ok(())
    }
}

impl super::slot::Slot {
    pub(super) fn related_cancellation(&self) -> Option<crate::contract::CancellationToken> {
        self.state()
            .related_parent
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|parent| parent.cancel.clone())
    }
}

/// Poll the same two revocation facts inside the actual view; no cancellation
/// task or driver is created to wake a pending protocol poll.
pub(in crate::controlled_carrier) struct ViewCancellation {
    own: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    parent: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
}
impl ViewCancellation {
    pub(in crate::controlled_carrier) fn new(slot: &super::slot::Slot) -> Self {
        Self {
            own: Box::pin(slot.cancel.clone().cancelled_owned()),
            parent: slot
                .related_cancellation()
                .map(|parent| Box::pin(parent.cancelled_owned()) as _),
        }
    }
    pub(in crate::controlled_carrier) fn stopped(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> bool {
        self.own.as_mut().poll(cx).is_ready()
            || self
                .parent
                .as_mut()
                .is_some_and(|parent| parent.as_mut().poll(cx).is_ready())
    }
}
