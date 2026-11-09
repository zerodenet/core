//! One sealed actual metadata candidate/initializer, never arbitrary routing.
use super::sources::memory::Allocation;
use super::*;
use crate::{
    controlled_icmp::{ControlledIcmpOwner, ControlledIcmpSocket, IcmpAttemptObservation},
    EgressSelection,
};
use std::{
    net::IpAddr,
    sync::atomic::{AtomicBool, Ordering},
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit};
mod memory;
pub use memory::MetadataIcmpMemory;
pub(super) struct Attempt {
    slot: Arc<Slot>,
    authority: Arc<binding::Authority>,
    caller: TaskJoinWitness,
    selection: Allocation<EgressSelection>,
    egress: crate::EgressInterfaceControl,
    source: IpAddr,
    call: crate::contract::network::CarrierContext,
    issued: AtomicBool,
    observation: IcmpAttemptObservation,
    acquisition_failure: Mutex<Option<PacketMetadataCredit>>,
}
#[derive(Clone)]
pub struct MetadataIcmpAttempt(PacketMetadataArc<Attempt>);
pub struct MetadataIcmpAuthorization(MetadataIcmpAttempt);
impl MetadataIcmpAttempt {
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.slot.instance
    }
    pub fn metadata_owner(&self) -> &MetadataPoolRegistration {
        &self.0.slot.registration
    }
    pub fn observation(&self) -> IcmpAttemptObservation {
        self.0.observation.clone()
    }
    /// Seals only this not-yet-issued attempt, preventing later native bind.
    pub fn reject_unissued(&self) {
        if self
            .0
            .issued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.0.observation.reject_unstarted()
        }
    }
    pub fn authorize_host(
        &self,
        original: &MetadataPoolRegistration,
        instance: ComponentInstanceId,
        scope: &TaskScopeIdentity,
        host_cancel: &Arc<CancellationToken>,
    ) -> Result<MetadataIcmpAuthorization, ChannelError> {
        if self.0.slot.instance != instance
            || !self.0.slot.registration.same_owner(original)
            || !self.0.caller.scope_identity().same_scope(scope)
            || !Arc::ptr_eq(&self.0.authority.host_cancel, host_cancel)
            || !self.0.authority.scope.same_scope(scope)
        {
            return Err(error(
                Reason::StalePlan,
                "ICMP attempt requires its actual metadata Host",
            ));
        }
        self.0.check()?;
        self.0
            .issued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                error(
                    Reason::Revoked,
                    "metadata ICMP attempt was already consumed",
                )
            })?;
        Ok(MetadataIcmpAuthorization(self.clone()))
    }
}
impl Attempt {
    fn check(&self) -> Result<(), ChannelError> {
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata owner gone"))?;
        self.authority.check(&owner, &self.caller)?;
        if self.call.cancel.is_cancelled() {
            return Err(error(
                Reason::Cancelled,
                "actual ICMP initializer cancelled",
            ));
        }
        if self
            .call
            .deadline
            .is_some_and(|end| tokio::time::Instant::now() >= end)
        {
            return Err(error(
                Reason::Timeout,
                "actual ICMP initializer deadline elapsed",
            ));
        }
        {
            let native = self.slot.native()?;
            if native.unbound.is_some()
                || native.confirmed.is_some()
                || !native
                    .icmp
                    .as_ref()
                    .and_then(|record| record.upgrade())
                    .is_some_and(|actual| std::ptr::eq(&*actual, self))
            {
                return Err(error(
                    Reason::Revoked,
                    "actual metadata initializer no longer owns its ICMP attempt",
                ));
            }
        }
        if self.slot.closed.get().is_some()
            || self.slot.retired.is_cancelled()
            || self
                .slot
                .protocol_stop
                .get()
                .is_some_and(CancellationToken::is_cancelled)
            || self.egress.generation() != self.selection.generation()
        {
            return Err(error(
                Reason::Revoked,
                "metadata ICMP actual original retired",
            ));
        }
        Ok(())
    }
}
impl MetadataIcmpAuthorization {
    pub fn observation(&self) -> IcmpAttemptObservation {
        self.0.observation()
    }
    pub fn bind(
        &self,
        owner: &mut ControlledIcmpOwner,
    ) -> Result<ControlledIcmpSocket, ChannelError> {
        self.0 .0.check()?;
        if !self.0 .0.observation.start() {
            return Err(error(Reason::Revoked, "ICMP binding already started"));
        }
        owner
            .bind_for_metadata(
                self.0 .0.source,
                self.0 .0.selection.interface(),
                &self.0 .0.caller,
                self.0
                     .0
                    .slot
                    .closed
                    .observation(self.0 .0.slot.instance, self.0 .0.slot.ordinal),
                &self.0 .0.observation,
            )
            .map_err(|source| {
                let unknown = matches!(
                    &source.outcome,
                    crate::controlled_icmp::IcmpAcquisitionOutcome::Unknown(_)
                );
                let credit = {
                    self.0
                         .0
                        .acquisition_failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                        .expect("one actual ICMP acquisition outcome")
                };
                let source = PacketMetadataArc::from_prepaid(source, credit)
                    .unwrap_or_else(|_| panic!("actual ICMP outcome prepaid"));
                let mut failure = ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                    .with_metadata_source(source.into_error());
                failure.delivery = DeliveryState::NotSubmitted;
                if unknown {
                    failure.delivery = DeliveryState::Unknown;
                }
                failure
            })
    }
}
impl Drop for MetadataIcmpAuthorization {
    fn drop(&mut self) {
        self.0 .0.observation.reject_unstarted()
    }
}
impl MetadataPoolPermit {
    pub(crate) fn prepare_icmp(
        &self,
        query: &TaskJoinWitness,
        target: MetadataCandidate<'_>,
        call: &crate::contract::network::CarrierContext,
        memory: MetadataIcmpMemory,
    ) -> Result<MetadataIcmpAttempt, ChannelError> {
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata owner gone"))?;
        let authority = owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bound
            .clone()
            .ok_or_else(|| error(Reason::InvalidConfig, "unregistered metadata ICMP owner"))?;
        let actual_memory = authority.metadata.as_ref().ok_or_else(|| {
            error(
                Reason::InvalidConfig,
                "metadata ICMP requires actual Host accounting",
            )
        })?;
        // Reject a foreign wallet before taking any actual original mutex.
        let _notifications = actual_memory.notifications();
        let (attempt_credit, observation, acquisition_failure) = memory.take(actual_memory)?;
        authority.check(&owner, query)?;
        let mut native = self.0.native()?;
        if !self
            .0
            .caller
            .get()
            .is_some_and(|actual| actual.same_registration(query))
            || target.instance != self.0.instance
            || !target.binding.prefix.is_empty()
            || native.control.is_none()
            || native.endpoints.is_none_or(|(_, peer)| peer != target.peer)
            || !native
                .binding
                .as_ref()
                .is_some_and(|(endpoint, binding, dns)| {
                    endpoint == target.endpoint
                        && binding == target.binding
                        && *dns == target.dns_generation
                })
        {
            return Err(error(
                Reason::StalePlan,
                "ICMP attempt differs from actual metadata original",
            ));
        }
        if native.icmp_prepared {
            return Err(error(
                Reason::Revoked,
                "metadata original already has an ICMP attempt",
            ));
        }
        let (selection, egress) = native.selection.as_ref().ok_or_else(|| {
            error(
                Reason::InvalidConfig,
                "metadata original lost actual egress selection",
            )
        })?;
        let source = selection
            .dial_source_address()
            .map(|source| source.ip())
            .unwrap_or_else(|| {
                if target.peer.is_ipv6() {
                    std::net::Ipv6Addr::UNSPECIFIED.into()
                } else {
                    std::net::Ipv4Addr::UNSPECIFIED.into()
                }
            });
        let attempt = PacketMetadataArc::from_prepaid(
            Attempt {
                slot: self.0.clone(),
                authority,
                caller: query.clone(),
                selection: selection.clone(),
                egress: egress.clone(),
                source,
                call: call.clone(),
                issued: AtomicBool::new(false),
                observation,
                acquisition_failure: Mutex::new(Some(acquisition_failure)),
            },
            attempt_credit,
        )
        .unwrap_or_else(|_| panic!("actual attempt prepaid before native IO"));
        native.icmp_prepared = true;
        native.icmp = Some(PacketMetadataArc::downgrade(&attempt));
        Ok(MetadataIcmpAttempt(attempt))
    }
}
