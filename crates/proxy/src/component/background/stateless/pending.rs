//! Foreign cleanup claims stay on the same finite original, including no observer.
use super::*;
use std::sync::Weak;
use zero_platform_tokio::contract::{
    context::RequestIdentity, network::PendingCarrierObserver, scope::TaskJoinWitness,
};
struct Pending {
    transfer: Weak<Transfer>,
    upstream: UpstreamTransport,
    binding: UpstreamBinding,
    call: CarrierContext,
    caller: TaskJoinWitness,
    waiting: Option<PendingCarrierObserver>,
    limit: usize,
}
pub(super) fn transport(
    context: &OpenContext,
    transfer: &Arc<Transfer>,
    caller: TaskJoinWitness,
) -> Result<UpstreamTransport, ChannelError> {
    let binding = context.upstream.binding().clone();
    let roles: Arc<[zero_core::component::RoleInstanceId]> = binding.prefix.clone().into();
    if let Some(previous) = transfer.retained_prefix.get() {
        if previous.as_ref() != roles.as_ref() {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "actual retained prefix changed on one original",
            ));
        }
    } else {
        let _ = transfer.retained_prefix.set(roles);
    }
    let limit = match context.upstream.pending_carrier_capacity() {
        Some(limit) => limit,
        None if binding.prefix.is_empty() => 0,
        None => {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "actual setup graph has no finite pending declaration",
            ))
        }
    };
    Ok(UpstreamTransport::new(
        binding.clone(),
        Arc::new(Pending {
            transfer: Arc::downgrade(transfer),
            upstream: context.upstream.clone(),
            binding,
            call: CarrierContext {
                session: context.session_identity()?,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
            caller,
            waiting: context.upstream.observed_pending_carrier(),
            limit,
        }),
    ))
}
impl Pending {
    fn check(&self, binding: &UpstreamBinding) -> Result<Arc<Transfer>, ChannelError> {
        let original = self.transfer.upgrade().ok_or_else(|| {
            crate::component::instance::invalid(Reason::Revoked, "actual setup record retired")
        })?;
        let bound = original
            .adoption
            .cleanup_owner()
            .and_then(|owner| owner.bound())
            .ok_or_else(|| {
                crate::component::instance::invalid(Reason::Revoked, "actual setup owner retired")
            })?;
        if binding != &self.binding
            || self.call.cancel.is_cancelled()
            || original.stop.is_cancelled()
            || self.caller.is_joined()
            || !self.caller.scope_identity().resource_open()
            || !bound
                .scope_identity()
                .same_scope(&self.caller.scope_identity())
            || bound.host_cancelled()
        {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "actual setup caller changed",
            ));
        }
        Ok(original)
    }
}
impl UpstreamOpener for Pending {
    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        self.check(binding).ok().map(|_| self.limit)
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        self.check(binding)?;
        if call.session.is_some()
            || call.deadline != self.call.deadline
            || call.cancel.is_cancelled()
            || !matches!(identity, RequestIdentity::Service(facts) if facts.origin == zero_core::request::ServiceOrigin::Runtime
                && facts.purpose == zero_core::request::ServicePurpose::BackgroundControl)
        {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "actual setup Service projection changed",
            ));
        }
        Ok(self.caller.clone())
    }
    fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        original: &CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check(binding)?;
        self.upstream.validate_pending_carrier(binding, original)
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let original = self.check(binding)?;
        self.upstream.validate_pending_carrier(binding, &adoption)?;
        if let Some(waiting) = &self.waiting {
            waiting.retain(adoption.clone())?;
        }
        original.release_pending();
        let mut pending = original.pending.lock().unwrap();
        if pending.iter().any(|slot| slot.same_slot(&adoption)) {
            return Ok(());
        }
        if pending.len() >= self.limit {
            return Err(crate::component::instance::invalid(
                Reason::Capacity,
                "actual setup pending limit",
            ));
        }
        pending.try_reserve(1).map_err(|source| {
            crate::component::instance::invalid(Reason::Capacity, "actual setup pending allocation")
                .with_source(source)
        })?;
        pending.push(adoption);
        Ok(())
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            Err(crate::component::instance::invalid(
                Reason::Unsupported,
                "setup observation has no IO authority",
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async {
            Err(crate::component::instance::invalid(
                Reason::Unsupported,
                "setup observation has no IO authority",
            ))
        })
    }
}
impl Transfer {
    fn retains_cached_driver(&self, original: &CarrierAdoption) -> bool {
        let Some(network) = &self.cleanup_network else {
            return false;
        };
        let Ok(source) = network.current_services() else {
            return false;
        };
        let Some(roles) = self.retained_prefix.get() else {
            return false;
        };
        if !roles.iter().any(|role| {
            source
                .validate_owned_carrier_adoption(*role, original)
                .is_ok()
        }) {
            return false;
        }
        original
            .retained_driver()
            .is_some_and(|proof| proof.retains(original))
    }
    pub(super) fn release_pending(&self) {
        let observed = self.pending.lock().unwrap().clone();
        let installed = self.adoption.retained_loan();
        let releasable = observed
            .iter()
            .filter(|slot| {
                slot.closed().is_some_and(|closed| closed.matches(slot))
                    && slot
                        .users_closed()
                        .is_some_and(|closed| closed.matches(slot))
                    || installed.as_ref().is_some_and(|proof| proof.retains(slot))
                    || self.retains_cached_driver(slot)
                    || observed.iter().any(|receiver| {
                        !receiver.same_slot(slot)
                            && receiver
                                .retained_loan()
                                .is_some_and(|proof| proof.retains(slot))
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        let removed = {
            let mut pending = self.pending.lock().unwrap();
            let mut removed = Vec::new();
            let mut index = 0;
            while index < pending.len() {
                if releasable
                    .iter()
                    .any(|slot| slot.same_slot(&pending[index]))
                {
                    removed.push(pending.remove(index));
                } else {
                    index += 1;
                }
            }
            removed
        };
        drop(removed);
    }
    pub(super) async fn close_pending(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.release_pending();
        let pending = self.pending.lock().unwrap().clone();
        let mut report = CloseReport::default();
        for original in pending {
            report.append(original.retire(end).await?);
        }
        self.release_pending();
        Ok(report)
    }
    pub(super) fn try_close_pending(&self) -> Result<CloseReport, ChannelError> {
        self.release_pending();
        let pending = self.pending.lock().unwrap().clone();
        let mut report = CloseReport::default();
        for original in pending {
            report.append(original.try_retire()?);
        }
        self.release_pending();
        Ok(report)
    }
    pub(super) fn request_pending_retirement(&self) {
        self.release_pending();
        let pending = self.pending.lock().unwrap().clone();
        for original in pending {
            original.request_retirement();
        }
    }
}
