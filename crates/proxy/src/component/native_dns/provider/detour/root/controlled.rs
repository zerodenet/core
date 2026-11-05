//! Retry the original caller's finite Node candidates with fresh actual grants.
use super::*;
use zero_platform_tokio::controlled_carrier::{CarrierCandidates, CarrierGrant, CarrierIo};
pub(super) struct Candidates {
    grantee: zero_core::component::ComponentInstanceId,
    values: CarrierCandidates,
    next: usize,
}
impl Root {
    pub(super) fn acquire_original(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<CarrierIo, ChannelError>> {
        Box::pin(async move {
            if endpoint != self.endpoint || call.session.is_some() {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::StalePlan,
                    "DNS controlled root differs from its actual declaration/caller",
                ));
            }
            self.retain_pending_carrier(&binding, grant.adoption())?;
            let mut cached = self.candidates.lock().await;
            if cached.is_none() {
                let opener = ResolvingCarrierOpener::new(
                    grant.instance(),
                    binding.clone(),
                    endpoint.clone(),
                    self.node_dns.clone(),
                    self.egress.clone(),
                    self.observer.clone(),
                )?;
                // A failed per-connection record cancels its own stop token.
                // Candidate authority remains with the actual DNS caller.
                let values = opener
                    .resolve_controlled_carrier(
                        &binding,
                        endpoint,
                        CarrierContext {
                            session: None,
                            deadline: call.deadline,
                            cancel: self.cancel.clone(),
                        },
                    )
                    .await?;
                *cached = Some(Candidates {
                    grantee: grant.instance(),
                    values,
                    next: 0,
                });
            }
            self.check_pending(&binding, &grant.adoption())?;
            let candidates = cached
                .as_mut()
                .expect("original caller candidates installed");
            if candidates.grantee != grant.instance() {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::StalePlan,
                    "DNS candidate grant belongs to another actual role",
                ));
            }
            if candidates.next == candidates.values.len() {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Io,
                    "DNS caller exhausted its original Node candidates",
                ));
            }
            let prepared = candidates.values.prepare_stream(candidates.next, grant)?;
            candidates.next += 1;
            drop(cached);
            Ok(prepared
                .open_with_deadline(call.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
}
