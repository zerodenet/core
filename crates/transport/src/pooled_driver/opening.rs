//! Original physical preparation; protocol TLS/auth still supplies one closure.
use super::{failure, Driver, ManagedPool, Physical};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, PendingCarrierObserver, UpstreamBinding},
        scope::TaskJoinWitness,
        ChannelError, Reason, Stage,
    },
    controlled_carrier::{CarrierDriver, CarrierGrant, CarrierIo},
};
impl ManagedPool {
    pub(super) async fn settle_failed(
        &self,
        record: &Arc<Physical>,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        record.release_installed_pending();
        let report = record.original.retire(deadline).await?;
        self.settled.lock().unwrap().append(report);
        let pending = record.pending.lock().unwrap().clone();
        for original in pending {
            let report = original.retire(deadline).await?;
            self.settled.lock().unwrap().append(report);
        }
        record.release_installed_pending();
        self.harvest();
        Ok(())
    }

    pub(super) async fn prepare_original<C, F, Fut>(
        self: Arc<Self>,
        physical: Arc<Physical>,
        binding: UpstreamBinding,
        call: CarrierContext,
        grant: CarrierGrant,
        waiting: Option<PendingCarrierObserver>,
        caller: Option<TaskJoinWitness>,
        candidates: Option<Arc<super::candidates::Candidates>>,
        prepare: Arc<Mutex<Option<F>>>,
    ) -> Result<CarrierDriver<(C, Driver)>, ChannelError>
    where
        C: Send + 'static,
        F: FnOnce(CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(C, Driver), crate::RuntimeError>> + Send + 'static,
    {
        let physical_call = CarrierContext {
            session: None,
            deadline: call.deadline,
            cancel: physical.stop.clone(),
        };
        let selected = if self.options.scoped_preparation {
            let pending = super::pending::transport(
                self.clone(),
                physical.clone(),
                binding.clone(),
                physical_call.clone(),
                waiting,
                caller.ok_or_else(|| failure(self.instance, Stage::Prepare, Reason::StalePlan))?,
            )?;
            self.upstream
                .select_background_carrier_observed(
                    Some(binding.clone()),
                    physical_call.clone(),
                    pending.pending_carrier_observer(),
                )
                .await?
        } else {
            // The fixed library API already owns this inline preparation and
            // its later original driver. No synthetic setup task or witness.
            self.upstream
                .select_background_carrier(Some(binding.clone()), physical_call.clone())
                .await?
        };
        if selected.binding() != &binding {
            return Err(failure(self.instance, Stage::Prepare, Reason::StalePlan));
        }
        self.verify_epoch(&binding).map_err(|source| {
            failure(self.instance, Stage::Prepare, Reason::StalePlan).with_source(source)
        })?;
        let raw = if binding.prefix.is_empty() && self.stream_dependency.is_none() {
            let prepared = candidates
                .ok_or_else(|| failure(self.instance, Stage::Prepare, Reason::Internal))?
                .prepare(&selected, self.options.endpoint.clone(), grant)
                .await?;
            prepared
                .open_with_deadline(call.deadline)
                .await?
                .into_parts()
                .0
        } else {
            let issuer = if binding.prefix.is_empty() {
                let dependency = self
                    .stream_dependency
                    .ok_or_else(|| failure(self.instance, Stage::Prepare, Reason::StalePlan))?;
                Some(
                    self.resources
                        .upgrade()
                        .ok_or_else(|| failure(self.instance, Stage::Prepare, Reason::Revoked))?
                        .dependency_stream_loan_issuer(
                            dependency,
                            &self.options.endpoint,
                            &binding,
                            &grant,
                        )?,
                )
            } else {
                None
            };
            let loan = selected
                .borrow_stream(self.options.endpoint.clone(), physical_call.clone())
                .await?;
            let matches_dependency = self
                .stream_dependency
                .is_none_or(|expected| loan.owner.original().instance() == expected);
            // Install the actual controller even on a rejected foreign loan.
            // The same persistent record then closes it, never bare Drop.
            let raw = match issuer {
                Some(issuer) => grant.adopt_dependency_stream_loan(
                    &binding,
                    self.options.endpoint.clone(),
                    &physical_call,
                    loan,
                    issuer,
                )?,
                None => grant.adopt_stream_loan(
                    &binding,
                    self.options.endpoint.clone(),
                    &physical_call,
                    loan,
                )?,
            };
            if !matches_dependency {
                return Err(failure(self.instance, Stage::Prepare, Reason::StalePlan));
            }
            raw
        };
        // Only after native connect succeeded is the protocol's single owning
        // callback consumed. A refused address therefore preserves it for the
        // next actual candidate, never for retrying an authenticated session.
        let prepare = { prepare.lock().unwrap().take() }
            .ok_or_else(|| failure(self.instance, Stage::Handshake, Reason::Internal))?;
        let prepared = raw.prepare_driver(prepare).await.map_err(|source| {
            failure(self.instance, Stage::Handshake, Reason::Io).with_source(source)
        })?;
        physical.release_installed_pending();
        self.verify_epoch(&binding).map_err(|source| {
            failure(self.instance, Stage::Prepare, Reason::StalePlan).with_source(source)
        })?;
        Ok(prepared)
    }
}
