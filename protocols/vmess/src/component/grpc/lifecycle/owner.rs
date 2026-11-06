//! The same actual protocol, native and Scope owner, with no extra worker.
use super::*;
pub(super) struct Owner {
    pub(super) managed: Arc<Managed>,
    pub(super) carriers: CarrierOwner,
    pub(super) service_carriers: Option<CarrierOwner>,
    pub(super) base: Box<dyn DynResourceLifecycle>,
    pub(super) attempted: bool,
    pub(super) resources: Arc<dyn ResourceAccess>,
    pub(super) carrier_ordinal: usize,
    pub(super) stream_admission_claimed: bool,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.managed.check().is_ok() {
            return Ok(());
        }
        if self.attempted || self.managed.cancel.is_cancelled() {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        if Instant::now() >= deadline {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Timeout,
            ));
        }
        self.attempted = true;
        if let Some(admission) = &self.managed.options.stream_admission {
            admission.claim_node_streams(self.managed.instance)?;
            self.stream_admission_claimed = true;
        }
        self.managed.options.state.claim(self.managed.instance)?;
        if self.managed.options.business.is_some() {
            self.resources.register_controlled_carrier(
                self.carrier_ordinal,
                self.carriers.registration(),
                &self.managed.tasks,
            )?;
        }
        if let Some(service) = &self.service_carriers {
            self.resources.register_controlled_carrier(
                self.carrier_ordinal + 1,
                service.registration(),
                &self.managed.tasks,
            )?;
        }
        self.base.start(deadline).await?;
        self.carriers.start(deadline).await?;
        if let Some(service) = &mut self.service_carriers {
            service.start(deadline).await?;
        }
        if self.managed.cancel.is_cancelled() || self.managed.tasks.cancellation().is_cancelled() {
            return Err(error(
                self.managed.instance,
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        self.managed.ready.store(true, Ordering::Release);
        Ok(())
    }
    fn revoke(&self, reason: CloseReason) {
        self.managed.stop();
        self.carriers.revoke();
        if let Some(service) = &self.service_carriers {
            service.revoke();
        }
        self.base.revoke(reason);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let (base, physical, service) = tokio::join!(
            self.base.shutdown(deadline),
            self.carriers.shutdown(deadline),
            async {
                match &mut self.service_carriers {
                    Some(service) => service.shutdown(deadline).await,
                    None => Ok(CloseReport::default()),
                }
            },
        );
        let records = self.managed.records.lock().unwrap().clone();
        for record in records {
            record.receive_available();
        }
        let mut report = base?;
        report.append(physical?);
        report.append(service?);
        self.managed
            .records
            .lock()
            .unwrap()
            .retain(|record| !record.closed());
        if !self.managed.records.lock().unwrap().is_empty() {
            report.unfinished.push(Unfinished::Io(
                "VMess original gRPC carriers remain unconfirmed".into(),
            ));
        }
        let service_closed = self.service_carriers.as_ref().is_none_or(|owner| {
            owner
                .registration()
                .closed()
                .is_some_and(|closed| closed.matches(&owner.registration()))
        });
        if self.managed.options.node()
            && service_closed
            && report.is_complete()
            && self
                .carriers
                .registration()
                .closed()
                .is_some_and(|closed| closed.matches(&self.carriers.registration()))
        {
            self.managed.options.state.release(self.managed.instance);
            if self.stream_admission_claimed {
                self.managed
                    .options
                    .stream_admission
                    .as_ref()
                    .unwrap()
                    .release_node_streams(self.managed.instance);
                self.stream_admission_claimed = false;
            }
        }
        Ok(report)
    }
}
