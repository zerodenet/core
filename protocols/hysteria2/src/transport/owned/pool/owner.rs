//! Actual owner startup and final handoff of native/task cleanup reports.
use super::*;
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        if self.attempted {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        self.attempted = true;
        if Instant::now() >= end {
            return Err(error(Stage::Prepare, Reason::Timeout));
        }
        if self
            .pool
            .current
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some()
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let mut clients = Vec::new();
        for profile in self.generation.profiles.iter() {
            let material = match profile.leaf.node.ca_cert_path() {
                Some(path) => Some(self.resources.read_file(path, 16 * 1024 * 1024).await?),
                None => None,
            };
            clients.push(profile.client_config(material.as_deref())?);
        }
        self.generation
            .clients
            .set(clients.into())
            .map_err(|_| error(Stage::Prepare, Reason::Revoked))?;
        for (ordinal, owner) in self.generation.metadata.iter().enumerate() {
            self.resources.register_metadata_pool(
                ordinal,
                owner.registration(),
                &self.generation.tasks,
            )?;
            owner.start()?;
        }
        *self.pool.current.lock().unwrap() = Some(Arc::downgrade(&self.generation));
        let weak = Arc::downgrade(&self.generation);
        let stop = self.generation.cancel.clone();
        let (_,witness)=self.generation.tasks.spawn_tracked(TaskClass::Critical,async move {
            loop {tokio::select! {biased;_=stop.cancelled()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(1))=>{}}
                let Some(generation)=weak.upgrade() else{return Ok(())};
                generation.maintain(Instant::now()+Duration::from_secs(5)).await?;
            }
        }).map_err(|source|error(Stage::Prepare,Reason::Capacity).with_source(source))?;
        *self.generation.maintenance.lock().unwrap() = Some(witness);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.generation.cancel.cancel();
        for entry in self.generation.entries.lock().unwrap().values() {
            entry.cancel.cancel();
        }
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let maintenance = self.generation.maintenance.lock().unwrap().clone();
        if let Some(witness) = maintenance {
            let joined = tokio::time::timeout_at(end, witness.wait())
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
            if joined.result().is_err() {
                witness
                    .acknowledge_hosted_failure(&joined)
                    .map_err(|source| error(Stage::Close, Reason::Internal).with_source(source))?;
            }
        }
        let entries = self
            .generation
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            entry.close(end).await?;
        }
        let mut report = CloseReport::default();
        let entries = self
            .generation
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            let state = entry.state.lock().await;
            ledger::append_report(
                &mut report,
                state
                    .retired_drivers
                    .iter()
                    .map(|delivery| &delivery.joined)
                    .chain(state.retired_application.iter()),
            );
        }
        {
            let failures = self.generation.failures.lock().unwrap();
            for group in failures.iter() {
                ledger::append_report(
                    &mut report,
                    group
                        .drivers
                        .iter()
                        .map(|delivery| &delivery.joined)
                        .chain(group.application.iter()),
                );
            }
        }
        for owner in &self.generation.metadata {
            report.append(owner.shutdown()?);
        }
        if report.is_complete() {
            let retired_entries = std::mem::take(&mut *self.generation.entries.lock().unwrap());
            let retired_failures = std::mem::take(&mut *self.generation.failures.lock().unwrap());
            *self.pool.current.lock().unwrap() = None;
            drop(retired_entries);
            drop(retired_failures);
        }
        Ok(report)
    }
}
