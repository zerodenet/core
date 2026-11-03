//! Actual owner startup and final handoff of native/task cleanup reports.
use super::*;
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        if self.attempted {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        self.attempted = true;
        // Register these exact inert owners before any fallible material read
        // or deadline gate. A failed start still owes their real no-acquisition
        // shutdown receipts; a missing host registration cannot stand in.
        for (ordinal, owner) in self.generation.metadata.iter().enumerate() {
            self.resources.register_metadata_pool(
                ordinal,
                owner.registration(),
                &self.generation.tasks,
            )?;
        }
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
        if self.generation.clients.get().is_none() {
            let prepared = self
                .generation
                .prepared_ca
                .as_ref()
                .map(|storage| {
                    storage
                        .lock()
                        .unwrap()
                        .clone()
                        .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
                })
                .transpose()?;
            let mut clients = Vec::new();
            for (index, profile) in self.generation.profiles.iter().enumerate() {
                let material = match &prepared {
                    Some(materials) => materials[index].clone(),
                    None => match profile.ca_cert_path() {
                        Some(path) => Some(Arc::<[u8]>::from(
                            self.resources.read_file(path, 16 * 1024 * 1024).await?,
                        )),
                        None => None,
                    },
                };
                clients.push(profile.client_config(material.as_deref())?);
            }
            // The actual cold recipe owns this write-once crypto capsule.
            // A changed material identity owns another capsule. Restoring this old
            // recipe after a failed candidate reuses its last-good material;
            // it never reopens a changed path or retains any original I/O.
            self.generation
                .clients
                .set(clients.into())
                .map_err(|_| error(Stage::Prepare, Reason::Revoked))?;
            drop(prepared);
            if let Some(storage) = &self.generation.prepared_ca {
                drop(storage.lock().unwrap().take());
            }
        }
        for owner in &self.generation.metadata {
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
