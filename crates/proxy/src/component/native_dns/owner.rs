//! Actual owner closure hands real task faults to the existing Host receiver.
use super::*;
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), BuildError> {
        for (ordinal, owner) in self.owners.iter_mut().enumerate() {
            self.resources.register_controlled_carrier(
                ordinal,
                owner.registration(),
                &self.tasks,
            )?;
            owner.start(end).await?;
        }
        #[cfg(feature = "dns")]
        for (index, owner) in self.metadata.iter().enumerate() {
            self.resources.register_metadata_pool(
                self.owners.len() + index,
                owner.registration(),
                &self.tasks,
            )?;
            owner.start()?;
        }
        if let Some(ordinal) = self.journal {
            let dns = self
                .network
                .resolver
                .get()
                .and_then(std::sync::Weak::upgrade)
                .ok_or_else(|| {
                    provider::error(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "actual Default DNS resolver was withdrawn",
                    ))
                })?;
            dns.ensure_fake_ip_journal_startable()
                .map_err(provider::error)?;
            let grant = self.resources.journal(ordinal)?;
            dns.install_fake_ip_journal(grant, &self.tasks)
                .map_err(provider::error)?;
        }
        let provider: Arc<dyn zero_dns::native::NativeDnsProvider> = self.provider.clone();
        self.purpose
            .source(&self.network)
            .install(&provider)
            .map_err(provider::error)?;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        let provider: Arc<dyn zero_dns::native::NativeDnsProvider> = self.provider.clone();
        let current = self.purpose.source(&self.network).withdraw(&provider);
        self.provider.stop();
        if current {
            if let Some(dns) = self
                .network
                .resolver
                .get()
                .and_then(std::sync::Weak::upgrade)
            {
                dns.retire_native_query_source(self.purpose.source(&self.network));
            }
        }
        for owner in &self.owners {
            owner.revoke();
        }
        #[cfg(feature = "dns")]
        for owner in &self.metadata {
            owner.revoke();
        }
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut unfinished = Vec::new();
        if let Some(dns) = self
            .network
            .resolver
            .get()
            .and_then(std::sync::Weak::upgrade)
        {
            if let Err(source) = dns.shutdown_native_pools(&self.provider.tasks(), end).await {
                if provider::retirement::pending_observation(&source) {
                    unfinished.push(
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                            "DNS pool's actual cleanup wait remains pending".into(),
                        ),
                    );
                } else {
                    receive(&self.provider, &mut self.pending, provider::error(source))?;
                }
            }
        }
        let queries_joined = match self.provider.close_queries(end).await {
            Ok(()) => true,
            Err(source) => {
                if !provider::retirement::pending_observation(&source) {
                    receive(&self.provider, &mut self.pending, provider::error(source))?;
                }
                false
            }
        };
        for owner in &mut self.owners {
            match owner.shutdown(end).await {
                Ok(mut report) => {
                    unfinished.append(&mut report.unfinished);
                    self.pending.append(report);
                }
                Err(source) => {
                    receive(&self.provider, &mut self.pending, source)?;
                }
            }
        }
        if let Err(source) = self.provider.reap(end).await {
            if provider::retirement::pending_observation(&source) {
                unfinished.push(
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "DNS original's actual cleanup wait remains pending".into(),
                    ),
                );
            } else {
                receive(&self.provider, &mut self.pending, provider::error(source))?;
            }
        }
        #[cfg(feature = "dns")]
        for owner in &self.metadata {
            match owner.shutdown_after_registration_ack(end).await {
                Ok(mut report) => {
                    unfinished.append(&mut report.unfinished);
                    self.pending.append(report);
                }
                Err(source) => {
                    receive(&self.provider, &mut self.pending, source)?;
                }
            }
        }
        self.provider.tasks().observe_native_retirements();
        let faults = self.provider.tasks().retained_faults();
        if let Some(faults) = &faults {
            provider::retirement::report_faults(
                faults,
                &mut self.reported_faults,
                &mut self.pending,
            );
        }
        let protocol_complete = queries_joined && self.provider.native_retirement_complete();
        // Exact worker sources are now persisted in failed_tasks. Their faults
        // are separate from positive native/logical closure, never its proof.
        if !protocol_complete {
            unfinished.push(
                zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                    "DNS original logical/native query retirement remains unconfirmed".into(),
                ),
            );
        }
        let mut report = std::mem::take(&mut self.pending);
        report.unfinished = unfinished;
        Ok(report)
    }
}

fn receive(
    provider: &Provider,
    report: &mut CloseReport,
    source: BuildError,
) -> Result<(), BuildError> {
    let actual = provider.receive_cleanup_failure(source)?;
    if !report
        .resource_failures
        .iter()
        .any(|same| Arc::ptr_eq(same, &actual))
    {
        report.resource_failures.push(actual);
    }
    Ok(())
}
