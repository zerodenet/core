use super::{failure, ManagedPool};
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    dns::DnsGeneration,
    network::{CarrierContext, UpstreamBinding},
    Reason, Stage,
};
pub(super) type Epoch = (
    zero_core::component::NetworkGeneration,
    Option<DnsGeneration>,
);
impl ManagedPool {
    /// Select the actual current root provider for compatibility users of this
    /// same pool. Selection grants no carrier and never replaces an original.
    pub async fn current_root_binding(&self) -> Result<UpstreamBinding, crate::RuntimeError> {
        self.check(&self.options.profile)?;
        let deadline = Instant::now()
            .checked_add(self.options.open_budget)
            .ok_or_else(|| std::io::Error::other("carrier deadline overflow"))?;
        let selected = tokio::select! { biased;
            _=self.cancel.cancelled()=>return Err(std::io::Error::other("pool revoked").into()),
            result=tokio::time::timeout_at(deadline, self.upstream.select_background_carrier(None, CarrierContext {
                session:None, deadline:Some(deadline), cancel:self.cancel.child_token(),
            }))=>result.map_err(|source|std::io::Error::new(std::io::ErrorKind::TimedOut,source))?.map_err(std::io::Error::other)?,
        };
        let binding = selected.binding();
        if !binding.prefix.is_empty() || binding.dial != self.options.binding.dial {
            return Err(
                std::io::Error::other("compatibility carrier is not the actual root").into(),
            );
        }
        Ok(binding.clone())
    }
    /// Check the retained real provider again before logical exposure. A
    /// changed DNS generation keeps the already installed original for cleanup.
    pub fn verify_epoch(&self, binding: &UpstreamBinding) -> Result<(), crate::RuntimeError> {
        if !self.ready()
            || binding.dial != self.options.binding.dial
            || self.epoch.lock().unwrap().as_ref()
                != Some(&(
                    binding.network_generation,
                    self.upstream.carrier_dns_generation(),
                ))
        {
            return Err(std::io::Error::other(failure(
                self.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ))
            .into());
        }
        Ok(())
    }

    /// Called while the real pool's admission mutex is held. Selection is a
    /// current-host authorization, not an acquisition or a close proof.
    pub async fn prepare_epoch(
        &self,
        profile: &str,
        binding: &UpstreamBinding,
    ) -> Result<(), crate::RuntimeError> {
        self.check(profile)?;
        if binding.dial != self.options.binding.dial {
            return Err(std::io::Error::other(failure(
                self.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ))
            .into());
        }
        let deadline = Instant::now()
            .checked_add(self.options.open_budget)
            .ok_or_else(|| std::io::Error::other("carrier deadline overflow"))?;
        // Root metadata can be selected without a caller witness. The actual
        // prefix (especially endpoint-free devices) is selected only inside
        // the already registered setup task with its exact observer/witness.
        tokio::select! { biased;
            _=self.cancel.cancelled()=>return Err(std::io::Error::other(failure(self.instance,Stage::Prepare,Reason::Revoked)).into()),
            selected=tokio::time::timeout_at(deadline,self.upstream.select_background_carrier(None,CarrierContext {
                session:None,deadline:Some(deadline),cancel:self.cancel.child_token(),
            }))=>{
                let selected=selected.map_err(|source|std::io::Error::new(std::io::ErrorKind::TimedOut,source))?.map_err(std::io::Error::other)?;
                if selected.binding().dial!=binding.dial || selected.binding().network_generation!=binding.network_generation {
                    return Err(std::io::Error::other(failure(self.instance,Stage::Prepare,Reason::StalePlan)).into());
                }
            },
        }
        let epoch = (
            binding.network_generation,
            self.upstream.carrier_dns_generation(),
        );
        if self
            .epoch
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|actual| actual == &epoch)
        {
            *self.epoch.lock().unwrap() = Some(epoch);
            return Ok(());
        }
        let records = self.records.lock().unwrap().clone();
        for record in &records {
            record.stop.cancel();
        }
        for record in records {
            let report = record
                .original
                .retire(deadline)
                .await
                .map_err(std::io::Error::other)?;
            let complete = report.is_complete();
            self.settled.lock().unwrap().append(report);
            if !complete {
                return Err(std::io::Error::other(failure(
                    self.instance,
                    Stage::Close,
                    Reason::Timeout,
                ))
                .into());
            }
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.harvest();
            if self.records.lock().unwrap().is_empty() {
                break;
            }
            tokio::select! {biased;
                _=self.cancel.cancelled()=>return Err(std::io::Error::other(failure(self.instance,Stage::Close,Reason::Revoked)).into()),
                result=tokio::time::timeout_at(deadline,changed)=>{result.map_err(|source|std::io::Error::new(std::io::ErrorKind::TimedOut,source))?;},
            }
        }
        self.check(profile)?;
        *self.epoch.lock().unwrap() = Some(epoch);
        // Both open preparations and logical users settled through matching
        // originals above; a body-exit flag alone was never used as evidence.
        Ok(())
    }
}
