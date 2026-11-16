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
#[derive(Default)]
pub(super) enum EpochState {
    #[default]
    Cold,
    Ready(Epoch),
    // This remains set across cancellation, errors and incomplete cleanup.
    Draining,
}
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
        let current = (
            binding.network_generation,
            self.upstream.carrier_dns_generation(),
        );
        if !self.ready()
            || binding.dial != self.options.binding.dial
            || !matches!(&*self.epoch.lock().unwrap(), EpochState::Ready(actual) if *actual == current)
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

    /// One epoch transition fences every physical prefix of this actual pool.
    /// It never holds the prefix publication lane across another prefix's IO.
    pub async fn prepare_epoch(
        &self,
        profile: &str,
        binding: &UpstreamBinding,
    ) -> Result<(), crate::RuntimeError> {
        let _transition = tokio::select! { biased;
            _=self.cancel.cancelled()=>return Err(std::io::Error::other(failure(self.instance,Stage::Prepare,Reason::Revoked)).into()),
            result=tokio::time::timeout(self.options.open_budget,self.connecting.lock())=>result.map_err(|source|std::io::Error::new(std::io::ErrorKind::TimedOut,source))?,
        };
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
        let records = {
            let mut state = self.epoch.lock().unwrap();
            if matches!(&*state, EpochState::Cold)
                || matches!(&*state, EpochState::Ready(actual) if *actual == epoch)
            {
                *state = EpochState::Ready(epoch);
                return Ok(());
            }
            // Same lock order as install_original: fence admission before
            // taking the complete census, including interrupted preparations.
            *state = EpochState::Draining;
            self.records.lock().unwrap().clone()
        };
        if !self
            .retire_originals(records, deadline)
            .await
            .map_err(std::io::Error::other)?
        {
            return Err(std::io::Error::other(failure(
                self.instance,
                Stage::Close,
                Reason::Timeout,
            ))
            .into());
        }
        self.check(profile)?;
        *self.epoch.lock().unwrap() = EpochState::Ready(epoch);
        // Both open preparations and logical users settled through matching
        // originals above; a body-exit flag alone was never used as evidence.
        Ok(())
    }
}
