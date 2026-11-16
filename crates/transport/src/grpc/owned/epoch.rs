//! An egress/Node change cannot run a second physical epoch alongside the old.
use super::*;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Epoch(
    zero_core::component::NetworkGeneration,
    Option<zero_platform_tokio::contract::dns::DnsGeneration>,
);
#[derive(Default)]
pub(super) enum EpochState {
    #[default]
    Cold,
    Ready(Epoch),
    Draining,
}
impl Managed {
    pub(super) fn verify_epoch(
        &self,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
        dns: Option<zero_platform_tokio::contract::dns::DnsGeneration>,
    ) -> Result<(), ChannelError> {
        self.verify_epoch_state(&self.epoch.lock().unwrap(), binding, dns)
    }
    pub(super) fn verify_epoch_state(
        &self,
        state: &EpochState,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
        dns: Option<zero_platform_tokio::contract::dns::DnsGeneration>,
    ) -> Result<(), ChannelError> {
        if !self.options.selected_background {
            return Ok(());
        }
        let expected = Epoch(binding.network_generation, dns);
        if !matches!(state, EpochState::Ready(actual) if *actual == expected) {
            return Err(error(self.instance, Stage::Prepare, Reason::StalePlan));
        }
        Ok(())
    }

    pub(super) async fn admit_epoch(
        &self,
        demand: &Demand,
        dns: Option<zero_platform_tokio::contract::dns::DnsGeneration>,
    ) -> Result<(), ChannelError> {
        let epoch = Epoch(demand.binding.network_generation, dns);
        {
            let mut current = self.epoch.lock().unwrap();
            match &*current {
                EpochState::Cold => {
                    *current = EpochState::Ready(epoch);
                    return Ok(());
                }
                EpochState::Ready(actual) if *actual == epoch => return Ok(()),
                EpochState::Ready(_) | EpochState::Draining => {}
            }
        }
        let pool = self
            .options
            .pool
            .upgrade()
            .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::Revoked))?;
        // Epoch replacement shares the actual original pool's connection gate;
        // no candidate may open a new physical carrier during old retirement.
        let _serial = self
            .wait(demand, async { Ok(pool.connecting.lock().await) })
            .await?;
        let records = {
            let mut state = self.epoch.lock().unwrap();
            if matches!(&*state, EpochState::Ready(actual) if *actual == epoch) {
                return Ok(());
            }
            // Errors/cancellation cannot turn the old epoch into live admission.
            *state = EpochState::Draining;
            self.records.lock().unwrap().clone()
        };
        if let Some(pool) = self.options.pool.upgrade() {
            let retired = pool.cached.lock().unwrap().take();
            drop(retired);
        }
        let end = demand
            .deadline
            .unwrap_or_else(|| Instant::now() + std::time::Duration::from_secs(30));
        if !self
            .wait(demand, self.retire_originals(records, end))
            .await?
        {
            return Err(error(self.instance, Stage::Close, Reason::Capacity));
        }
        if !self.records.lock().unwrap().is_empty() || self.budget.retained() != 0 {
            return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
        }
        *self.epoch.lock().unwrap() = EpochState::Ready(epoch);
        Ok(())
    }
}
