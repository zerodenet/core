//! An egress/Node change cannot run a second physical epoch alongside the old.
use super::*;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Epoch(
    zero_core::component::NetworkGeneration,
    Option<zero_platform_tokio::contract::dns::DnsGeneration>,
);
impl Managed {
    pub(super) async fn admit_epoch(
        &self,
        demand: &Demand,
        dns: Option<zero_platform_tokio::contract::dns::DnsGeneration>,
    ) -> Result<(), ChannelError> {
        let epoch = Epoch(demand.binding.network_generation, dns);
        {
            let mut current = self.epoch.lock().unwrap();
            match *current {
                None => {
                    *current = Some(epoch);
                    return Ok(());
                }
                Some(actual) if actual == epoch => return Ok(()),
                Some(_) => {}
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
        if *self.epoch.lock().unwrap() == Some(epoch) {
            return Ok(());
        }
        let records = self.records.lock().unwrap().clone();
        for record in &records {
            record.stop.cancel();
            record.adoption.request_retirement();
        }
        if let Some(pool) = self.options.pool.upgrade() {
            let retired = pool.cached.lock().unwrap().take();
            drop(retired);
        }
        self.wait(demand, self.reap_demand(demand.deadline)).await?;
        if !self.records.lock().unwrap().is_empty() || self.budget.retained() != 0 {
            return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
        }
        *self.epoch.lock().unwrap() = Some(epoch);
        Ok(())
    }
}
