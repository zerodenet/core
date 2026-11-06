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
        for record in &records {
            // Cancellation requests are not exits. Observe the actual original
            // initializer/driver and the Host's registration ACK before the
            // reap predicate can become true. These observers never consume a
            // join, grant another task or close a still-held logical view.
            let auxiliary = self
                .grants
                .registration()
                .observe()
                .ok_or_else(|| error(self.instance, Stage::Close, Reason::Revoked))?
                .slots()
                .iter()
                .filter_map(|slot| slot.auxiliary().cloned())
                .find(|witness| record.adoption.matches_auxiliary(witness));
            if let Some(auxiliary) = auxiliary {
                let joined = self
                    .wait(demand, record.adoption.accept_auxiliary())
                    .await?;
                if !joined
                    .as_ref()
                    .is_some_and(|joined| auxiliary.matches_joined(joined))
                {
                    return Err(error(self.instance, Stage::Close, Reason::Internal));
                }
                let released = self
                    .wait(demand, async {
                        Ok(auxiliary.wait_registration_released().await)
                    })
                    .await?;
                if !auxiliary.matches_joined(&released) || !released.registration_released() {
                    return Err(error(self.instance, Stage::Close, Reason::Internal));
                }
            }
            let driver = record.witness.lock().unwrap().clone();
            if let Some(driver) = driver {
                let joined = self.wait(demand, async { Ok(driver.wait().await) }).await?;
                if !driver.matches_joined(&joined) {
                    return Err(error(self.instance, Stage::Close, Reason::Internal));
                }
                record.receive(joined);
                let released = self
                    .wait(demand, async {
                        Ok(driver.wait_registration_released().await)
                    })
                    .await?;
                if !driver.matches_joined(&released) || !released.registration_released() {
                    return Err(error(self.instance, Stage::Close, Reason::Internal));
                }
            }
        }
        self.wait(demand, self.reap_demand(demand.deadline)).await?;
        if !self.records.lock().unwrap().is_empty() || self.budget.retained() != 0 {
            return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
        }
        *self.epoch.lock().unwrap() = Some(epoch);
        Ok(())
    }
}
