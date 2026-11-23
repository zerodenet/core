//! Preparation coordination belongs to this actual pool and its finite originals.
//! Different shorter prefixes never wait on a lock held by their own consumer.
use super::{epoch::EpochState, failure, ManagedPool, Physical};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{network::UpstreamBinding, ChannelError, Reason, Stage},
    controlled_carrier::CarrierGrant,
};
impl ManagedPool {
    /// Serialize cache publication only for an equal physical prefix. The weak
    /// lane index retains no old configuration, resources, tasks or caller.
    /// Its size cannot exceed the existing physical-carrier ceiling.
    pub async fn serialize_binding(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, crate::RuntimeError> {
        self.check(&self.options.profile)?;
        let lane = {
            let mut lanes = self.openings.lock().unwrap();
            lanes.retain(|_, lane| lane.strong_count() != 0);
            if let Some(lane) = lanes.get(binding).and_then(std::sync::Weak::upgrade) {
                lane
            } else {
                // Its last guard may have returned after the first census.
                // A dead equal key is not another occupied coordination lane.
                lanes.remove(binding);
                lanes.retain(|_, lane| lane.strong_count() != 0);
                if lanes.len() >= self.options.limit.get() {
                    return Err(std::io::Error::other(failure(
                        self.instance,
                        Stage::Prepare,
                        Reason::Capacity,
                    ))
                    .into());
                }
                let lane = Arc::new(tokio::sync::Mutex::new(()));
                lanes.insert(binding.clone(), Arc::downgrade(&lane));
                lane
            }
        };
        tokio::select! { biased;
            _=self.cancel.cancelled()=>Err(std::io::Error::other(failure(self.instance,Stage::Prepare,Reason::Revoked)).into()),
            result=tokio::time::timeout(self.options.open_budget,lane.lock_owned())=>
                result.map_err(|source|std::io::Error::new(std::io::ErrorKind::TimedOut,source).into()),
        }
    }
    /// Epoch admission and insertion of the real original record are one
    /// synchronous operation. A draining transition cannot miss a late record.
    pub(super) fn install_original(
        self: &Arc<Self>,
        binding: &UpstreamBinding,
        scoped_preparation: bool,
    ) -> Result<(CarrierGrant, Arc<Physical>), ChannelError> {
        let epoch = self.epoch.lock().unwrap();
        let current = (
            binding.network_generation,
            self.upstream.carrier_dns_generation(),
        );
        if !self.ready()
            || binding.dial != self.options.binding.dial
            || !matches!(&*epoch, EpochState::Ready(actual) if *actual == current)
        {
            return Err(failure(self.instance, Stage::Prepare, Reason::StalePlan));
        }
        let mut records = self.records.lock().unwrap();
        if records.len() >= self.options.limit.get()
            || records.iter().any(|record| {
                (record.stop.is_cancelled()
                    || record.done.load(std::sync::atomic::Ordering::Acquire))
                    && !record.confirmed_closed()
            })
        {
            // A vacant budget slot cannot stand in for the original close of
            // a failed/expired carrier. Existing live cache users still run;
            // new physical IO waits for the retained retirement census.
            return Err(failure(self.instance, Stage::Prepare, Reason::Capacity));
        }
        let mut grant = self.grants.reserve()?;
        let view = if scoped_preparation {
            grant.claim_preparing_driver_view().map(|(view, _)| view)
        } else {
            grant.claim_driver_view()
        }?;
        let record = Physical::new(
            grant.adoption(),
            grant.cancellation(),
            view,
            self,
            binding.clone(),
        )?;
        records.push(record.clone());
        Ok((grant, record))
    }
}
