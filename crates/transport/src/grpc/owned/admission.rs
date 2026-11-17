//! Same actual gRPC pool: finite prefix lanes and atomic original admission.
use super::{state::Physical, *};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use zero_platform_tokio::controlled_carrier::CarrierGrant;
impl Managed {
    pub(super) async fn serialize_binding(
        &self,
        demand: &Demand,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, ChannelError> {
        let lane = {
            let mut lanes = self.openings.lock().unwrap();
            lanes.retain(|_, lane| lane.strong_count() != 0);
            if let Some(lane) = lanes
                .get(&demand.binding)
                .and_then(std::sync::Weak::upgrade)
            {
                lane
            } else {
                lanes.remove(&demand.binding);
                lanes.retain(|_, lane| lane.strong_count() != 0);
                if lanes.len() >= self.options.carrier_limit.get() {
                    return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
                }
                let lane = Arc::new(tokio::sync::Mutex::new(()));
                lanes.insert(demand.binding.clone(), Arc::downgrade(&lane));
                lane
            }
        };
        self.wait(demand, async { Ok(lane.lock_owned().await) })
            .await
    }
    pub(super) fn install_original(
        self: &Arc<Self>,
        demand: &Demand,
        attempted: &mut Option<Arc<Physical>>,
    ) -> Result<(CarrierGrant, Arc<Physical>, super::state::Body), ChannelError> {
        let epoch = self.epoch.lock().unwrap();
        self.verify_epoch_state(
            &epoch,
            &demand.binding,
            self.upstream.carrier_dns_generation(),
        )?;
        if !self.ready() {
            return Err(error(self.instance, Stage::Prepare, Reason::Revoked));
        }
        let mut records = self.records.lock().unwrap();
        if records.len() >= self.options.carrier_limit.get()
            || records.iter().any(|record| {
                (record.stop.is_cancelled() || record.done.load(Ordering::Acquire))
                    && !record.closed()
            })
        {
            return Err(error(self.instance, Stage::Prepare, Reason::Capacity));
        }
        let totals = self.totals.lock().unwrap().clone();
        let total = totals
            .as_ref()
            .map(|totals| totals.physical(self.instance))
            .transpose()?;
        let grant = match self.grants.reserve() {
            Ok(grant) => grant,
            Err(source) => {
                if let Some(total) = &total {
                    total.refuse_unassigned()?;
                }
                return Err(source);
            }
        };
        let prepared_protocols = (|| {
            Ok::<_, ChannelError>(
                if self.options.selected_background && grant.adoption().has_original_root() {
                    let limit = self
                        .upstream
                        .pending_carrier_capacity_for(&demand.binding)
                        .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::StalePlan))?;
                    if limit == 0 {
                        None
                    } else {
                        let memory = self
                            .resources
                            .upgrade()
                            .ok_or_else(|| error(self.instance, Stage::Prepare, Reason::Revoked))?
                            .packet_driver_metadata()?;
                        Some(crate::protocol_pending::Children::new(memory, limit))
                    }
                } else {
                    None
                },
            )
        })();
        let protocols = match prepared_protocols {
            Ok(prepared) => prepared,
            Err(source) => {
                if let Some(total) = &total {
                    total.refuse_unassigned()?;
                }
                return Err(source);
            }
        };
        let physical = Arc::new(Physical {
            owner: state::WeakManaged::new(self),
            binding: demand.binding.clone(),
            pending: Mutex::new(Vec::new()),
            protocols,
            adoption: grant.adoption(),
            stop: grant.cancellation(),
            view: Mutex::new(None),
            witness: Mutex::new(None),
            received: Mutex::new(None),
            accepted: AtomicBool::new(false),
            done: AtomicBool::new(false),
        });
        records.push(physical.clone());
        *attempted = Some(physical.clone());
        let body = super::state::Body(physical.clone());
        if let Some(total) = total {
            total.install(physical.clone())?;
        }
        Ok((grant, physical, body))
    }
}
