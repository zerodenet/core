use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::{Arc, Weak};

use zero_config::ModeConfig;

use super::{EngineRuntimeSnapshot, RouteEvaluation};

const MAX_CACHED_IP_ROUTES: usize = 1_024;

/// Bounded caller-owned memoization of pure IP rule evaluation.
///
/// Only a weak snapshot identity and evaluated route facts are retained. This
/// cache never owns an engine, configuration, resolved outbound or executable
/// operation, and does not replace live target resolution or admission.
#[derive(Default)]
pub struct IpRouteCache {
    epoch: Option<Epoch>,
    entries: HashMap<IpAddr, RouteEvaluation>,
    order: VecDeque<IpAddr>,
    #[cfg(test)]
    hits: usize,
    #[cfg(test)]
    evaluations: usize,
}

struct Epoch {
    snapshot: Weak<EngineRuntimeSnapshot>,
    mode: ModeConfig,
    inbound: Option<String>,
}

impl IpRouteCache {
    pub(super) fn get(
        &mut self,
        snapshot: &Arc<EngineRuntimeSnapshot>,
        mode: &ModeConfig,
        inbound: Option<&str>,
        address: IpAddr,
    ) -> Option<RouteEvaluation> {
        let current = self.epoch.as_ref().is_some_and(|epoch| {
            // The retained Weak keeps this allocation's identity unique even
            // after its snapshot is retired, without retaining its contents.
            epoch.snapshot.as_ptr() == Arc::as_ptr(snapshot)
                && &epoch.mode == mode
                && epoch.inbound.as_deref() == inbound
        });
        if !current {
            self.entries.clear();
            self.order.clear();
            self.epoch = Some(Epoch {
                snapshot: Arc::downgrade(snapshot),
                mode: mode.clone(),
                inbound: inbound.map(str::to_owned),
            });
        }
        let evaluation = self.entries.get(&address).cloned();
        #[cfg(test)]
        if evaluation.is_some() {
            self.hits += 1;
        }
        evaluation
    }

    pub(super) fn insert(&mut self, address: IpAddr, evaluation: RouteEvaluation) {
        #[cfg(test)]
        {
            self.evaluations += 1;
        }
        // Every insertion follows a miss in the same synchronous evaluation.
        // FIFO bounds both structures without an all-entry scan on a hit.
        if self.entries.len() == MAX_CACHED_IP_ROUTES {
            let oldest = self
                .order
                .pop_front()
                .expect("cached route FIFO is present");
            self.entries.remove(&oldest);
        }
        self.order.push_back(address);
        self.entries.insert(address, evaluation);
    }
}

#[cfg(test)]
mod tests;
