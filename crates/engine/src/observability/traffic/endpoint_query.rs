//! The existing traffic book/period, with only fixed endpoint counter capture.
use super::*;
use std::sync::{MutexGuard, RwLockReadGuard};
use zero_api::EndpointCounters;
pub(crate) struct EndpointTrafficGuard<'a> {
    entries: RwLockReadGuard<'a, BTreeMap<TrafficScope, Arc<CounterSet>>>,
    _management: MutexGuard<'a, ()>,
}
pub(crate) struct EndpointTrafficRow<'a> {
    pub(crate) counters: EndpointCounters,
    period: MutexGuard<'a, counter::Period>,
}
impl TrafficRegistry {
    pub(crate) fn endpoint_query(&self) -> EndpointTrafficGuard<'_> {
        let management = self
            .management
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = self
            .entries
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        EndpointTrafficGuard {
            entries,
            _management: management,
        }
    }
}
impl EndpointTrafficGuard<'_> {
    pub(crate) fn freeze<'a>(&'a self, id: &str) -> Option<EndpointTrafficRow<'a>> {
        let entry = self.entries.iter().find_map(|(scope, entry)| {
            matches!(scope,TrafficScope::Endpoint{endpoint_id} if endpoint_id==id).then_some(entry)
        })?;
        let period = entry
            .period
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let values = entry.capture();
        let value = |plane: usize, metric: TrafficMetric| {
            let index = metric as usize;
            let available = entry.planes[plane].available.load(Ordering::Acquire)
                & !entry.planes[plane].unobservable.load(Ordering::Acquire);
            (available & (1 << index) != 0)
                .then(|| values[plane][index].saturating_sub(period.baseline[plane][index]))
        };
        Some(EndpointTrafficRow {
            counters: EndpointCounters {
                inner_rx_bytes: value(1, TrafficMetric::RxBytes),
                inner_tx_bytes: value(1, TrafficMetric::TxBytes),
                inner_rx_packets: value(1, TrafficMetric::RxPackets),
                inner_tx_packets: value(1, TrafficMetric::TxPackets),
                outer_rx_bytes: value(2, TrafficMetric::RxBytes),
                outer_tx_bytes: value(2, TrafficMetric::TxBytes),
                outer_rx_packets: value(2, TrafficMetric::RxPackets),
                outer_tx_packets: value(2, TrafficMetric::TxPackets),
                dropped_packets: value(1, TrafficMetric::DroppedPackets),
                active_packet_routes: (entry.routes_available.load(Ordering::Acquire) != 0)
                    .then(|| entry.active_routes.load(Ordering::Relaxed)),
                ..Default::default()
            },
            period,
        })
    }
}
impl EndpointTrafficRow<'_> {
    pub(crate) fn epoch(&self) -> &str {
        &self.period.epoch
    }
    pub(crate) fn started(&self) -> u64 {
        self.period.started
    }
}
