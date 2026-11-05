use std::sync::Arc;

use tokio::sync::OnceCell;

use super::{RawIpDevicePool, MAX_RAW_IP_DEVICES};

#[test]
fn shutdown_reports_stopped_instead_of_not_started() {
    let pool = RawIpDevicePool::default();
    assert!(pool.health_snapshot("wg", 0, [1; 32]).is_none());
    pool.request_shutdown();
    assert_eq!(
        pool.health_snapshot("wg", 0, [1; 32]).unwrap().state,
        zero_api::OutboundDeviceHealthState::Stopped,
    );
}

#[test]
fn unresolved_endpoint_is_reported_only_for_matching_configuration() {
    let pool = RawIpDevicePool::default();
    pool.mark_endpoint_unresolved("wg", 0, [2; 32]);
    assert!(pool.health_snapshot("wg", 0, [1; 32]).is_none());
    let snapshot = pool.health_snapshot("wg", 0, [2; 32]).unwrap();
    assert_eq!(
        snapshot.state,
        zero_api::OutboundDeviceHealthState::EndpointUnresolved
    );
    assert!(snapshot.endpoint_resolution_failed);
    pool.clear_endpoint_unresolved("wg", 0, [1; 32]);
    assert!(pool.health_snapshot("wg", 0, [2; 32]).is_some());
    pool.clear_endpoint_unresolved("wg", 0, [2; 32]);
    assert!(pool.health_snapshot("wg", 0, [2; 32]).is_none());
}

#[test]
fn staged_raw_ip_devices_reject_over_limit_without_publishing() {
    let pool = RawIpDevicePool::default();
    let mut staged = pool.begin_stage();
    for index in 0..MAX_RAW_IP_DEVICES {
        staged
            .insert(
                "wg".to_owned(),
                index,
                [1; 32],
                1,
                Arc::new(OnceCell::new()),
                false,
                false,
            )
            .unwrap();
    }
    assert!(staged
        .insert(
            "wg".to_owned(),
            MAX_RAW_IP_DEVICES,
            [1; 32],
            1,
            Arc::new(OnceCell::new()),
            false,
            false,
        )
        .is_err());
    assert_eq!(staged.len(), MAX_RAW_IP_DEVICES);
    assert!(pool.cell_for("wg", 0, [1; 32], 1).is_err());
}

#[cfg(feature = "wireguard")]
impl RawIpDevicePool {
    pub(crate) fn test_current_devices(
        &self,
        tags: &[String],
    ) -> Vec<Arc<super::SharedRawIpDevice>> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut devices: Vec<_> = state
            .entries
            .iter()
            .filter(|((tag, _), _)| tags.contains(tag))
            .filter_map(|(_, entry)| entry.cell.get().cloned())
            .collect();
        devices.sort_by_key(|device| device.incarnation);
        devices
    }
}
