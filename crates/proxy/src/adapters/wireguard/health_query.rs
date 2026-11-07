//! The adapter interprets its own peers; the control host only stores scalars.
use super::*;
use crate::adapters::devices::HealthRow;
use zero_config::RuntimeConfig;
impl WireguardAdapter {
    pub(crate) fn health_row_count(&self, config: &RuntimeConfig) -> std::io::Result<usize> {
        config.outbounds.iter().try_fold(0usize, |total, outbound| {
            let count = match &outbound.protocol {
                OutboundProtocolConfig::Wireguard { peers, .. } => peers.len(),
                _ => 0,
            };
            total
                .checked_add(count)
                .ok_or_else(|| std::io::Error::other("device health row count overflow"))
        })
    }
    pub(crate) fn fill_health_rows(&self, config: &RuntimeConfig, rows: &mut Vec<HealthRow>) {
        for (outbound_index, outbound) in config.outbounds.iter().enumerate() {
            let OutboundProtocolConfig::Wireguard { peers, .. } = &outbound.protocol else {
                continue;
            };
            let Some(identity) = protocol_identity(&outbound.protocol) else {
                continue;
            };
            let identity = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&outbound.tag)
                .filter(|cached| cached.identity == identity)
                .map_or(identity, |cached| cached.device_identity);
            for peer_index in 0..peers.len() {
                let facts = self
                    .pool
                    .health_snapshot_unlabeled(&outbound.tag, peer_index, identity)
                    .unwrap_or_else(|| zero_api::OutboundDeviceHealthSnapshot {
                        peer_index,
                        ..Default::default()
                    });
                rows.push(HealthRow {
                    outbound: outbound_index,
                    facts,
                });
            }
        }
    }
}
