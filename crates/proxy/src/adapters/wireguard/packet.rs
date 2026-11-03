//! Materialize the existing Packet executors once per published profile.
use super::*;
impl WireguardAdapter {
    pub(super) fn packet_operations(
        &self,
        tag: &str,
        identity: [u8; 32],
        plan: Arc<udp::WireguardRawIpPlan>,
    ) -> crate::runtime::packet_route::PacketRouteOperations {
        crate::runtime::packet_route::PacketRouteOperations {
            native: Arc::new(crate::runtime::raw_ip::RawIpPacketOperation {
                translate_source: false,
                tag: tag.to_owned(),
                identity,
                plan: plan.clone(),
                pool: self.pool.clone(),
            }),
            translated: Some(Arc::new(crate::runtime::raw_ip::RawIpPacketOperation {
                translate_source: true,
                tag: tag.to_owned(),
                identity,
                plan: plan.clone(),
                pool: self.pool.clone(),
            })),
            datagram: Some(Arc::new(crate::runtime::raw_ip::RawIpDatagramOperation {
                tag: tag.to_owned(),
                identity,
                plan: plan.clone(),
                pool: self.pool.clone(),
            })),
            flow: Some(crate::runtime::raw_ip::RawIpFlowSource::adapter(
                tag.to_owned(),
                identity,
                plan,
                self.pool.clone(),
            )),
            correlated_returns: false,
        }
    }
}
