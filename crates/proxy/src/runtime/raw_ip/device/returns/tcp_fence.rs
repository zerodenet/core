//! Keep the original Packet return-route priority during prepared TCP commit.
use super::*;
pub(in crate::runtime::raw_ip::device) struct TcpReturnFence<'a> {
    _routes: std::sync::MutexGuard<'a, PacketMetadataVec<(IpAddr, ReturnRoute)>>,
}
impl PacketReturns {
    pub(in crate::runtime::raw_ip::device) fn fence_tcp_delivery(
        &self,
        packet: &[u8],
    ) -> Option<TcpReturnFence<'_>> {
        let routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        if packet::ip_destination(packet).is_some_and(|destination| {
            routes
                .iter()
                .find(|(actual, _)| *actual == destination)
                .map(|(_, value)| value)
                .is_some_and(|route| {
                    (!route.replies.is_closed() || route.touched.elapsed() < IDLE_TIMEOUT)
                })
        }) {
            return None;
        }
        Some(TcpReturnFence { _routes: routes })
    }
}
