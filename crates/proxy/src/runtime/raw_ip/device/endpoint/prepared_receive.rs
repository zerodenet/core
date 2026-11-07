//! Exact original Stack consumer and Packet return-route admission together.
use super::*;
pub(crate) struct CorrelatedDeviceReceive<'a> {
    routes: Option<super::super::returns::TcpReturnFence<'a>>,
    tcp: zero_stack::ClientTcpControlReceive<'a>,
}
impl<'a> CorrelatedDeviceReceive<'a> {
    pub(crate) fn release_return_fence(mut self) -> zero_stack::ClientTcpControlReceive<'a> {
        drop(self.routes.take());
        self.tcp
    }
}
impl SharedRawIpDevice {
    pub(crate) async fn prepare_correlated_receive<'a>(
        &'a self,
        packet: &'a [u8],
        read: &zero_tun::packet_memory::NativeReadGrant,
        allow_native: bool,
    ) -> Result<Option<CorrelatedDeviceReceive<'a>>, zero_tun::packet_memory::PacketMemoryError>
    {
        let Some(tcp) = self.tcp.prepare_correlated_owned(packet, read).await? else {
            return Ok(None);
        };
        // All existing return readers release this mutex before awaiting the
        // TCP table. This is table -> routes, with no opposite nested acquire.
        // The new route is linearized after this exact prepared operation.
        let routes = if allow_native {
            let Some(routes) = self.returns.fence_tcp_delivery(packet) else {
                return Ok(None);
            };
            Some(routes)
        } else {
            None
        };
        Ok(Some(CorrelatedDeviceReceive { routes, tcp }))
    }
}
