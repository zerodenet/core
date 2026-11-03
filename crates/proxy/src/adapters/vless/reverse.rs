//! Only project virtual-outbound config into protocol-owned portal state.
mod service;
use super::*;
use crate::runtime::tcp_dispatch::operation::LogicalTcpConnectOperation;

pub(super) fn claim<'a>(
    runtime: &VlessTransportRuntime,
    outbound: &'a zero_config::OutboundConfig,
) -> Option<OutboundLeafClaim<'a>> {
    if !matches!(outbound.protocol, OutboundProtocolConfig::VlessReverse) {
        return None;
    }
    let portal = runtime.reverse_portal(outbound.tag());
    let tcp = LogicalTcpConnectOperation {
        tag: outbound.tag().to_owned(),
        open: std::sync::Arc::new(move |session| {
            let portal = portal.clone();
            Box::pin(async move {
                portal
                    .open_tcp(&session)
                    .map(crate::transport::TcpRelayStream::new)
            })
        }),
    };
    Some(OutboundLeafClaim {
        tcp_path: TCP_PATH,
        tcp: Some(Ok(std::sync::Arc::new(tcp))),
        tcp_relay: None,
        udp: Some(Ok(Box::new(
            crate::runtime::udp_dispatch::operation::LogicalUdpOperation {
                tag: outbound.tag().to_owned(),
                open: std::sync::Arc::new({
                    let portal = runtime.reverse_portal(outbound.tag());
                    move |session| {
                        portal
                            .open_udp_connection(session)
                            .map(crate::runtime::udp_flow::registered::LogicalConnection::from_flow)
                    }
                }),
            },
        ))),
        udp_relay: None,
        #[cfg(feature = "raw-ip-runtime")]
        packet: None,
        packet_path: None,
    })
}
