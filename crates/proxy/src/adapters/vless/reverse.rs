//! Only project virtual-outbound config into protocol-owned portal state.
mod service;
mod udp;
use super::*;
use crate::protocol_registry::ClaimedTcpOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::{
    LogicalTcpConnectOperation, PreparedTcpConnectOperation,
};
use crate::transport::TcpOutboundFailure;

struct ClaimedPortal {
    tag: String,
    portal: ::vless::reverse::Portal,
}
impl<'a> ClaimedTcpOutboundLeaf<'a> for ClaimedPortal {
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure> {
        let portal = self.portal.clone();
        Ok(Box::new(LogicalTcpConnectOperation {
            tag: self.tag.clone(),
            open: std::sync::Arc::new(move |session| {
                let portal = portal.clone();
                Box::pin(async move {
                    portal
                        .open_tcp(&session)
                        .map(crate::transport::TcpRelayStream::new)
                })
            }),
        }))
    }
}

pub(super) fn claim<'a>(
    runtime: &VlessTransportRuntime,
    outbound: &'a zero_config::OutboundConfig,
) -> Option<OutboundLeafClaim<'a>> {
    if !matches!(outbound.protocol, OutboundProtocolConfig::VlessReverse) {
        return None;
    }
    Some(OutboundLeafClaim {
        tcp_path: TCP_PATH,
        tcp: Box::new(ClaimedPortal {
            tag: outbound.tag().to_owned(),
            portal: runtime.reverse_portal(outbound.tag()),
        }),
        udp: Some(Box::new(ClaimedPortal {
            tag: outbound.tag().to_owned(),
            portal: runtime.reverse_portal(outbound.tag()),
        })),
        packet_path: None,
    })
}
