use std::sync::Arc;

use super::udp::WireguardRawIpPlan;
use crate::{
    protocol_registry::ClaimedTcpOutboundLeaf,
    runtime::{
        raw_ip::RawIpDevicePool,
        tcp_dispatch::operation::{PreparedTcpConnectOperation, RawIpTcpOperation},
    },
    transport::TcpOutboundFailure,
};

pub(super) struct WireguardTcpLeaf {
    pub(super) tag: String,
    pub(super) plan: Arc<WireguardRawIpPlan>,
    pub(super) identity: [u8; 32],
    pub(super) pool: Arc<RawIpDevicePool>,
}

impl ClaimedTcpOutboundLeaf<'_> for WireguardTcpLeaf {
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure> {
        Ok(Box::new(RawIpTcpOperation {
            tag: self.tag.clone(),
            identity: self.identity,
            plan: self.plan.clone(),
            pool: self.pool.clone(),
        }))
    }
}
