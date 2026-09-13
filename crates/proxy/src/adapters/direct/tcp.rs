use crate::adapters::direct::DirectAdapter;
use crate::protocol_registry::ClaimedTcpOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::{
    DirectTcpConnectOperation, PreparedTcpConnectOperation,
};
use crate::transport::TcpOutboundFailure;

struct ClaimedDirectTcpLeaf {
    tag: String,
}

impl<'a> ClaimedTcpOutboundLeaf<'a> for ClaimedDirectTcpLeaf {
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure> {
        Ok(Box::new(DirectTcpConnectOperation {
            tag: self.tag.clone(),
        }))
    }
}

impl DirectAdapter {
    pub(super) fn claim_tcp_outbound_leaf_impl<'a>(
        &self,
        tag: String,
    ) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a> {
        Box::new(ClaimedDirectTcpLeaf { tag })
    }
}
