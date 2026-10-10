use crate::adapters::direct::DirectAdapter;
use crate::protocol_registry::ClaimedTcpOutboundLeaf;
use crate::runtime::tcp_dispatch::operation::{
    DirectTcpConnectOperation, PreparedTcpConnectOperation,
};
use crate::transport::TcpOutboundFailure;

struct ClaimedDirectTcpLeaf {
    tag: String,
    dial_policy: zero_traits::DialPolicy,
}

impl<'a> ClaimedTcpOutboundLeaf<'a> for ClaimedDirectTcpLeaf {
    fn prepare_tcp_connect(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation>, TcpOutboundFailure> {
        zero_platform_tokio::validate_dial_policy(&self.dial_policy).map_err(|error| {
            TcpOutboundFailure {
                stage: "validate_dial_policy",
                error: zero_engine::EngineError::Io(error),
                upstream_endpoint: None,
                network: None,
            }
        })?;
        Ok(Box::new(DirectTcpConnectOperation {
            tag: self.tag.clone(),
            dial_policy: self.dial_policy.clone(),
        }))
    }
}

impl DirectAdapter {
    pub(super) fn claim_tcp_outbound_leaf_impl<'a>(
        &self,
        tag: String,
        dial_policy: zero_traits::DialPolicy,
    ) -> Box<dyn ClaimedTcpOutboundLeaf<'a> + 'a> {
        Box::new(ClaimedDirectTcpLeaf { tag, dial_policy })
    }
}
