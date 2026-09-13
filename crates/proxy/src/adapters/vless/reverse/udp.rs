use super::ClaimedPortal;
use crate::{
    protocol_registry::ClaimedUdpFlowLeaf,
    runtime::{
        udp_dispatch::{
            operation::{LogicalUdpOperation, PreparedUdpFlowOperation},
            FlowFailure,
        },
        udp_flow::registered::LogicalConnection,
    },
};
impl<'a> ClaimedUdpFlowLeaf<'a> for ClaimedPortal {
    fn prepare_udp_flow(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let portal = self.portal.clone();
        Ok(Box::new(LogicalUdpOperation {
            tag: self.tag.clone(),
            open: std::sync::Arc::new(move |session| {
                portal
                    .open_udp_connection(session)
                    .map(LogicalConnection::from_flow)
            }),
        }))
    }
}
