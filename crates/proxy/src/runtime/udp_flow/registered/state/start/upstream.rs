use super::super::model::RegisteredUdpState;
use crate::runtime::udp_flow::{
    registered::upstream::UpstreamAssociationSend, result::FlowFailure,
};
impl RegisteredUdpState {
    pub(crate) async fn start_upstream_udp_flow(
        &mut self,
        inbound_tag: &str,
        request: UpstreamAssociationSend<'_>,
    ) -> Result<usize, FlowFailure> {
        self.upstream
            .start_upstream_flow(inbound_tag, request)
            .await
    }
}
