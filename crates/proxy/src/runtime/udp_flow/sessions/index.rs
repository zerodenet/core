use std::net::SocketAddr;

use zero_core::Address;

use crate::runtime::udp_flow::outbound::UdpFlowOutbound;

use super::model::{UdpFlowKey, UdpSessionFlows, UdpUpstreamResponseKey};

impl UdpSessionFlows {
    pub(crate) fn direct_response_session_id(&self, sender: SocketAddr) -> Option<u64> {
        self.direct_by_sender
            .get(&sender)
            .and_then(|key| self.flows.get(key))
            .map(|flow| flow.session.id)
    }

    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn upstream_response_session_id(
        &self,
        outbound_tag: &str,
        target: &Address,
        port: u16,
    ) -> Option<u64> {
        self.upstream_by_response
            .get(&UdpUpstreamResponseKey::new(outbound_tag, target, port))
            .and_then(|key| self.flows.get(key))
            .map(|flow| flow.session.id)
            .or_else(|| self.single_tagged_upstream_flow_session_id(outbound_tag))
    }

    pub(super) fn index_flow(
        &mut self,
        key: &UdpFlowKey,
        response_target: &Address,
        outbound: &UdpFlowOutbound,
    ) {
        let index_keys = outbound.index_keys();
        if let Some(sender) = index_keys.direct_sender {
            self.direct_by_sender.insert(sender, key.clone());
        }

        if let Some(tag) = index_keys.upstream_response_tag {
            self.upstream_by_response.insert(
                UdpUpstreamResponseKey::new(tag, response_target, key.port),
                key.clone(),
            );
        }
    }

    pub(super) fn unindex_flow(
        &mut self,
        key: &UdpFlowKey,
        response_target: &Address,
        outbound: &UdpFlowOutbound,
    ) {
        let index_keys = outbound.index_keys();
        if let Some(sender) = index_keys.direct_sender {
            if self.direct_by_sender.get(&sender) == Some(key) {
                self.direct_by_sender.remove(&sender);
            }
        }

        if let Some(tag) = index_keys.upstream_response_tag {
            let response_key = UdpUpstreamResponseKey::new(tag, response_target, key.port);
            if self.upstream_by_response.get(&response_key) == Some(key) {
                self.upstream_by_response.remove(&response_key);
            }
        }
    }

    #[cfg(feature = "upstream-association-runtime")]
    fn single_tagged_upstream_flow_session_id(&self, outbound_tag: &str) -> Option<u64> {
        let mut upstream_flows = self
            .flows
            .values()
            .filter(|flow| flow.outbound.index_keys().upstream_response_tag == Some(outbound_tag));
        let flow = upstream_flows.next()?;
        upstream_flows.next().is_none().then_some(flow.session.id)
    }
}
