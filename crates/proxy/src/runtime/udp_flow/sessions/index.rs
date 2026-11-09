use zero_core::Address;

use crate::runtime::udp_flow::outbound::UdpFlowOutbound;

use super::model::{UdpFlowKey, UdpSessionFlows, UdpUpstreamResponseKey};

impl UdpSessionFlows {
    #[cfg(feature = "upstream-association-runtime")]
    pub(crate) fn upstream_response_session_id(
        &self,
        outbound_tag: &str,
        target: &Address,
        port: u16,
    ) -> Option<u64> {
        let lookup = super::model::ResponseLookup {
            tag: outbound_tag,
            target,
            port,
        };
        self.upstream_by_response
            .get(&lookup as &dyn super::model::ResponseIdentity)
            .and_then(|key| self.flows.get(key))
            .map(|flow| flow.session.id)
            .or_else(|| self.single_tagged_upstream_flow_session_id(outbound_tag))
    }

    pub(super) fn index_flow(
        &mut self,
        key: &UdpFlowKey,
        session: &super::UdpFlowSession,
        outbound: &UdpFlowOutbound,
    ) {
        let index_keys = outbound.index_keys();
        if let Some(tag) = index_keys.upstream_response_tag {
            self.upstream_by_response.insert(
                UdpUpstreamResponseKey::from_session(tag, session, key.port),
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
        if let Some(tag) = index_keys.upstream_response_tag {
            let response_key = super::model::ResponseLookup {
                tag,
                target: response_target,
                port: key.port,
            };
            if self
                .upstream_by_response
                .get(&response_key as &dyn super::model::ResponseIdentity)
                == Some(key)
            {
                self.upstream_by_response
                    .remove(&response_key as &dyn super::model::ResponseIdentity);
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
