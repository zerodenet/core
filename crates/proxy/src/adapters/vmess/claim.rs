//! Existing compatibility claims retain the actual native root.
use super::*;
impl VmessAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Proxy {
            config,
            outbound,
            endpoint,
            source_dir,
        } = input
        else {
            return None;
        };
        let options = outbound_options(outbound.tag(), endpoint, &outbound.protocol)?;
        let leaf = VmessOutboundLeaf::from_options_refs(source_dir, options, &self.runtime);
        let (udp, udp_relay) = crate::adapters::udp::transport_operations(
            Some(endpoint),
            !component::grpc::selected(outbound, Some(config), self.native_node_dns),
            leaf.as_ref(),
        );
        let (tcp, tcp_relay) =
            if component::grpc::streams_selected(outbound, Some(config), self.native_node_dns) {
                // Every configured caller now selects the same Node Managed owner.
                // No compatibility TCP executor may create a second original.
                (None, None)
            } else {
                let (tcp, tcp_relay) = crate::adapters::tcp::transport_operations(leaf);
                (Some(tcp), Some(tcp_relay))
            };
        let tcp = if self.native_node_dns && component::owned_input(outbound) {
            Some(Ok(std::sync::Arc::new(tcp_owned::Connect {
                tag: outbound.tag().into(),
                server: endpoint.0.into(),
                port: endpoint.1,
            })
                as std::sync::Arc<
                    dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation,
                >))
        } else {
            tcp
        };
        Some(OutboundLeafClaim {
            tcp_path: TCP_PATH,
            tcp,
            tcp_relay,
            udp: Some(udp),
            udp_relay: Some(udp_relay),
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path: None,
        })
    }
}
