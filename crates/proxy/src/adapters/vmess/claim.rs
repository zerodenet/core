//! Compatibility projections retain only execution not owned by standard roles.
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
        let streams =
            !matches!(
                &outbound.protocol,
                OutboundProtocolConfig::Vmess { grpc: Some(_), .. }
            ) || component::grpc::streams_selected(outbound, Some(config), self.native_node_dns);
        if self.native_node_dns
            && streams
            && self.production_selected(outbound, Some(config), true)
            && (!component::owned_input(outbound)
                || !crate::adapters::mux_prefix::used_by_relay(config, outbound.tag()))
        {
            // No compatibility leaf may acquire the same native pool again.
            return None;
        }
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
            None
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
