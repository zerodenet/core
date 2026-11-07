//! Existing claim roots use the same configured original when installed.
use super::*;
impl TrojanAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        let OutboundLeafInput::Proxy {
            outbound,
            endpoint,
            source_dir,
            ..
        } = input
        else {
            return None;
        };
        let options = outbound_options(outbound.tag(), endpoint, &outbound.protocol)?;
        let leaf =
            TrojanOutboundLeaf::from_options_refs(source_dir, options, self.mux_pool.clone());
        let (udp, udp_relay) = crate::adapters::udp::transport_operations(
            Some(endpoint),
            !(self.native_node_dns && component::native_input(outbound)),
            leaf.as_ref(),
        );
        let (tcp, tcp_relay) = crate::adapters::tcp::transport_operations(leaf);
        let tcp = if self.native_node_dns && component::native_input(outbound) {
            Ok(std::sync::Arc::new(tcp_owned::Connect {
                tag: outbound.tag().into(),
                server: endpoint.0.into(),
                port: endpoint.1,
            })
                as std::sync::Arc<
                    dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation,
                >)
        } else {
            tcp
        };
        Some(OutboundLeafClaim {
            tcp_path: TCP_PATH,
            tcp: Some(tcp),
            tcp_relay: Some(tcp_relay),
            udp: Some(udp),
            udp_relay: Some(udp_relay),
            #[cfg(feature = "raw-ip-runtime")]
            packet: None,
            packet_path: None,
        })
    }
}
