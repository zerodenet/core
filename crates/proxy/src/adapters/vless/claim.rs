//! Compatibility claims dispatch roots through their configured actual role.
use super::*;
impl VlessAdapter {
    pub(crate) fn claim_outbound_leaf_impl<'a>(
        &self,
        input: OutboundLeafInput<'a>,
    ) -> Option<OutboundLeafClaim<'a>> {
        if let OutboundLeafInput::Virtual { outbound } = input {
            return reverse::claim(&self.runtime, outbound);
        }
        let OutboundLeafInput::Proxy {
            config,
            outbound,
            endpoint,
            source_dir,
            ..
        } = input
        else {
            return None;
        };
        if component::grpc::selected(outbound, Some(config), self.native_node_dns) {
            // All enabled ordinary callers use the same role and actual pool;
            // no legacy leaf may allocate a second original alongside it.
            return Some(OutboundLeafClaim {
                tcp_path: TCP_PATH,
                tcp: None,
                tcp_relay: None,
                udp: None,
                udp_relay: None,
                #[cfg(feature = "raw-ip-runtime")]
                packet: None,
                packet_path: None,
            });
        }
        let options = outbound_options(outbound.tag(), endpoint, &outbound.protocol)?;
        let tcp_leaf =
            VlessOutboundLeaf::from_options_refs(source_dir, options.clone(), &self.runtime);
        let packet_identity =
            serde_json::to_string(&(outbound.tag(), endpoint, &outbound.protocol)).ok()?;
        let packet_path = tcp_leaf
            .as_ref()
            .ok()
            .and_then(|leaf| packet_path::operation(&packet_identity, source_dir, leaf));
        let (udp, udp_relay) = crate::adapters::udp::two_stream_transport_operations(
            Some(endpoint),
            tcp_leaf.as_ref(),
        );
        let (tcp, tcp_relay) = crate::adapters::tcp::transport_operations(tcp_leaf);
        let tcp = if self.native_node_dns && component::owned_input(outbound) {
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
            packet_path,
        })
    }
}
