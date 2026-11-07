//! Install only the exact current Source grant into this cold graph.
use super::*;
impl WireguardAdapter {
    /// Called only alongside actual Source-origin projection by the ordinary
    /// host. The exact pending graph gets its same Source allocation view;
    /// this never reconstructs a memory Root from a tag or component identity.
    pub(crate) fn install_source_memory(
        &self,
        config: &zero_config::RuntimeConfig,
        tag: &str,
        memory: zero_tun::packet_memory::PacketMemoryView,
    ) -> Result<(), EngineError> {
        let linked = config.outbounds.iter().any(|outbound| {
            matches!(&outbound.protocol,
            OutboundProtocolConfig::Wireguard {inbound_tag:Some(source),outer_udp_proxy:None,..}
                if source==tag)
        });
        if !linked {
            return Ok(());
        }
        if !self
            .packet_return_totals
            .get()
            .is_some_and(|root| root.owns_view(&memory))
        {
            return Err(invalid(
                "Source memory belongs to another actual Kernel Root",
            ));
        }
        let pending = PendingEndpoints::staged(&self.pending_endpoints, config)
            .and_then(|candidate| candidate.get(tag).cloned())
            .ok_or_else(|| invalid("Source memory has no same actual linked candidate"))?;
        let graph = pending
            .actual_graph()
            .ok_or_else(|| invalid("actual linked memory factory owner was released"))?;
        graph.install_source_memory(memory)
    }
}
