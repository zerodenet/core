//! Transfer only the original before-bind Source-declared memory slots.
use super::*;
impl Wire {
    pub(in crate::runtime::inbound_operation::raw_ip) fn take_declared_proxy_memory(
        &mut self,
        resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let Some(endpoint) = self.endpoint.as_ref() else {
            return Ok(());
        };
        let peers = endpoint.peers.borrow().clone();
        let mut ordinal = 0usize;
        for (index, carrier) in peers.carriers.iter().enumerate() {
            if carrier.is_none()
                && peers
                    .prepared_outer
                    .get(index)
                    .and_then(Option::as_ref)
                    .is_none()
            {
                self.proxy_reads.push(None);
                self.proxy_codecs.push(Vec::new());
                continue;
            }
            let layouts = peers
                .outer_memory
                .get(index)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::InvalidConfig,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                })?;
            self.proxy_reads.push(Some(
                resources.take_packet_datagram_receive_workspace(0, ordinal)?,
            ));
            ordinal += 1;
            let mut codecs = Vec::new();
            for layout in layouts.iter() {
                let wire = resources.take_packet_datagram_receive_workspace(0, ordinal)?;
                ordinal += 1;
                let mut buffers = Vec::new();
                for length in layout.iter() {
                    let actual = resources.take_packet_datagram_receive_workspace(0, ordinal)?;
                    ordinal += 1;
                    if actual.bytes() != length.get() {
                        return Err(zero_platform_tokio::contract::ChannelError::new(
                            zero_platform_tokio::contract::Stage::Prepare,
                            zero_platform_tokio::contract::Reason::StalePlan,
                            zero_platform_tokio::contract::Impact::Component,
                        ));
                    }
                    buffers.push(actual);
                }
                codecs.push(crate::protocol_registry::packet_memory::PacketCodecReceiveMemory::from_declared(wire,buffers).map_err(|source|zero_platform_tokio::contract::ChannelError::new(zero_platform_tokio::contract::Stage::Prepare,zero_platform_tokio::contract::Reason::InvalidConfig,zero_platform_tokio::contract::Impact::Component).with_source(source))?);
            }
            self.proxy_codecs.push(codecs);
        }
        Ok(())
    }
}
