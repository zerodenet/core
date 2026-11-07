//! The same inert adapter projection, consumed as data before native Source bind.
use super::*;
use crate::runtime::udp_flow::packet_path::{PacketPathCarrierDescriptor, UdpDatagramSource};

/// Same cold material group, without an executor, TaskScope, grant or provider.
/// The Source must separately bind its actual canonical slot/caller/current
/// plan. Neither this value nor a Datagram role authorizes native I/O.
#[derive(Clone)]
pub(crate) struct ColdPacketPathProjection {
    pub(crate) descriptor: PacketPathCarrierDescriptor,
    pub(crate) associated: bool,
    pub(crate) formatter: Option<UdpDatagramSource>,
}
impl ProtocolRegistry {
    /// Only registration-proven pure projections are called. Other protocols
    /// return None before their claim can reread material or mutate a pool.
    /// Keep the returned actual formatter Arc family through the later build;
    /// it is not a per-tag process-wide association or a fresh second Session.
    pub(crate) fn prepare_cold_packet_path_projection(
        &self,
        config: &RuntimeConfig,
        leaf: ResolvedLeafOutbound<'_>,
    ) -> Result<Option<ColdPacketPathProjection>, EngineError> {
        let ResolvedLeafOutbound::Proxy { identity } = &leaf else {
            return Ok(None);
        };
        let outbound = config
            .outbounds
            .get(identity.config_index())
            .ok_or_else(|| {
                EngineError::Io(std::io::Error::other(
                    "cold packet path index is outside its actual configuration",
                ))
            })?;
        let protocol = outbound.protocol.protocol_name();
        let entry = self
            .outbound_protocol_entry(protocol)
            .ok_or_else(|| unsupported_outbound_leaf(protocol))?;
        if !entry.pure_packet_carrier {
            return Ok(None);
        }
        // Protocol-private grammar still determines whether this exact input
        // has the old capability (notably UDP-capable plugins return None).
        let claimed = self.claim_outbound_leaf(config, leaf)?;
        let Some(operation) = claimed.udp_packet_path() else {
            return Ok(None);
        };
        let Some(descriptor) = operation.carrier_descriptor() else {
            return Ok(None);
        };
        let associated = operation.supports_associated_relay_carrier();
        let formatter = entry
            .pure_datagram_formatter
            .then(|| operation.datagram_source())
            .flatten();
        // Do not retain/expose the old operation executor to the Source. It
        // selects its same configured canonical Datagram slot at actual open.
        Ok(Some(ColdPacketPathProjection {
            descriptor,
            associated,
            formatter,
        }))
    }
    pub(crate) fn prepare_cold_datagram_source(
        &self,
        config: &RuntimeConfig,
        leaf: ResolvedLeafOutbound<'_>,
    ) -> Result<Option<UdpDatagramSource>, EngineError> {
        Ok(self
            .prepare_cold_packet_path_projection(config, leaf)?
            .and_then(|material| material.formatter))
    }
}
