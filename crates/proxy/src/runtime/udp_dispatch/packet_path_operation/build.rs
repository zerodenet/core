//! Consume the same actual root and preserve the original Codec suffix order.
use super::*;
impl PreparedDatagramRelayCarrier {
    pub(crate) async fn build(
        &self,
        services: PacketPathExecutionServices,
    ) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
        let start = self
            .layers
            .iter()
            .rposition(|layer| !matches!(layer, PreparedDatagramRelayLayer::Codec(_)))
            .expect("prepared datagram relay carrier has a build layer");
        let services =
            if let Some(PreparedDatagramRelayLayer::Codec(source)) = self.layers.get(start + 1) {
                let endpoint = source.descriptor().endpoint();
                services.with_packet_target(zero_core::request::Destination::new(
                    endpoint.target(),
                    endpoint.port(),
                ))
            } else {
                services
            };
        let business = if !services.has_packet_driver_caller() && services.has_business_invocation()
        {
            Some(
                crate::protocol_registry::packet_memory::business::BusinessMemory::from_services(
                    &services.tcp(),
                )?,
            )
        } else {
            None
        };
        let mut path = match &self.layers[start] {
            PreparedDatagramRelayLayer::Base(operation) => {
                let observed = observed_services(&services, operation.as_ref());
                match crate::component::stream_bridge::datagram::packet_path::StandardPacketPath::build_installed(operation.as_ref(), &observed).await? {
                    Some(actual) => actual,
                    None => operation.build_carrier(observed).await?,
                }
            }
            PreparedDatagramRelayLayer::Associated {
                operation,
                tcp_prefix,
            } => {
                let observed = observed_services(&services, operation.as_ref());
                if let Some(path) = crate::component::stream_bridge::datagram::packet_path::StandardPacketPath::build_associated(
                    &observed, tcp_prefix, operation.carrier_descriptor().ok_or_else(|| unsupported("associated carrier descriptor is absent"))?,
                ).await? {
                    path
                } else {
                let carrier = services
                    .prepare_lazy_tcp_relay_prefix(tcp_prefix.clone())
                    .with_observer(observed.tcp().upstream().observer());
                operation
                    .build_associated_relay_carrier(observed, carrier)
                    .await?
                }
            }
            PreparedDatagramRelayLayer::Codec(_) => unreachable!(),
        };
        for (codec_index, layer) in self.layers[start + 1..].iter().enumerate() {
            let PreparedDatagramRelayLayer::Codec(source) = layer else {
                unreachable!("last associated packet-path layer must be the build root")
            };
            path = crate::runtime::udp_flow::packet_path_chain::carriers::observed::wrap(
                path,
                services
                    .tcp()
                    .outbound_io(&source.descriptor().tag, zero_api::TrafficPlane::Outer),
            );
            path = crate::runtime::udp_flow::packet_path_chain::carriers::encoded::wrap_with_memory(
                path,
                source.clone(),
                services.packet_codec_receive(codec_index)?,
                business.clone(),
            );
        }
        Ok(path)
    }
}

pub(super) fn observed_services(
    services: &PacketPathExecutionServices,
    operation: &dyn PreparedUdpPacketPathOperation,
) -> PacketPathExecutionServices {
    match operation
        .carrier_descriptor()
        .and_then(|descriptor| descriptor.tag)
    {
        Some(tag) => services.clone().with_outbound_io(&tag),
        None => services.clone(),
    }
}
