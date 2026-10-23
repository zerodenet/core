//! Explicit owned branch of the same selected neutral relay recipe.
use super::*;
use crate::runtime::udp_dispatch::packet_owner::{PacketCarrierDemand, PacketCarrierOwnerSlot};

pub(super) async fn build(
    carrier: &PreparedDatagramRelayCarrier,
    services: PacketPathExecutionServices,
    owner: Arc<PacketCarrierOwnerSlot>,
    demand: PacketCarrierDemand,
) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
    // Preflight the complete graph. Associated carriers still require their
    // original control-stream owner; never acquire a root and then discover it.
    if carrier
        .layers
        .iter()
        .any(|layer| matches!(layer, PreparedDatagramRelayLayer::Associated { .. }))
    {
        return Err(unsupported(
            "owned associated relay control is not migrated",
        ));
    }
    let Some(PreparedDatagramRelayLayer::Base(operation)) = carrier.layers.first() else {
        return Err(unsupported("owned relay has no physical root"));
    };
    let mut path = operation
        .build_owned_carrier(
            observed_services(&services, operation.as_ref()),
            owner,
            demand,
        )
        .await?;
    for layer in &carrier.layers[1..] {
        let PreparedDatagramRelayLayer::Codec(source) = layer else {
            unreachable!()
        };
        path = crate::runtime::udp_flow::packet_path_chain::carriers::observed::wrap(
            path,
            services
                .tcp()
                .outbound_io(&source.descriptor().tag, zero_api::TrafficPlane::Outer),
        );
        path = crate::runtime::udp_flow::packet_path_chain::carriers::encoded::wrap(
            path,
            source.clone(),
        );
    }
    Ok(path)
}

pub(super) fn tcp_prefix_key(prefix: &PreparedTcpRelayPrefix) -> String {
    let mut key = format!("{}:{}", prefix.first.protocol.len(), prefix.first.protocol);
    if let Some(tag) = &prefix.first.tag {
        key.push_str(&format!("|{}:{tag}", tag.len()));
    }
    if let Some((server, port)) = &prefix.first.endpoint {
        key.push_str(&format!("|{}:{server}:{port}", server.len()));
    }
    for hop in &prefix.relay_hops {
        key.push_str(&format!(
            "|{}:{}|{}:{}|{}:{}:{}",
            hop.tag.len(),
            hop.tag,
            hop.protocol.len(),
            hop.protocol,
            hop.server.len(),
            hop.server,
            hop.port
        ));
    }
    if let Some(Some(datagram)) = prefix.datagram_prefixes.last() {
        key.push_str(&format!(
            "|packet:{}:{}",
            datagram.identity().len(),
            datagram.identity()
        ));
    }
    key.push_str(&format!(
        "|{}:{}|{}:{}|{}:{}:{}",
        prefix.final_tag.len(),
        prefix.final_tag,
        prefix.final_protocol.len(),
        prefix.final_protocol,
        prefix.final_server.len(),
        prefix.final_server,
        prefix.final_port
    ));
    key
}
