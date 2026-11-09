use std::sync::Arc;

use tracing::debug;
use zero_engine::EngineError;

use super::bridge::recv_loop;
use super::model::{Entry, EntryCandidate};
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;

pub(super) fn log_candidate(candidate: &EntryCandidate) {
    let carrier_desc = &candidate.carrier_desc;
    let datagram_desc = candidate.datagram.descriptor();
    debug!(
        carrier = %carrier_desc.cache_key,
        carrier_server = %carrier_desc.server,
        carrier_port = carrier_desc.port,
        datagram_tag = %datagram_desc.tag,
        datagram_server = %datagram_desc.server,
        datagram_port = datagram_desc.port,
        "ensuring UDP packet-path relay chain"
    );
}

pub(super) async fn build_entry(
    ctx: UdpAdapterContext<'_>,
    build_operation: Arc<dyn PreparedUdpPacketPathOperation>,
    candidate: EntryCandidate,
    receiver: &super::receiver::Receiver,
) -> Result<Entry, EngineError> {
    log_candidate(&candidate);
    let datagram_endpoint = candidate.datagram.descriptor().endpoint();
    let mut services = ctx.packet_path_execution_services().with_packet_target(
        zero_core::request::Destination::new(datagram_endpoint.target(), datagram_endpoint.port()),
    );
    if let Some(tag) = &candidate.carrier_desc.tag {
        services = services.with_outbound_io(tag);
    }
    // Source cells and retained diagnostics are paid before protocol/native
    // preparation. Actual RX work starts only after its original path installs.
    let memory = crate::protocol_registry::packet_memory::business::BusinessMemory::from_services(
        &services.tcp(),
    )?;
    let waiters = super::bridge::Waiters::new(&memory)?;
    let (receiving, scratch) = receiver.prepare_path()?;
    let path = match crate::component::stream_bridge::datagram::packet_path::StandardPacketPath::build_installed(build_operation.as_ref(), &services).await? {
        Some(actual) => actual,
        None => build_operation.build_carrier(services.clone()).await?,
    };
    let codec = candidate.datagram.codec.clone();
    let datagram_desc = candidate.datagram.descriptor();
    let path = super::carriers::observed::wrap(
        path,
        services
            .tcp()
            .outbound_io(&datagram_desc.tag, zero_api::TrafficPlane::Outer),
    );
    let receiving = receiver.retain(
        receiving,
        recv_loop(
            path.clone(),
            waiters.clone(),
            memory.clone(),
            codec.clone(),
            scratch,
        ),
    )?;

    Ok(Entry {
        path: Some(path),
        retiring: None,
        receiving,
        waiters,
        memory,
        codec,
        datagram_endpoint: datagram_desc.endpoint(),
    })
}
