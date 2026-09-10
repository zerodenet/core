use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

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
    build_operation: Box<dyn PreparedUdpPacketPathOperation>,
    candidate: EntryCandidate,
) -> Result<Entry, EngineError> {
    log_candidate(&candidate);
    let path = build_operation
        .build_carrier(ctx.network_services())
        .await?;
    let codec = candidate.datagram.codec.clone();
    let datagram_desc = candidate.datagram.descriptor();
    let waiters = Arc::new(Mutex::new(VecDeque::new()));
    tokio::spawn(recv_loop(path.clone(), waiters.clone(), codec.clone()));

    Ok(Entry {
        path,
        waiters,
        codec,
        datagram_endpoint: datagram_desc.endpoint(),
    })
}
