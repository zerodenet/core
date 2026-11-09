use super::bridge::dispatch_via_entry;
use super::key::PathKey;
use super::model::{CacheRow, Entry};
use super::state::PacketPathManager;
use crate::runtime::udp_flow::packet_path::{PacketPathLookupKey, UdpFlowContext, UdpPacketRef};
use crate::runtime::udp_flow::result::FlowFailure;
use zero_engine::EngineError;

pub(super) struct SnapshotLookup {
    pub(super) lookup_key: PacketPathLookupKey,
}

pub(super) fn lookup_entry(
    upstreams: &[CacheRow],
    lookup: SnapshotLookup,
) -> Result<&Entry, FlowFailure> {
    let upstream = lookup.lookup_key.datagram_endpoint();
    let key = PathKey::from_lookup(lookup.lookup_key);
    upstreams
        .iter()
        .find(|row| row.key == key)
        .map(|row| &row.entry)
        .ok_or_else(|| FlowFailure {
            stage: "packet_path_carrier_dropped",
            error: EngineError::Io(std::io::Error::other(
                "cached packet-path carrier not found",
            )),
            upstream: Some(upstream),
        })
}

pub(crate) struct SendWithSnapshotRequest<'a> {
    pub ctx: UdpFlowContext<'a>,
    pub lookup_key: PacketPathLookupKey,
    pub packet_ref: UdpPacketRef<'a>,
}

impl PacketPathManager {
    /// Forward path: the carrier was cached at start time; look it up by the
    /// stored snapshot's cache key. No leaves available, so no re-dial.
    pub(crate) async fn send_with_snapshot(
        &mut self,
        request: SendWithSnapshotRequest<'_>,
    ) -> Result<usize, FlowFailure> {
        let entry = lookup_entry(
            &self.upstreams,
            SnapshotLookup {
                lookup_key: request.lookup_key,
            },
        )?;
        dispatch_via_entry(entry, request.ctx, request.packet_ref).await
    }
}
