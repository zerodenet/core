use std::collections::HashMap;

use super::entry::build_entry;
use super::key::PathKey;
use super::model::{Entry, EntryCandidate, PacketPathCarrierRequest};
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_flow::packet_path::UdpDatagramSource;
use zero_engine::EngineError;

pub(crate) struct PacketPathManager {
    pub(super) upstreams: HashMap<PathKey, Entry>,
}

impl PacketPathManager {
    pub(crate) fn new() -> Self {
        Self {
            upstreams: HashMap::new(),
        }
    }

    pub(super) async fn ensure_entry(
        &mut self,
        ctx: UdpAdapterContext<'_>,
        carrier: PacketPathCarrierRequest,
        datagram: UdpDatagramSource,
    ) -> Result<&Entry, EngineError> {
        let candidate = EntryCandidate {
            carrier_desc: carrier.descriptor,
            datagram,
        };
        let key = candidate.key();

        if !self.upstreams.contains_key(&key) {
            let entry = build_entry(ctx, carrier.build_operation, candidate).await?;
            self.upstreams.insert(key.clone(), entry);
        }

        Ok(self
            .upstreams
            .get(&key)
            .expect("packet path entry inserted"))
    }
}
