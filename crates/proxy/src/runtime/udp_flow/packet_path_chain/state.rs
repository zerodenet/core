use zero_platform_tokio::contract::component::PacketMetadataVec;

use super::entry::build_entry;
use super::model::{CacheRow, Entry, EntryCandidate, PacketPathCarrierRequest};
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_flow::packet_path::UdpDatagramSource;
use zero_engine::EngineError;

pub(crate) struct PacketPathManager {
    pub(super) upstreams: PacketMetadataVec<CacheRow>,
    receiver: Option<super::receiver::Receiver>,
}

impl PacketPathManager {
    pub(crate) fn new() -> Self {
        Self {
            upstreams: PacketMetadataVec::default(),
            receiver: None,
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
        let existing = self
            .upstreams
            .iter()
            .position(|row| row.key.matches_sources(&candidate.carrier_desc, &candidate));

        if let Some(index) = existing {
            let entry = &mut self.upstreams[index].entry;
            if let Some(source) = entry.receiving.failure() {
                if entry.retiring.is_none() {
                    let original = entry
                        .path
                        .as_ref()
                        .and_then(|path| path.logical_registration())
                        .ok_or_else(|| EngineError::from(std::io::Error::other(source)))?;
                    entry.retiring = Some(original);
                    // No table/Source lock spans the actual view/control Drop.
                    drop(entry.path.take());
                }
                let original = entry
                    .retiring
                    .as_ref()
                    .expect("same failed original retained");
                let execution = ctx.packet_path_execution_services().tcp();
                // No invented timeout extends this request. Without a request
                // deadline poll current cleanup once, retaining unknown debt.
                let deadline = execution
                    .invocation
                    .as_ref()
                    .and_then(|call| call.deadline)
                    .unwrap_or_else(tokio::time::Instant::now);
                let report = original
                    .retire_and_wait(deadline)
                    .await
                    .map_err(std::io::Error::other)?;
                if !report.is_complete() || !original.closed() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "same failed packet original has not closed",
                    )
                    .into());
                }
                entry.receiving.forget_closed(original);
                drop(self.upstreams.remove(index));
            }
        }
        if let Some(index) = self
            .upstreams
            .iter()
            .position(|row| row.key.matches_sources(&candidate.carrier_desc, &candidate))
        {
            return Ok(&self.upstreams[index].entry);
        }
        {
            let services = ctx.packet_path_execution_services();
            if self.receiver.is_none() {
                self.receiver = Some(super::receiver::Receiver::start(&services.tcp())?);
            }
            let receiver = self
                .receiver
                .as_ref()
                .expect("same association response duty");
            receiver.check_caller(&services.tcp())?;
            let memory =
                crate::protocol_registry::packet_memory::business::BusinessMemory::from_services(
                    &services.tcp(),
                )?;
            let source = candidate.datagram.descriptor();
            let strings = [
                candidate.carrier_desc.cache_key.as_str(),
                source.tag.as_str(),
                source.server.as_str(),
                source.cache_key.as_str(),
                source.server.as_str(),
            ];
            let bytes = strings
                .iter()
                .try_fold(0usize, |sum, value| sum.checked_add(value.len()))
                .ok_or_else(|| std::io::Error::other("packet path cache key layout overflow"))?;
            let credit = memory
                .metadata
                .reserve(zero_traits::packet_metadata::PacketMetadataLayout::new(
                    strings.iter().filter(|value| !value.is_empty()).count(),
                    bytes,
                ))
                .map_err(std::io::Error::other)?;
            self.upstreams
                .try_reserve_one_with_metadata(
                    &memory.metadata,
                    services.tcp().retired_datagrams().capacity(),
                )
                .map_err(std::io::Error::other)?;
            let key = candidate.key();
            let entry = build_entry(ctx, carrier.build_operation, candidate, receiver).await?;
            self.upstreams
                .push_reserved(CacheRow {
                    key,
                    entry,
                    _strings: credit,
                })
                .unwrap_or_else(|_| panic!("same prepaid cache row"));
        }

        Ok(&self
            .upstreams
            .last()
            .expect("packet path entry inserted")
            .entry)
    }
}
