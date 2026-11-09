//! Same actual codec grants and paid owning row/header storage across retries.
use crate::protocol_registry::packet_memory::PacketCodecReceiveMemory;
use zero_platform_tokio::contract::{
    component::{PacketDriverMetadataView, PacketMetadataVec},
    ChannelError, Impact, Reason, Stage,
};
use zero_traits::packet_metadata::PacketMetadataArc;
#[derive(Clone)]
pub(crate) enum PacketCodecReceiveGroups {
    Empty,
    Rows(PacketMetadataArc<PacketMetadataVec<PacketCodecReceiveMemory>>),
}
impl std::ops::Deref for PacketCodecReceiveGroups {
    type Target = [PacketCodecReceiveMemory];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Empty => &[],
            Self::Rows(rows) => rows.as_ref(),
        }
    }
}
impl PacketCodecReceiveGroups {
    pub(super) fn from_values(
        memory: &PacketDriverMetadataView,
        values: Vec<PacketCodecReceiveMemory>,
    ) -> Result<Self, ChannelError> {
        let mut rows = PacketMetadataVec::default();
        let maximum = values.len();
        for value in values {
            rows.try_push_with_metadata(value, memory, maximum)
                .map_err(|(source, returned)| {
                    drop(returned);
                    failed(source)
                })?;
        }
        Self::from_rows(memory, rows)
    }
    pub(crate) fn from_originals(
        memory: &PacketDriverMetadataView,
        grants: &zero_tun::packet_memory::PacketReadBatch,
        layouts: &[std::sync::Arc<[std::num::NonZeroUsize]>],
    ) -> Result<Self, ChannelError> {
        let mut cursor = 2;
        let mut rows = PacketMetadataVec::default();
        for layout in layouts {
            if layout.is_empty() || layout.len() > 2 {
                return Err(invalid());
            }
            let wire = grants.get(cursor).ok_or_else(invalid)?.clone();
            cursor += 1;
            let mut take = |length: std::num::NonZeroUsize| {
                let actual = grants.get(cursor).ok_or_else(invalid)?;
                cursor += 1;
                if actual.bytes() != length.get() {
                    return Err(invalid());
                }
                Ok(actual.clone())
            };
            let first = take(layout[0])?;
            let second = layout.get(1).copied().map(take).transpose()?;
            let group = PacketCodecReceiveMemory::from_declared(wire, first, second)
                .map_err(|source| invalid().with_source(source))?;
            rows.try_push_with_metadata(group, memory, layouts.len())
                .map_err(|(source, returned)| {
                    drop(returned);
                    failed(source)
                })?;
        }
        if cursor != grants.len() {
            return Err(invalid());
        }
        Self::from_rows(memory, rows)
    }
    fn from_rows(
        memory: &PacketDriverMetadataView,
        rows: PacketMetadataVec<PacketCodecReceiveMemory>,
    ) -> Result<Self, ChannelError> {
        if rows.is_empty() {
            return Ok(Self::Empty);
        }
        let credit = memory
            .reserve(
                PacketMetadataArc::<PacketMetadataVec<PacketCodecReceiveMemory>>::layout()
                    .map_err(failed)?,
            )
            .map_err(failed)?;
        PacketMetadataArc::from_prepaid(rows, credit)
            .map(Self::Rows)
            .map_err(|(source, rows, credit)| {
                drop(rows);
                drop(credit);
                failed(source)
            })
    }
}
fn invalid() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::StalePlan, Impact::Component)
}
fn failed(source: zero_traits::packet_metadata::PacketMemoryError) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component).with_source(source)
}
