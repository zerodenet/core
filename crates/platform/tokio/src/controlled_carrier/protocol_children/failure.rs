//! A single paid observation if this exact child Root disappears.
use super::*;
use crate::contract::RetainedError;
use std::sync::OnceLock;
use zero_traits::packet_metadata::PacketMetadataArc;
pub(super) struct Record {
    pub original: ProtocolOriginal,
    pub ordinal: u64,
    pub caller: Option<crate::contract::scope::TaskJoinWitness>,
    cause: PacketMetadataArc<MissingRoot>,
    retained: RetainedError,
}
struct MissingRoot(OnceLock<ChannelError>);
impl std::fmt::Debug for MissingRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedOriginalMissingRoot")
            .field("observed", &self.0.get().is_some())
            .finish()
    }
}
impl std::fmt::Display for MissingRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actual managed child Root disappeared before return")
    }
}
impl std::error::Error for MissingRoot {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.get().map(|source| source as _)
    }
}
impl Record {
    pub(super) fn prepare(
        original: ProtocolOriginal,
        memory: &PacketDriverMetadataView,
        ordinal: u64,
        caller: Option<crate::contract::scope::TaskJoinWitness>,
    ) -> Result<PacketMetadataArc<Self>, ChannelError> {
        let credit = memory
            .reserve(PacketMetadataArc::<MissingRoot>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let cause = PacketMetadataArc::from_prepaid(MissingRoot(OnceLock::new()), credit)
            .map_err(|(source, _, _)| capacity(source))?;
        let credit = memory
            .reserve(RetainedError::metadata_layout().map_err(capacity)?)
            .map_err(capacity)?;
        let failure = ChannelError::new(Stage::Close, Reason::Revoked, Impact::Component)
            .with_metadata_source(cause.clone().into_error());
        let retained = RetainedError::from_prepaid(failure, credit)
            .map_err(|(source, _, _)| capacity(source))?;
        let credit = memory
            .reserve(PacketMetadataArc::<Self>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        PacketMetadataArc::from_prepaid(
            Self {
                original,
                ordinal,
                caller,
                cause,
                retained,
            },
            credit,
        )
        .map_err(|(source, _, _)| capacity(source))
    }
    pub(super) fn receive(&self, source: ChannelError) -> (RetainedError, Option<ChannelError>) {
        (self.retained.clone(), self.cause.0.set(source).err())
    }
}
