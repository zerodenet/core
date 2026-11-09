use super::*;
use zero_traits::packet_metadata::PacketMetadataCredit;
/// Specific prepaid promise for the three private Scope headers. The actual
/// attachment's own two Arc allocations are included separately.
pub(super) struct HeaderLiability {
    _headers: PacketMetadataCredit,
}
impl HeaderLiability {
    pub(super) fn layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<Self>::layout()?
            .checked_add(TaskJoinWitness::task_failure_header_layout()?)
    }
    pub(super) fn from_prepaid(
        mut credit: PacketMetadataCredit,
    ) -> Result<PacketMetadataArc<Self>, ChannelError> {
        let headers = credit
            .split(TaskJoinWitness::task_failure_header_layout().map_err(capacity)?)
            .map_err(capacity)?;
        PacketMetadataArc::from_prepaid(Self { _headers: headers }, credit)
            .map_err(|(source, _, _)| capacity(source))
    }
}
