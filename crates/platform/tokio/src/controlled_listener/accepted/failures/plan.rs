use super::*;
pub(super) struct Plan {
    pub(super) available: Option<usize>,
    pub(super) table: Option<PacketMetadataLayout>,
    pub(super) header: PacketMetadataLayout,
    pub(super) total: PacketMetadataLayout,
}
pub(super) fn required(rows: &mut rows::Rows) -> Result<Plan, ChannelError> {
    let available = rows
        .values
        .iter()
        .position(|row| !row.reserved && row.failure.is_none() && row.success.is_none());
    let table = if available.is_some() {
        None
    } else {
        rows.values.growth_layout(usize::MAX).map_err(capacity)?
    };
    let needs_header = match available.and_then(|index| rows.values.get_mut(index)) {
        Some(row) => row
            .liability
            .as_mut()
            .is_none_or(|actual| PacketMetadataArc::get_mut(actual).is_none()),
        None => true,
    };
    let header = if needs_header {
        liability::HeaderLiability::layout().map_err(capacity)?
    } else {
        PacketMetadataLayout::new(0, 0)
    };
    let total = header
        .checked_add(table.unwrap_or(PacketMetadataLayout::new(0, 0)))
        .map_err(capacity)?;
    Ok(Plan {
        available,
        table,
        header,
        total,
    })
}
