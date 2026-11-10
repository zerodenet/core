//! Fixed actual per-call reservation rows; observations and destruction are lock-free.
use super::*;
use zero_traits::packet_metadata::PacketMetadataCredit;
struct Row {
    ordinal: u64,
    entry: Option<PacketMetadataArc<entry::Entry>>,
    reserved: bool,
}
pub(super) struct Table {
    next: u64,
    rows: Vec<Row>,
    _credit: PacketMetadataCredit,
}
pub(super) struct Position {
    table: PacketMetadataArc<Mutex<Table>>,
    index: usize,
    pub ordinal: u64,
    published: bool,
}
impl Table {
    pub fn prepare(
        memory: &PacketDriverMetadataView,
        limit: usize,
    ) -> Result<PacketMetadataArc<Mutex<Self>>, ChannelError> {
        let header = memory
            .reserve(PacketMetadataArc::<Mutex<Self>>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let credit = memory
            .reserve(PacketMetadataLayout::array::<Row>(limit).map_err(capacity)?)
            .map_err(capacity)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(limit).map_err(capacity)?;
        rows.resize_with(limit, || Row {
            ordinal: 0,
            entry: None,
            reserved: false,
        });
        PacketMetadataArc::from_prepaid(
            Mutex::new(Self {
                next: 0,
                rows,
                _credit: credit,
            }),
            header,
        )
        .map_err(|(source, _, _)| capacity(source))
    }
    pub fn next(
        table: &PacketMetadataArc<Mutex<Self>>,
        after: u64,
    ) -> Option<(u64, PacketMetadataArc<entry::Entry>)> {
        table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rows
            .iter()
            .filter(|row| row.ordinal > after && row.entry.is_some())
            .min_by_key(|row| row.ordinal)
            .map(|row| (row.ordinal, row.entry.as_ref().unwrap().clone()))
    }
    pub fn reap(table: &PacketMetadataArc<Mutex<Self>>) {
        let mut after = 0;
        while let Some((ordinal, entry)) = Self::next(table, after) {
            after = ordinal;
            if !entry
                .retirement
                .get()
                .is_some_and(|original| original.is_settled())
            {
                continue;
            }
            let removed = {
                let mut state = table
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state
                    .rows
                    .iter_mut()
                    .find(|row| {
                        row.ordinal == ordinal
                            && row
                                .entry
                                .as_ref()
                                .is_some_and(|actual| PacketMetadataArc::ptr_eq(actual, &entry))
                    })
                    .and_then(|row| {
                        row.reserved = false;
                        row.entry.take()
                    })
            };
            drop(removed);
        }
    }
    pub fn reserve(table: &PacketMetadataArc<Mutex<Self>>) -> Result<Position, ChannelError> {
        Self::reap(table);
        let mut state = table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ordinal = state
            .next
            .checked_add(1)
            .ok_or_else(|| error(Stage::Prepare, Reason::Capacity))?;
        let (index, row) = state
            .rows
            .iter_mut()
            .enumerate()
            .find(|(_, row)| !row.reserved)
            .ok_or_else(|| error(Stage::Prepare, Reason::Capacity))?;
        row.ordinal = ordinal;
        row.reserved = true;
        state.next = ordinal;
        Ok(Position {
            table: table.clone(),
            index,
            ordinal,
            published: false,
        })
    }
    pub fn request_retirement(table: &PacketMetadataArc<Mutex<Self>>) {
        let mut after = 0;
        while let Some((ordinal, entry)) = Self::next(table, after) {
            after = ordinal;
            if let Some(original) = entry.retirement.get() {
                original.request_retirement();
            }
        }
    }
}
impl Position {
    pub fn publish(mut self, entry: PacketMetadataArc<entry::Entry>) {
        let replaced = {
            let mut table = self
                .table
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let row = &mut table.rows[self.index];
            assert!(row.reserved && row.ordinal == self.ordinal);
            row.entry.replace(entry)
        };
        assert!(replaced.is_none());
        drop(replaced);
        self.published = true;
    }
}
impl Drop for Position {
    fn drop(&mut self) {
        if !self.published {
            let mut table = self
                .table
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let row = &mut table.rows[self.index];
            assert!(row.ordinal == self.ordinal && row.entry.is_none());
            row.reserved = false;
        }
    }
}
