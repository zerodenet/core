//! Lock-free original observation, exact row removal for the shared half table.
use super::*;
impl Table {
    pub(in crate::component::quic) fn next_entry(
        entries: &PacketMetadataArc<Mutex<Self>>,
        after: u64,
    ) -> Option<(u64, PacketMetadataArc<Entry>)> {
        entries
            .lock()
            .unwrap()
            .rows
            .iter()
            .filter(|row| row.reserved && row.ordinal > after && row.entry.is_some())
            .min_by_key(|row| row.ordinal)
            .map(|row| (row.ordinal, row.entry.as_ref().unwrap().clone()))
    }
    pub(in crate::component::quic) fn reap(entries: &PacketMetadataArc<Mutex<Self>>) {
        let mut after = 0;
        while let Some((ordinal, entry)) = Self::next_entry(entries, after) {
            after = ordinal;
            if !entry.retirement.is_settled() {
                continue;
            }
            let removed = {
                let mut table = entries.lock().unwrap();
                if let Some(row) = table.rows.iter_mut().find(|row| {
                    row.reserved
                        && row.ordinal == ordinal
                        && row
                            .entry
                            .as_ref()
                            .is_some_and(|actual| PacketMetadataArc::ptr_eq(actual, &entry))
                }) {
                    let entry = row.entry.take();
                    row.reserved = false;
                    table.used -= 1;
                    entry
                } else {
                    None
                }
            };
            // Another actual half can have reaped the same positive row.
            // Never Drop an original/controller or refund under this table.
            drop(removed);
        }
    }
    pub(in crate::component::quic) fn request_retirement(entries: &PacketMetadataArc<Mutex<Self>>) {
        let mut after = 0;
        while let Some((ordinal, entry)) = Self::next_entry(entries, after) {
            after = ordinal;
            entry.retirement.request_retirement();
        }
    }
}
