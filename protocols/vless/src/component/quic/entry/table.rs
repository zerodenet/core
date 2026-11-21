//! Lock-free original observation, exact row removal for the shared half table.
use super::*;
impl Table {
    /// A rotated HTTP original still occupies N until its actual Root closes
    /// native IO and receives the original Scope ACK. Wait only for originals
    /// already retiring, without evicting active users or waiting on our driver.
    pub(in crate::component::quic) async fn reserve_after_retiring(
        entries: &PacketMetadataArc<Mutex<Self>>,
        needed: [bool; 2],
        context: &zero_platform_tokio::contract::context::OpenContext,
        stopped: &zero_platform_tokio::contract::CancellationToken,
    ) -> Result<[Option<Position>; 2], ChannelError> {
        let end = context
            .deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(30));
        loop {
            if stopped.is_cancelled() || context.cancel.is_cancelled() {
                return Err(error(Stage::Prepare, Reason::Cancelled));
            }
            Self::reap(entries);
            let failure = match Self::reserve_needed(entries, needed) {
                Ok(positions) => return Ok(positions),
                Err(source) if source.reason == Reason::Capacity => source,
                Err(source) => return Err(source),
            };
            if needed.into_iter().filter(|needed| *needed).count()
                > entries.lock().unwrap().rows.len()
            {
                return Err(failure);
            }
            let mut previous = 0;
            let mut retiring = None;
            while let Some((ordinal, entry)) = Self::next_entry(entries, previous) {
                previous = ordinal;
                let own_driver = tokio::task::try_id().is_some_and(|current| {
                    entry
                        .retirement
                        .original_receiver()
                        .is_some_and(|receiver| {
                            receiver.protocol_driver().native_task_id() == Some(current)
                        })
                });
                if entry.retirement.is_settled()
                    || entry.retirement.retirement_requested() && !own_driver
                {
                    retiring = Some(entry.retirement.clone());
                    break;
                }
            }
            let Some(retiring) = retiring else {
                return Err(failure);
            };
            tokio::select! { biased;
                _ = stopped.cancelled() => return Err(error(Stage::Prepare, Reason::Cancelled)),
                _ = context.cancel.cancelled() => return Err(error(Stage::Prepare, Reason::Cancelled)),
                result = tokio::time::timeout_at(end, retiring.wait_settled_original()) => {
                    result.map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))??;
                }
            }
        }
    }
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
