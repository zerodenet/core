//! Age schedules ONLY closed pure metadata. Open originals remain deliverable.
use super::*;
fn reclaim<T>(
    table: &mut PacketMetadataVec<T>,
    memory: &zero_tun::packet_memory::PacketDriverMetadataView,
) {
    if table.capacity() > table.len().saturating_mul(4).max(16) {
        let _ = table.try_shrink_to_fit_with_metadata(memory);
    }
}
impl PacketReturns {
    pub(crate) fn clear(&self) {
        let _notifications = self.budget.defer_notifications();
        let conversations =
            { std::mem::take(&mut *self.conversations.lock().unwrap_or_else(|e| e.into_inner())) };
        let observations =
            { std::mem::take(&mut *self.observations.lock().unwrap_or_else(|e| e.into_inner())) };
        let routes =
            { std::mem::take(&mut *self.routes.lock().unwrap_or_else(|e| e.into_inner())) };
        let translated =
            { std::mem::take(&mut *self.translated.lock().unwrap_or_else(|e| e.into_inner())) };
        drop((conversations, observations, routes, translated));
    }
    pub(crate) fn expire(&self) {
        let _notifications = self.budget.defer_notifications();
        let now = Instant::now();
        for _ in 0..self.row_limits.routes {
            let old = {
                let mut rows = self.routes.lock().unwrap_or_else(|e| e.into_inner());
                let index = rows.iter().position(|(_, row)| {
                    row.replies.is_closed() && now.duration_since(row.touched) >= IDLE_TIMEOUT
                });
                index.map(|index| rows.remove(index))
            };
            match old {
                Some(old) => drop(old),
                None => break,
            }
        }
        for _ in 0..self.row_limits.conversations {
            let old = {
                let mut rows = self.conversations.lock().unwrap_or_else(|e| e.into_inner());
                let index = rows.iter().position(|(_, row)| {
                    row.replies.is_closed()
                        && now.duration_since(row.touched) >= Duration::from_secs(600)
                });
                index.map(|index| rows.remove(index))
            };
            match old {
                Some(old) => drop(old),
                None => break,
            }
        }
        for _ in 0..self.row_limits.observations {
            let old = {
                let mut rows = self.observations.lock().unwrap_or_else(|e| e.into_inner());
                let index = rows.iter().position(|(_, row)| {
                    row.replies.is_closed() && now.duration_since(row.touched) >= IDLE_TIMEOUT
                });
                index.map(|index| rows.remove(index))
            };
            match old {
                Some(old) => drop(old),
                None => break,
            }
        }
        let memory = self.budget.metadata();
        reclaim(
            &mut self.routes.lock().unwrap_or_else(|e| e.into_inner()),
            &memory,
        );
        reclaim(
            &mut self.conversations.lock().unwrap_or_else(|e| e.into_inner()),
            &memory,
        );
        reclaim(
            &mut self.observations.lock().unwrap_or_else(|e| e.into_inner()),
            &memory,
        );
        // Existing translated echo protocol TTL is distinct from a live return lease.
        self.translated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .expire(now, |row| row.replies.is_closed());
    }
}
