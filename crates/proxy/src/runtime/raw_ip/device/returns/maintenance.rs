//! Reclaim expired execution/observation state and oversized table storage.
use super::*;

fn reclaim<K: Eq + std::hash::Hash, V>(table: &mut HashMap<K, V>) {
    // Maintenance only: avoid rehashing on packet receipt or every removal.
    // Hysteresis retains room for ordinary churn, releasing burst capacity.
    if table.capacity() > table.len().saturating_mul(4).max(16) {
        table.shrink_to(table.len());
    }
}

impl PacketReturns {
    pub(crate) fn clear(&self) {
        *self.conversations.lock().unwrap_or_else(|e| e.into_inner()) = HashMap::new();
        *self.observations.lock().unwrap_or_else(|e| e.into_inner()) = HashMap::new();
        self.translated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        *self.routes.lock().unwrap_or_else(|e| e.into_inner()) = HashMap::new();
    }

    pub(crate) fn expire(&self) {
        let now = Instant::now();
        {
            let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
            // Do not remove a closed but unexpired return: it must still consume
            // and count late replies instead of admitting them as new ingress.
            routes.retain(|_, r| now.duration_since(r.touched) < IDLE_TIMEOUT);
            reclaim(&mut routes);
        }
        {
            let mut conversations = self.conversations.lock().unwrap_or_else(|e| e.into_inner());
            conversations.retain(|_, c| now.duration_since(c.touched) < Duration::from_secs(600));
            reclaim(&mut conversations);
        }
        {
            let mut observations = self.observations.lock().unwrap_or_else(|e| e.into_inner());
            observations.retain(|_, o| now.duration_since(o.touched) < IDLE_TIMEOUT);
            reclaim(&mut observations);
        }
        self.translated
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .expire(now, |r| r.replies.is_closed());
    }
}
