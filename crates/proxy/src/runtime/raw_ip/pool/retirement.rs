use super::*;
impl RawIpDevicePool {
    /// Drain only originals already abandoned by a dropped staging guard.
    /// Published healthy entries stay admitted; no replacement is allocated.
    pub(crate) async fn close_discarded(&self) -> Result<(), EngineError> {
        self.drain_retired().await
    }
    /// Fence and retain affected originals BEFORE any candidate allocation.
    pub(crate) async fn retire_changed(
        &self,
        desired: &HashMap<String, ([u8; 32], u64, usize)>,
    ) -> Result<(), EngineError> {
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let keys = state
                .entries
                .iter()
                .filter(|((tag, peer), entry)| {
                    !desired
                        .get(tag)
                        .is_some_and(|(identity, generation, count)| {
                            entry.identity == *identity
                                && entry.egress_generation == *generation
                                && peer < count
                                && entry.cell.get().is_some_and(|d| d.is_usable())
                        })
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in keys {
                if let Some(device) = state
                    .entries
                    .remove(&key)
                    .and_then(|entry| entry.cell.get().cloned())
                {
                    device.close_now();
                    state.retiring.push_back(device);
                }
            }
        }
        self.drain_retired().await
    }
    async fn drain_retired(&self) -> Result<(), EngineError> {
        loop {
            let original = self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retiring
                .front()
                .cloned();
            let Some(original) = original else {
                return Ok(());
            };
            original.wait_closed().await?;
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state
                .retiring
                .front()
                .is_some_and(|first| Arc::ptr_eq(first, &original))
            {
                state.retiring.pop_front();
            }
        }
    }
    pub(crate) fn request_shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.shutdown = true;
        let entries = std::mem::take(&mut state.entries);
        for entry in entries.into_values() {
            if let Some(device) = entry.cell.get() {
                device.close_now();
                state.retiring.push_back(device.clone());
            }
        }
        for device in &state.retiring {
            device.close_now();
        }
    }
    pub(crate) async fn close_all(&self) -> Result<(), EngineError> {
        self.request_shutdown();
        self.drain_retired().await
    }
}
