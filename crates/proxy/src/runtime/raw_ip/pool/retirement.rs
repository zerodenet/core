use super::*;
impl RawIpDevicePool {
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
                    !entry.compatibility
                        && !desired
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
        let result = self.drain_retired().await;
        self.drain_compatibility_workers().await;
        result
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
                if entry.compatibility {
                    state.compatibility_stopping.push(device.clone());
                } else {
                    state.retiring.push_back(device.clone());
                }
            }
        }
        let compatibility = std::mem::take(&mut state.compatibility_retiring);
        for (_, device) in compatibility {
            if let Some(device) = device.upgrade() {
                device.close_now();
                state.compatibility_stopping.push(device);
            }
        }
        for device in &state.compatibility_stopping {
            device.close_now();
        }
        for device in &state.retiring {
            device.close_now();
        }
    }
    pub(crate) async fn close_all(&self) -> Result<(), EngineError> {
        self.request_shutdown();
        let result = self.drain_retired().await;
        self.drain_compatibility_workers().await;
        result
    }
    /// Existing compatibility completion consumes actual worker joins only.
    /// It grants neither native activation nor a confirmed replacement receipt.
    async fn drain_compatibility_workers(&self) {
        loop {
            let device = self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .compatibility_stopping
                .last()
                .cloned();
            let Some(device) = device else {
                return;
            };
            if let Err(error) = device.wait_workers_stopped().await {
                tracing::warn!(%error, "compatibility raw-IP original workers stopped with a retained failure");
            }
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state
                .compatibility_stopping
                .last()
                .is_some_and(|last| Arc::ptr_eq(last, &device))
            {
                state.compatibility_stopping.pop();
            }
        }
    }
}
