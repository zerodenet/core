//! Capacity is returned by the original entry's ledger removal.
use super::*;

impl Generation {
    pub(super) async fn admit_entry(
        &self,
        key: &Key,
        end: Instant,
    ) -> Result<Arc<Entry>, ChannelError> {
        if let Ok(_turn) = self.entry_admission.try_lock() {
            if let Some(entry) = self.try_admit_entry(key, end)? {
                return Ok(entry);
            }
        } else if let Some(existing) = self.entries.lock().unwrap().get(key).cloned() {
            // A cached lookup needs no new position and never queues behind an
            // unrelated retirement. The existing initializer still fences it.
            self.check_admission(end)?;
            return Ok(existing);
        }

        // One FIFO waiter observes the returned position and admits it from the
        // actual ledger. Keep this wait across candidates so newer callers
        // cannot repeatedly claim positions in front of a slow retirement.
        let admission = self.admit_when_released(key, end);
        tokio::pin!(admission);
        let mut candidates = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone(), *entry.touched.lock().unwrap()))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(_, _, touched)| *touched);
        let mut retiring = false;
        for (old, entry, _) in candidates {
            retiring |= entry.cancel.is_cancelled();
            let Ok(_initialization) = entry.initialization.try_lock() else {
                continue;
            };
            tokio::select! {
                biased;
                admitted = &mut admission => return admitted,
                result = async {
                    if entry.close_if_idle(end).await? {
                        self.remove_entry(&old, &entry).await?;
                    }
                    Ok::<_, ChannelError>(())
                } => result?,
            }
        }
        if retiring {
            return tokio::time::timeout_at(end, admission)
                .await
                .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?;
        }
        tokio::select! {
            biased;
            admitted = &mut admission => admitted,
            // Every candidate was busy or in use. Preserve the immediate
            // pressure rejection when we could not retire an original.
            () = std::future::ready(()) => Err(error(Stage::Prepare, Reason::Capacity)),
        }
    }

    async fn admit_when_released(
        &self,
        key: &Key,
        end: Instant,
    ) -> Result<Arc<Entry>, ChannelError> {
        let _turn = self.entry_admission.lock().await;
        loop {
            let changed = self.entry_released.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(entry) = self.try_admit_entry(key, end)? {
                // Maintenance continues any idle original already revoked by
                // this caller. Only another completed entry funded admission.
                return Ok(entry);
            }
            changed.await;
        }
    }

    fn try_admit_entry(&self, key: &Key, end: Instant) -> Result<Option<Arc<Entry>>, ChannelError> {
        let mut entries = self.entries.lock().unwrap();
        self.check_admission(end)?;
        if let Some(entry) = entries.get(key) {
            return Ok(Some(entry.clone()));
        }
        if entries.len() + self.failures.lock().unwrap().len() >= self.entry_limit.get() {
            return Ok(None);
        }
        let entry = Arc::new(Entry {
            resources: self.resources.clone(),
            initialization: Default::default(),
            cancel: self.cancel.child_token(),
            state: Default::default(),
            touched: Mutex::new(Instant::now()),
            protocol_wait: Default::default(),
        });
        entries.insert(key.clone(), entry.clone());
        Ok(Some(entry))
    }

    fn check_admission(&self, end: Instant) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        if Instant::now() >= end {
            return Err(error(Stage::Prepare, Reason::Timeout));
        }
        Ok(())
    }
}
