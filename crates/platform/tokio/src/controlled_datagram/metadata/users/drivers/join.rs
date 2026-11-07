use super::*;
impl<O: DriverKind, B: Send + Sync + 'static> DriverOwner<O, B> {
    pub async fn wait_joined(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<TaskJoined>, ChannelError> {
        tokio::time::timeout_at(deadline, self.wait_joined_original())
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?
    }
    /// Wait the same sealed original registrations without an observer timer.
    pub async fn wait_joined_original(&self) -> Result<Vec<TaskJoined>, ChannelError> {
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (pending, witnesses) = {
                let state = self.shared.state.lock().unwrap();
                (
                    state.pending,
                    state
                        .records
                        .values()
                        .map(|record| {
                            record
                                .registration
                                .lock()
                                .unwrap()
                                .as_ref()
                                .unwrap()
                                .1
                                .clone()
                        })
                        .collect::<Vec<_>>(),
                )
            };
            let mut joined = Vec::with_capacity(witnesses.len());
            for witness in witnesses {
                joined.push(witness.wait().await);
            }
            if pending == 0 {
                return Ok(joined);
            }
            changed.await;
        }
    }
}
