use super::*;
impl ProcessEndpointHandoff {
    pub async fn shutdown(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<Option<Arc<ProcessEndpointClosed>>, ChannelError> {
        self.request_stop();
        let mut faults = Vec::new();
        if let Err(error) = self.close_reservations(deadline).await {
            faults.push(error);
        }
        let probe = { self.lock().probe.clone() };
        if let Some(probe) = probe {
            if let Err(error) = probe.close(deadline).await {
                faults.push(error);
            }
        }
        let users: Vec<_> = {
            self.lock()
                .users
                .iter()
                .map(|user| user.adoption.clone())
                .collect()
        };
        // Same existing caller/Host cleanup; no task, detached future or reaper.
        let results =
            futures_util::future::join_all(users.iter().map(|user| user.retire(deadline))).await;
        for result in results {
            if let Err(error) = result {
                faults.push(error);
            }
        }
        let users_closed = users.iter().all(|user| {
            user.closed().is_some_and(|closed| closed.matches(user))
                && user
                    .users_closed()
                    .is_some_and(|closed| closed.matches(user))
        });
        let probe_closed = {
            self.lock()
                .probe
                .as_ref()
                .is_none_or(|probe| probe.is_closed())
        };
        let reservations_closed = self.lock().reservation_close.is_some();
        if reservations_closed && probe_closed && users_closed {
            let mut state = self.lock();
            state.retired.get_or_insert_with(|| {
                Arc::new(ProcessEndpointClosed {
                    original: Arc::downgrade(self),
                })
            });
        }
        if !faults.is_empty() {
            let source = Arc::new(channel(CleanupFailures(faults)));
            let original = {
                self.lock()
                    .failure
                    .get_or_insert_with(|| source.clone())
                    .clone()
            };
            drop(source);
            return Err(channel(original));
        }
        Ok(self.closed())
    }
}
#[derive(Debug)]
pub(super) struct CleanupFailures(pub(super) Vec<ChannelError>);
impl std::fmt::Display for CleanupFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} original process endpoint cleanup failures",
            self.0.len()
        )
    }
}
impl std::error::Error for CleanupFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|e| e as _)
    }
}
