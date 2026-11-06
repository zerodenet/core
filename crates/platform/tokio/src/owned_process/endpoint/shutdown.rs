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
        let mut pending: Vec<_> = users
            .iter()
            .map(|user| Some(Box::pin(user.retire(deadline))))
            .collect();
        // Poll this finite set inline in the existing cleanup operation. Every
        // original has already been revoked; no detached cleanup task is made.
        std::future::poll_fn(|cx| {
            use std::{future::Future, task::Poll};
            for entry in &mut pending {
                if let Some(future) = entry {
                    if let Poll::Ready(result) = future.as_mut().poll(cx) {
                        if let Err(error) = result {
                            faults.push(error);
                        }
                        *entry = None;
                    }
                }
            }
            if pending.iter().all(Option::is_none) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
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
