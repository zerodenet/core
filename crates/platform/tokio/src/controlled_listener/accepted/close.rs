use super::*;
use crate::contract::lifecycle::ChannelControl;
impl Pool {
    pub(super) async fn close(
        &self,
        claim: &AcceptedTcpClaim,
        deadline: Instant,
    ) -> Result<Option<AcceptedTcpNativeClosed>, ChannelError> {
        let entry = &claim.entry;
        if !Arc::ptr_eq(&self.identity, &entry.identity) {
            return Err(failure(
                Stage::Close,
                Reason::StalePlan,
                "accepted TCP claim belongs to another listener",
            ));
        }
        let native_closed = { entry.state().phase == Phase::Closed };
        if native_closed {
            self.confirm(entry);
            return Ok(Some(AcceptedTcpNativeClosed {
                entry: entry.clone(),
            }));
        }
        let Some(holder) = entry.control.get() else {
            let state = entry.state();
            return if state.phase == Phase::Waiting {
                Ok(None)
            } else {
                Err(retained(
                    state
                        .unknown
                        .clone()
                        .unwrap_or_else(|| entry.uncertain.clone()),
                ))
            };
        };
        let mut control = match tokio::time::timeout_at(deadline, holder.lock()).await {
            Ok(guard) => guard,
            Err(_) => return Ok(None),
        };
        // Concrete same original, never a caller's report or string receipt.
        match control.wait_closed(deadline).await {
            Ok(report) if report.is_complete() => {
                entry.state().phase = Phase::Closed;
                self.confirm(entry);
                Ok(Some(AcceptedTcpNativeClosed {
                    entry: entry.clone(),
                }))
            }
            Ok(_) => Ok(None),
            Err(source) => Err(self.remember(entry, source, true)),
        }
    }
    pub(super) async fn close_graph(
        &self,
        claim: &AcceptedTcpClaim,
        deadline: Instant,
    ) -> Result<Option<AcceptedTcpClosed>, ChannelError> {
        if !Arc::ptr_eq(&self.identity, &claim.entry.identity) {
            return Err(failure(
                Stage::Close,
                Reason::StalePlan,
                "accepted graph belongs to another listener",
            ));
        }
        let users = claim.entry.users.as_ref().ok_or_else(|| {
            failure(
                Stage::Close,
                Reason::Unsupported,
                "accepted task graph was not declared",
            )
        })?;
        if !users.retire(deadline).await {
            return Ok(None);
        }
        let Some(native) = self.close(claim, deadline).await? else {
            return Ok(None);
        };
        if !native.matches(claim) || !users.complete() {
            return Ok(None);
        }
        Ok(Some(AcceptedTcpClosed {
            entry: claim.entry.clone(),
        }))
    }
    pub(super) async fn shutdown(&self, deadline: Instant) -> Option<ChannelError> {
        let entries: Vec<_> = self.state().entries.values().cloned().collect();
        let mut first = None;
        for entry in entries {
            if let Some(users) = &entry.users {
                // Cancellation reaches the same I/O and all registered tasks.
                // The real host Scope consumes joins concurrently with this wait.
                if !users.retire(deadline).await {
                    continue;
                }
            }
            match self.close(&AcceptedTcpClaim { entry }, deadline).await {
                Err(error) if first.is_none() => first = Some(error),
                _ => {}
            }
        }
        first
    }
}
