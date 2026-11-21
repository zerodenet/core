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
                drop(control);
                self.confirm(entry);
                Ok(Some(AcceptedTcpNativeClosed {
                    entry: entry.clone(),
                }))
            }
            Ok(_) => Ok(None),
            Err(source) => Err(self.remember(entry, source, true)),
        }
    }
    /// One concrete native-close attempt after THIS original's users/joins/ACK
    /// are already complete. Never waits for or cancels an active user.
    pub(super) fn try_close_returned_graph(
        &self,
        claim: &AcceptedTcpClaim,
    ) -> Result<Option<AcceptedTcpClosed>, ChannelError> {
        let entry = &claim.entry;
        if !Arc::ptr_eq(&self.identity, &entry.identity) {
            return Err(failure(
                Stage::Close,
                Reason::StalePlan,
                "accepted graph belongs to another listener",
            ));
        }
        let users = entry.users.as_ref().ok_or_else(|| {
            failure(
                Stage::Close,
                Reason::Unsupported,
                "accepted task graph was not declared",
            )
        })?;
        if !users.complete() || !users.original_acks_returned() {
            return Ok(None);
        }
        if entry.state().phase == Phase::Closed {
            self.confirm(entry);
            return Ok(Some(AcceptedTcpClosed {
                entry: entry.clone(),
            }));
        }
        let Some(holder) = entry.control.get() else {
            return Ok(None);
        };
        let Ok(mut control) = holder.try_lock() else {
            return Ok(None);
        };
        match control.try_close_once() {
            Ok(report) if report.is_complete() => {
                entry.state().phase = Phase::Closed;
                drop(control);
                self.confirm(entry);
                Ok(Some(AcceptedTcpClosed {
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
            if entry.state().phase == Phase::Waiting {
                // A pending accept has no native child. Let its actual caller
                // return the reservation after admission was revoked.
                continue;
            }
            if let Some(users) = &entry.users {
                // Cancellation reaches the same I/O and all registered tasks.
                // The real host Scope consumes joins concurrently with this wait.
                if !users.retire(deadline).await {
                    continue;
                }
            }
            let claim = AcceptedTcpClaim { entry };
            match self.close(&claim, deadline).await {
                Err(error) if first.is_none() => first = Some(error),
                Ok(Some(_)) => {
                    // Native closure and user joins can precede the original
                    // Host registration ACK. Wait for that exact registration
                    // before taking a final retained-entry census. Returning
                    // an immediate unfinished snapshot races the Host and can
                    // turn ordinary shutdown into a synthetic Timeout.
                    if let Some(users) = &claim.entry.users {
                        for witness in users.original_acks() {
                            match tokio::time::timeout_at(
                                deadline,
                                witness.wait_registration_released(),
                            )
                            .await
                            {
                                Ok(joined)
                                    if witness.matches_joined(&joined)
                                        && joined.registration_released() => {}
                                _ => break,
                            }
                        }
                    }
                    // The existing exact native/user/ACK predicates remain
                    // authoritative after cancellation or a real deadline.
                    self.confirm(&claim.entry);
                }
                _ => {}
            }
        }
        // Closing the listener wakes an in-flight accept, but that future may
        // still own a known no-native reservation when this branch runs.
        // Wait for its removal before the owner takes the final census.
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let waiting = self
                .state()
                .entries
                .values()
                .any(|entry| entry.state().phase == Phase::Waiting);
            if !waiting || tokio::time::timeout_at(deadline, changed).await.is_err() {
                break;
            }
        }
        first
    }
}
