//! Bounded actual original custody, not a completed-query history.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierAdoption;
pub(super) struct NativeUse {
    pub(super) task: TaskJoinWitness,
    pub(super) cancel: CancellationToken,
    pub(super) original: CarrierAdoption,
    pub(super) capacity: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    pub(super) logical:
        tokio::sync::Mutex<Option<zero_platform_tokio::controlled_carrier::loan::LoanOwner>>,
}
impl NativeUse {
    pub(super) async fn settle(&self, end: tokio::time::Instant) -> std::io::Result<CloseReport> {
        let mut report = CloseReport::default();
        let mut logical = tokio::time::timeout_at(end, self.logical.lock())
            .await
            .map_err(std::io::Error::other)?;
        if let Some(owner) = logical.as_mut() {
            let closed = owner.close(end).await.map_err(std::io::Error::other)?;
            if !closed.is_complete() || !owner.closed().is_some_and(|closed| closed.matches(owner))
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "DNS protocol carrier view remains retained",
                ));
            }
            report.append(closed);
        }
        let closed = self
            .original
            .retire(end)
            .await
            .map_err(std::io::Error::other)?;
        if !closed.is_complete()
            || !self
                .original
                .closed()
                .is_some_and(|closed| closed.matches(&self.original))
            || !self
                .original
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.original))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "DNS original or transferred view is not closed",
            ));
        }
        report.append(closed);
        self.capacity
            .lock()
            .expect("DNS original budget poisoned")
            .take();
        Ok(report)
    }
}
impl Provider {
    pub(super) async fn close_caller_originals(
        &self,
        caller: TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> std::io::Result<()> {
        if !caller.is_joined()
            || !caller
                .scope_identity()
                .same_scope(&self.registrar.scope_identity())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "DNS original cleanup requires its actual joined driver",
            ));
        }
        self.close_attempt_originals(&caller, end).await
    }

    /// Called after the actual taskless open returned, or after the original
    /// query/driver I/O future dropped. Consume the originals before another
    /// candidate or task exit, without minting a join for the active caller.
    pub(super) async fn close_attempt_originals(
        &self,
        caller: &TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> std::io::Result<()> {
        if !caller
            .scope_identity()
            .same_scope(&self.registrar.scope_identity())
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "DNS cleanup requires the actual original caller Scope",
            ));
        }
        let descendants = self.tasks.close_descendants(caller, end).await;
        let entries = self
            .users
            .lock()
            .expect("DNS users poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        for entry in entries {
            entry.settle(end).await?;
            let removed = {
                let mut users = self.users.lock().expect("DNS users poisoned");
                users
                    .iter()
                    .position(|old| Arc::ptr_eq(old, &entry))
                    .map(|index| users.swap_remove(index))
            };
            drop(removed);
        }
        // Original close and nested Scope joins remain separate gates. A
        // failed descendant must not prevent collection of socket originals.
        descendants?;
        self.reap(end).await
    }

    pub(super) async fn failed_open(
        &self,
        opening: std::io::Error,
        original: &CarrierAdoption,
        end: tokio::time::Instant,
    ) -> std::io::Error {
        let cleanup = match original.retire(end).await {
            Ok(report) if report.is_complete() => {
                return zero_dns::native::NativeDnsOpenFailure::after_close(opening, original);
            }
            Ok(_) => std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "DNS failed-open original cleanup remains unconfirmed",
            ),
            Err(source) => std::io::Error::other(source),
        };
        std::io::Error::new(cleanup.kind(), OpenCleanupFailure { opening, cleanup })
    }
    pub(in crate::component::native_dns) async fn reap(
        &self,
        end: tokio::time::Instant,
    ) -> std::io::Result<()> {
        #[cfg(feature = "dns")]
        self.reap_quic(end).await?;
        let entries = self.users.lock().expect("DNS users poisoned").clone();
        for entry in entries {
            // A live query can still be dialing or reading. Its completed
            // original may also be reclaimed during the same task's fallback.
            if !entry.task.is_joined() && entry.original.closed().is_none() {
                continue;
            }
            entry.settle(end).await?;
            // This same ledger also retains a detour's real foreign grants,
            // installed before its handshake and DNS loan handoff. Task
            // failures remain in their original host TaskScope.
            let retired = {
                let mut users = self.users.lock().expect("DNS users poisoned");
                users
                    .iter()
                    .position(|old| Arc::ptr_eq(old, &entry))
                    .map(|index| users.swap_remove(index))
            };
            drop(retired);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct OpenCleanupFailure {
    pub(super) opening: std::io::Error,
    pub(super) cleanup: std::io::Error,
}
impl std::fmt::Display for OpenCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; original cleanup: {}", self.opening, self.cleanup)
    }
}
impl std::error::Error for OpenCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cleanup)
    }
}
