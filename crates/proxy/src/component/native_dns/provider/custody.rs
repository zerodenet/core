//! Bounded actual original custody, not a completed-query history.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierAdoption;
pub(super) struct NativeUse {
    pub(super) task: TaskJoinWitness,
    pub(super) cancel: CancellationToken,
    pub(super) original: CarrierAdoption,
    // Only pending foreign originals have the actual frozen selection IDs.
    // These metadata identities retain no host, role, driver or provider.
    pub(super) pending_roles: Arc<[zero_core::component::RoleInstanceId]>,
    pub(super) capacity: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
    pub(super) logical:
        tokio::sync::Mutex<Option<zero_platform_tokio::controlled_carrier::loan::LoanOwner>>,
}
impl NativeUse {
    pub(super) fn settle(
        &self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<CloseReport>> {
        Box::pin(async move {
            let mut report = CloseReport::default();
            let mut logical = tokio::time::timeout_at(end, self.logical.lock())
                .await
                .map_err(std::io::Error::other)?;
            if let Some(owner) = logical.as_mut() {
                let closed = owner.close(end).await.map_err(std::io::Error::other)?;
                if !closed.is_complete()
                    || !owner.closed().is_some_and(|closed| closed.matches(owner))
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
        })
    }
}
impl Provider {
    /// Transfer cleanup claims only through actual installed controllers or
    /// an original pool's retained same-Scope driver. Logical return and these
    /// custody proofs never claim native closure or consume a driver failure.
    pub(super) fn release_installed_pending(&self, caller: &TaskJoinWitness) {
        let users = self
            .users
            .lock()
            .expect("DNS users poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        let pending = self
            .pending
            .lock()
            .expect("DNS pending originals poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        // Borrow the actual current plan for this synchronous check only.
        // No persistent cleanup record keeps a graph or old configuration alive.
        let current = self
            .network
            .current_services()
            .ok()
            .and_then(|source| source.runtime_services().ok())
            .map(|services| services.execution().components);
        let plan = current.as_ref().and_then(|plan| plan.as_ref().ok());
        let loans = users
            .iter()
            .chain(pending.iter())
            .filter_map(|entry| {
                let proof = entry.original.retained_loan()?;
                proof
                    .receiver_adoption()
                    .is_some_and(|receiver| receiver.same_slot(&entry.original))
                    .then_some((entry.original.clone(), proof))
            })
            .collect::<Vec<_>>();
        let transferred = pending
            .into_iter()
            .filter(|entry| {
                let actual_pool_driver = entry.original.retained_driver().is_some_and(|proof| {
                    proof.retains(&entry.original)
                        && proof
                            .adoption()
                            .is_some_and(|adoption| adoption.same_slot(&entry.original))
                        && proof
                            .owner()
                            .is_some_and(|owner| owner.matches_adoption(&entry.original))
                        && plan.is_some_and(|plan| {
                            entry.pending_roles.iter().any(|role| {
                                plan.validate_carrier_adoption(*role, &entry.original)
                                    .is_ok()
                            })
                        })
                });
                // Before its first driver exists, an undelivered receiver stays in
                // this caller ledger. Once its real Pool Scope owns the registered
                // driver, cancelling this waiter must preserve other pool users.
                actual_pool_driver
                    || loans.iter().any(|(receiver, proof)| {
                        !receiver.same_slot(&entry.original) && proof.retains(&entry.original)
                    })
            })
            .collect::<Vec<_>>();
        // Proof checks can upgrade original owners and deliver their callbacks;
        // never execute them under this caller's custody mutex.
        self.pending
            .lock()
            .expect("DNS pending originals poisoned")
            .retain(|entry| {
                !transferred
                    .iter()
                    .any(|retained| Arc::ptr_eq(retained, entry))
            });
    }
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
    pub(super) fn close_attempt_originals<'a>(
        &'a self,
        caller: &'a TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> BoxFuture<'a, std::io::Result<()>> {
        Box::pin(async move {
            if !caller
                .scope_identity()
                .same_scope(&self.registrar.scope_identity())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "DNS cleanup requires the actual original caller Scope",
                ));
            }
            self.release_installed_pending(caller);
            let bootstrap = self
                .network
                .native_dns
                .close_bootstrap_descendants(caller, end)
                .await;
            let descendants = self.tasks.close_descendants(caller, end).await;
            let entries = self
                .users
                .lock()
                .expect("DNS users poisoned")
                .iter()
                .filter(|entry| entry.task.same_registration(caller))
                .cloned()
                .collect::<Vec<_>>();
            let mut original_failure = None;
            for entry in entries {
                if let Err(error) = entry.settle(end).await {
                    original_failure.get_or_insert(error);
                    continue;
                }
                let removed = {
                    let mut users = self.users.lock().expect("DNS users poisoned");
                    users
                        .iter()
                        .position(|old| Arc::ptr_eq(old, &entry))
                        .map(|index| users.swap_remove(index))
                };
                drop(removed);
            }
            let pending = self.close_pending_originals(caller, end).await;
            // Original close and nested Scope joins remain separate gates. A
            // failed descendant must not prevent collection of socket originals.
            descendants?;
            bootstrap?;
            if let Some(error) = original_failure {
                return Err(error);
            }
            pending?;
            self.reap(end).await
        })
    }

    async fn close_pending_originals(
        &self,
        caller: &TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> std::io::Result<()> {
        // Retire receivers before their transferred donors. Every entry is an
        // actual pre-await adoption; successful query installation removes only
        // originals whose real logical controller was transferred out of here.
        let entries = self
            .pending
            .lock()
            .expect("DNS pending originals poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        let mut failure = None;
        for entry in entries {
            match entry.settle(end).await {
                Ok(_) => {
                    let removed = {
                        let mut pending =
                            self.pending.lock().expect("DNS pending originals poisoned");
                        pending
                            .iter()
                            .position(|old| Arc::ptr_eq(old, &entry))
                            .map(|index| pending.remove(index))
                    };
                    drop(removed);
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
        }
        failure.map_or(Ok(()), Err)
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
    pub(in crate::component::native_dns) fn reap(
        &self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move {
            #[cfg(feature = "dns")]
            self.reap_quic(end).await?;
            let callers = self
                .pending
                .lock()
                .expect("DNS pending originals poisoned")
                .iter()
                .filter(|entry| entry.task.is_joined())
                .map(|entry| entry.task.clone())
                .collect::<Vec<_>>();
            for caller in callers {
                self.release_installed_pending(&caller);
                self.close_pending_originals(&caller, end).await?;
            }
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
        })
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
