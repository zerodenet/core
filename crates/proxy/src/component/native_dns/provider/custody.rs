//! Bounded actual original custody, not a completed-query history.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierAdoption;
mod failure;
mod report;
mod settle;
pub(super) use failure::OpenCleanupFailure;
pub(super) struct NativeUse {
    pub(super) receiver: super::failure::Receiver,
    // The same original's partial report survives cancellation between its
    // logical and native close awaits. This is not a completed-query history.
    pub(super) pending_report: Mutex<CloseReport>,
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
        // Cleanup validates the real catalog's retained original slots. An
        // Engine-staged candidate is neither the old pool's owner nor evidence
        // that its actual registered driver stopped retaining another user.
        let current = self.network.current_services().ok();
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
                        && current.as_ref().is_some_and(|source| {
                            entry.pending_roles.iter().any(|role| {
                                source
                                    .validate_owned_carrier_adoption(*role, &entry.original)
                                    .is_ok()
                            })
                        })
                });
                // Before its first driver exists, an undelivered receiver stays in
                // this caller ledger. Once its real Pool Scope owns the registered
                // driver, cancelling this waiter must preserve other pool users.
                let actual_job = entry
                    .original
                    .retained_preparation_job()
                    .is_some_and(|proof| {
                        proof.retains(&entry.original)
                            && proof
                                .owner()
                                .is_some_and(|owner| owner.matches_adoption(&entry.original))
                            && current.as_ref().is_some_and(|source| {
                                entry.pending_roles.iter().any(|role| {
                                    source
                                        .validate_owned_carrier_adoption(*role, &entry.original)
                                        .is_ok()
                                })
                            })
                    });
                actual_pool_driver
                    || actual_job
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
        self.release_installed_pending(caller);
        let bootstrap = match self
            .network
            .native_dns
            .close_bootstrap_descendants(caller, end)
            .await
        {
            Ok(()) => Ok(()),
            Err(source) => Err(self.retain_io_cleanup_failure(source)?),
        };
        let descendants = match self.tasks.close_descendants(caller, end).await {
            Ok(()) => Ok(()),
            Err(source) => Err(self.retain_io_cleanup_failure(source)?),
        };
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
                let original = self.retain_io_cleanup_failure(error)?;
                original_failure.get_or_insert(original);
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
                    let original = self.retain_io_cleanup_failure(error)?;
                    failure.get_or_insert(original);
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(in crate::component::native_dns) async fn reap(
        &self,
        end: tokio::time::Instant,
    ) -> std::io::Result<()> {
        let mut failure = None;
        #[cfg(feature = "dns")]
        if let Err(source) = self.reap_quic(end).await {
            failure = Some(self.retain_io_cleanup_failure(source)?);
        }
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
            if let Err(source) = self.close_pending_originals(&caller, end).await {
                let original = self.retain_io_cleanup_failure(source)?;
                failure.get_or_insert(original);
            }
        }
        let entries = self.users.lock().expect("DNS users poisoned").clone();
        for entry in entries {
            // A live query can still be dialing or reading. Its completed
            // original may also be reclaimed during the same task's fallback.
            if !entry.task.is_joined() && entry.original.closed().is_none() {
                continue;
            }
            if let Err(source) = entry.settle(end).await {
                let original = self.retain_io_cleanup_failure(source)?;
                failure.get_or_insert(original);
                continue;
            }
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
        failure.map_or(Ok(()), Err)
    }
}
