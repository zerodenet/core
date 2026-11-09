//! The owning receiver retains the real receipt before local error delivery.
use super::*;
use crate::contract::scope::{TaskClass, TaskJoined};

/// Weak access to the same listener's bounded entry table. No second task map,
/// scope owner, task joiner, or permission to fabricate TaskJoined is created.
#[derive(Clone)]
pub struct AcceptedTcpTaskReceiver(
    Weak<Pool>,
    Arc<std::sync::atomic::AtomicUsize>,
    Option<AcceptedTaskFailures>,
);
impl AcceptedTcpTaskReceiver {
    /// Retire logical originals even when their waiting caller was cancelled.
    /// Only the original Scope's consumed witnesses permit this operation.
    pub async fn retire_logical_resources(&self, deadline: Instant) -> Result<(), ChannelError> {
        let Some(pool) = self.0.upgrade() else {
            // This proves no remaining logical custody only. It is recorded
            // after original cleanup, never from Pool Drop or a native-TCP claim.
            return if self.1.load(Ordering::Acquire) == 0 {
                Ok(())
            } else {
                Err(failure(
                    Stage::Close,
                    Reason::Revoked,
                    "accepted logical originals disappeared without cleanup",
                ))
            };
        };
        let users: Vec<_> = pool
            .state()
            .entries
            .values()
            .filter_map(|entry| entry.users.clone())
            .collect();
        for users in users {
            if !users.logical_dirty.swap(false, Ordering::AcqRel) {
                continue;
            }
            if let Err(error) = users.retire_joined_logical_resources(deadline).await {
                users.logical_dirty.store(true, Ordering::Release);
                return Err(error);
            }
            if !users.retire_joined_carriers(deadline).await {
                users.logical_dirty.store(true, Ordering::Release);
                let source = users
                    .entry
                    .upgrade()
                    .and_then(|entry| entry.state().failure.clone());
                return Err(source.map_or_else(
                    || {
                        failure(
                            Stage::Close,
                            Reason::Timeout,
                            "joined caller carrier cleanup remains pending",
                        )
                    },
                    retained,
                ));
            }
        }
        Ok(())
    }
    pub(in crate::controlled_listener::accepted) fn new(pool: &Arc<Pool>) -> Self {
        Self(
            Arc::downgrade(pool),
            pool.logical_custody.clone(),
            pool.task_failures.get().cloned(),
        )
    }
    pub fn task_failures(&self) -> Option<&AcceptedTaskFailures> {
        self.2.as_ref()
    }
    pub fn limit(&self) -> Option<NonZeroUsize> {
        self.0.upgrade().map(|pool| pool.limit)
    }
    /// Call from the actual Scope exit consumer, outside all host locks. The ID
    /// selects an existing entry's witness; only that witness's actual joined
    /// receipt can be returned. A body exit or an arbitrary ID is insufficient.
    /// Drive the same persistent entry after the actual host Scope consumes its
    /// exits. Losing this wait never discards the entry/control or refunds cap.
    pub async fn close(
        &self,
        claim: &AcceptedTcpClaim,
        deadline: Instant,
    ) -> Result<Option<AcceptedTcpClosed>, ChannelError> {
        let pool = self.0.upgrade().ok_or_else(|| {
            failure(
                Stage::Close,
                Reason::Revoked,
                "accepted owner disappeared without closure proof",
            )
        })?;
        pool.close_graph(claim, deadline).await
    }
    /// Probe only already-consumed actual joins and returned custody. This
    /// never cancels children or waits for the Scope whose exit consumer is
    /// calling it; native close remains the original provider's operation.
    pub async fn close_if_joined(
        &self,
        claim: &AcceptedTcpClaim,
        deadline: Instant,
    ) -> Result<Option<AcceptedTcpClosed>, ChannelError> {
        let pool = self.0.upgrade().ok_or_else(|| {
            failure(
                Stage::Close,
                Reason::Revoked,
                "accepted owner disappeared without closure proof",
            )
        })?;
        if !Arc::ptr_eq(&pool.identity, &claim.entry.identity) {
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
        if !users.complete() {
            return Ok(None);
        }
        pool.close_graph(claim, deadline).await
    }
    pub fn observe_joined(&self, id: TaskId) -> Option<TaskJoined> {
        let pool = self.0.upgrade()?;
        let users: Vec<_> = pool
            .state()
            .entries
            .values()
            .filter_map(|entry| entry.users.clone())
            .collect();
        users.into_iter().find_map(|users| users.consume_joined(id))
    }
}
impl Users {
    pub(super) fn consume_joined(&self, id: TaskId) -> Option<TaskJoined> {
        let node = self.state().tasks.get(&id).cloned()?;
        // The actual Scope, never this graph, consumes the native task exit.
        let joined = node.witness.try_joined()?;
        if node.process_helper {
            if let Some(original) = &node.process {
                if let Err(source) = original.collect_native_after_join() {
                    self.remember(Arc::new(source));
                }
            }
        }
        if joined.result().is_ok() {
            if let Some(fault) = &node.fault {
                if !fault.matches(&node.witness) || fault.receive(&joined).is_err() {
                    return None;
                }
            }
        }
        if let Err(source) = joined.result() {
            // Persist original ID/class/Arc before accepting a local client
            // rejection. This bounded first receipt survives slot reuse.
            if let Some(pool) = self.pool.upgrade() {
                pool.state()
                    .first_user_failure
                    .get_or_insert_with(|| joined.clone());
            } else if let Some(peer) = &self.peer {
                peer.user_failure
                    .lock()
                    .unwrap()
                    .get_or_insert_with(|| joined.clone());
            } else {
                return None;
            }
            if !node.witness.is_hosted() {
                // Every ordinary error stays in THIS original finite Node,
                // including secondary errors. The first diagnostic is not its
                // receiver. No extra history table or Source/controller edge.
                if !self.retain_ordinary_caller_failure(id, &node.witness, &joined, &source) {
                    return None;
                }
            } else if joined.class() == TaskClass::Auxiliary {
                if let Some(fault) = &node.fault {
                    if !fault.matches(&node.witness) || fault.receive(&joined).is_err() {
                        return None;
                    }
                } else if self.task_failures.get().is_some() {
                    return None;
                }
                // Hosted local delivery is distinct from the host's actual
                // non-cloneable acknowledgement and never refunds Scope credit.
                if node.witness.acknowledge_hosted_failure(&joined).is_err() {
                    return None;
                }
            }
            drop(source);
        }
        // Preserve this actual registration across local user/native closure.
        // The existing Host's eventual ACK, not this receiver, refunds totals.
        self.prune_original_acks();
        {
            let mut acks = self
                .pending_acks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !acks
                .iter()
                .any(|actual| actual.same_registration(&node.witness))
            {
                acks.push(node.witness.clone());
            }
        }
        let carrier = self.has_carrier_caller(&node.witness);
        let removed = {
            let mut state = self.state();
            if node.primary {
                state.sealed = true;
            }
            if carrier
                || state.resources.iter().any(|resource| {
                    let Some(creator) = &resource.logical_owner else {
                        return false;
                    };
                    (match resource.driver.get() {
                        Some(driver) => driver.get(),
                        None => creator.get(),
                    })
                    .is_some_and(|(driver, _)| *driver == id)
                })
            {
                self.logical_dirty.store(true, Ordering::Release);
            }
            if node.process_helper
                || state
                    .tasks
                    .get(&id)
                    .is_some_and(|node| node.received_failure.is_some() && node.fault.is_none())
            {
                // Deliver the original fault first so the actual Scope can
                // ACK. The helper's native controller and credit stay until
                // that same registration is released, not just body/join.
                None
            } else {
                state.tasks.remove(&id)
            }
        };
        // A last receipt/source Drop and Notify can run callbacks; no graph or
        // host mutex encloses either operation.
        drop(removed);
        self.changed.notify_waiters();
        Some(joined)
    }
}

impl Users {
    pub(in crate::controlled_listener::accepted) fn original_acks(&self) -> Vec<TaskJoinWitness> {
        self.pending_acks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(in crate::controlled_listener::accepted) fn prune_original_acks(&self) {
        let snapshot = self.original_acks();
        let released = snapshot
            .iter()
            .filter(|witness| {
                witness.try_joined().is_some_and(|joined| {
                    witness.matches_joined(&joined) && joined.registration_released()
                })
            })
            .collect::<Vec<_>>();
        {
            let mut acks = self
                .pending_acks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            acks.retain(|actual| {
                !released
                    .iter()
                    .any(|witness| witness.same_registration(actual))
            });
        }
        // Snapshot holds each removed witness through the original lock; its
        // last source/controller destructor can only run here, outside it.
        drop(released);
        drop(snapshot);
    }
}
