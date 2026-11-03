//! The owning receiver retains the real receipt before local error delivery.
use super::*;
use crate::contract::scope::{TaskClass, TaskJoined};

/// Weak access to the same listener's bounded entry table. No second task map,
/// scope owner, task joiner, or permission to fabricate TaskJoined is created.
#[derive(Clone)]
pub struct AcceptedTcpTaskReceiver(Weak<Pool>, Arc<std::sync::atomic::AtomicUsize>);
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
        Self(Arc::downgrade(pool), pool.logical_custody.clone())
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
            if joined.class() == TaskClass::Auxiliary {
                // Hosted local delivery is distinct from the host's actual
                // non-cloneable acknowledgement and never refunds Scope credit.
                if node.witness.acknowledge_hosted_failure(&joined).is_err() {
                    return None;
                }
            }
            drop(source);
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
            state.tasks.remove(&id)
        };
        // A last receipt/source Drop and Notify can run callbacks; no graph or
        // host mutex encloses either operation.
        drop(removed);
        self.changed.notify_waiters();
        Some(joined)
    }
}
