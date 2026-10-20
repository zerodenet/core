//! The owning receiver retains the real receipt before local error delivery.
use super::*;
use crate::contract::scope::{TaskClass, TaskJoined};

/// Weak access to the same listener's bounded entry table. No second task map,
/// scope owner, task joiner, or permission to fabricate TaskJoined is created.
#[derive(Clone)]
pub struct AcceptedTcpTaskReceiver(Weak<Pool>);
impl AcceptedTcpTaskReceiver {
    pub(super) fn new(pool: &Arc<Pool>) -> Self {
        Self(Arc::downgrade(pool))
    }
    /// Call from the actual Scope exit consumer, outside all host locks. The ID
    /// selects an existing entry's witness; only that witness's actual joined
    /// receipt can be returned. A body exit or an arbitrary ID is insufficient.
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
    pub(super) fn reap_joined(&self) {
        let ids: Vec<_> = self.state().tasks.keys().copied().collect();
        for id in ids {
            self.consume_joined(id);
        }
    }
    pub(super) fn consume_joined(&self, id: TaskId) -> Option<TaskJoined> {
        let node = self.state().tasks.get(&id).cloned()?;
        // The actual Scope, never this graph, consumes the native task exit.
        let joined = node.witness.try_joined()?;
        if let Err(source) = joined.result() {
            // Persist original ID/class/Arc before accepting a local client
            // rejection. This bounded first receipt survives slot reuse.
            let pool = self.pool.upgrade()?;
            pool.state()
                .first_user_failure
                .get_or_insert_with(|| joined.clone());
            if joined.class() == TaskClass::Auxiliary {
                // Hosted local delivery is distinct from the host's actual
                // non-cloneable acknowledgement and never refunds Scope credit.
                if node.witness.acknowledge_hosted_failure(&joined).is_err() {
                    return None;
                }
            }
            drop(source);
        }
        let removed = {
            let mut state = self.state();
            if node.primary {
                state.sealed = true;
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
