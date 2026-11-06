//! Strong custody of one actual accepted original, independent of Source life.
use super::*;
use crate::contract::{BoxFuture, scope::TaskJoinWitness};
use crate::controlled_carrier::SourceCustody;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    task::Context,
};
impl Pool {
    pub(in crate::controlled_listener) fn matches_original_scope(
        &self,
        scope: &crate::contract::scope::TaskScopeIdentity,
    ) -> bool {
        self.graph
            .as_ref()
            .is_none_or(|graph| graph.matches_original_scope(scope))
    }

    pub(in crate::controlled_listener) fn bind_original_totals(
        &self,
        binding: super::super::totals::Binding,
    ) -> Result<(), ChannelError> {
        if let Some(actual) = self.originals.get() {
            return if actual.instance == binding.instance
                && actual.root.ptr_eq(&binding.root)
                && actual
                    .scope
                    .scope_identity()
                    .same_scope(&binding.scope.scope_identity())
            {
                Ok(())
            } else {
                Err(failure(
                    Stage::Prepare,
                    Reason::StalePlan,
                    "accepted original changed root",
                ))
            };
        }
        self.originals.set(binding).map_err(|_| {
            failure(
                Stage::Prepare,
                Reason::StalePlan,
                "accepted original already bound",
            )
        })
    }
    pub(super) fn reserve_original(
        self: &Arc<Self>,
        entry: &Arc<Entry>,
    ) -> Result<Option<super::super::totals::Reservation>, ChannelError> {
        let Some(binding) = self.originals.get() else {
            return Ok(None);
        };
        let root = binding
            .root
            .upgrade()
            .ok_or_else(|| failure(Stage::Prepare, Reason::Revoked, "original root retired"))?;
        super::super::totals::Reservation::new(
            root,
            binding.instance,
            Arc::new(Original {
                pool: self.clone(),
                entry: entry.clone(),
                waiting: Mutex::default(),
                polling: AtomicBool::new(false),
                repoll: AtomicBool::new(false),
            }),
        )
        .map(Some)
    }
}
struct Original {
    pool: Arc<Pool>,
    entry: Arc<Entry>,
    waiting: Mutex<Option<BoxFuture<'static, ()>>>,
    polling: AtomicBool,
    repoll: AtomicBool,
}
fn acked(witness: &TaskJoinWitness) -> bool {
    witness
        .try_joined()
        .is_some_and(|joined| witness.matches_joined(&joined) && joined.registration_released())
}
impl SourceCustody for Original {
    fn released(&self) -> bool {
        let phase = self.entry.state().phase;
        if phase == Phase::Waiting && self.entry.control.get().is_none() {
            return true;
        }
        if phase != Phase::Closed {
            return false;
        }
        self.entry
            .users
            .as_ref()
            .is_some_and(|users| users.complete() && users.original_acks().iter().all(acked))
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        let Some(users) = &self.entry.users else {
            return false;
        };
        users.prune_original_acks();
        if self.entry.state().phase != Phase::Closed || !users.complete() {
            return false;
        }
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return false;
        };
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                let originals = users.original_acks();
                Box::pin(async move {
                    for original in originals {
                        original.wait_registration_released().await;
                    }
                })
            });
        let ready = waiting.as_mut().poll(cx).is_ready();
        if !ready {
            *self
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
        }
        ready
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        // Pool and Entry remain strong even when the protocol drops its owner.
        let failure = { self.entry.state().failure.clone() };
        failure.or_else(|| self.pool.state().first_failure.clone())
    }
}
