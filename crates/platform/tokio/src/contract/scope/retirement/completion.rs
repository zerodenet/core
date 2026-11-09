//! Pure paid completion, issued by the actual registrar without retaining Scope.
use super::*;
use std::{io, sync::atomic::AtomicBool};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataWeak};
pub(in crate::contract::scope) struct CompletionFlag {
    drained: AtomicBool,
    memory: zero_tun::packet_memory::PacketDriverMetadataView,
}
impl CompletionFlag {
    pub(in crate::contract::scope) fn seal(&self) {
        self.drained.store(true, Ordering::Release);
    }
}
#[derive(Clone)]
pub struct ScopeRetirementCompletion {
    scope: TaskScopeIdentity,
    complete: PacketMetadataArc<CompletionFlag>,
}
impl ScopeRetirementCompletion {
    pub fn scope_identity(&self) -> &TaskScopeIdentity {
        &self.scope
    }
    pub fn is_drained(&self) -> bool {
        self.complete.drained.load(Ordering::Acquire)
    }
}
impl WeakTaskRegistrar {
    /// Same actual Scope only. The concrete caller prepays this pure flag's
    /// body/header; it grants no registration, joining, ACK or native authority.
    pub fn retirement_completion(
        &self,
        memory: &zero_tun::packet_memory::PacketDriverMetadataView,
    ) -> io::Result<ScopeRetirementCompletion> {
        let _notifications = memory.notifications();
        let observation = self
            .retirement_observation()
            .ok_or_else(|| io::Error::other("actual Scope expired"))?;
        let owner = &observation.0;
        let existing = owner
            .lock_state()
            .retirement
            .as_ref()
            .and_then(PacketMetadataWeak::upgrade);
        let complete = if let Some(existing) = existing {
            existing
        } else {
            let layout = PacketMetadataArc::<CompletionFlag>::layout().map_err(io::Error::other)?;
            let credit = memory.reserve(layout).map_err(io::Error::other)?;
            let actual = PacketMetadataArc::from_prepaid(
                CompletionFlag {
                    drained: AtomicBool::new(false),
                    memory: memory.clone(),
                },
                credit,
            )
            .map_err(|(source, _, _)| io::Error::other(source))?;
            let (previous, retired) = {
                let mut state = owner.lock_state();
                let previous = state
                    .retirement
                    .as_ref()
                    .and_then(PacketMetadataWeak::upgrade);
                let retired = if previous.is_none() {
                    state
                        .retirement
                        .replace(PacketMetadataArc::downgrade(&actual))
                } else {
                    None
                };
                (previous, retired)
            };
            drop(retired); // weak/header refund only after Scope State unlock
            previous.unwrap_or(actual)
        };
        if !complete.memory.same_root(memory) {
            return Err(io::Error::other(
                "retirement completion belongs to another actual Host metadata Root",
            ));
        }
        // Reacquire the same census so a cold empty closing Scope and an ACK
        // racing initial installation both seal through the ordinary unlock.
        drop(owner.lock_state());
        Ok(ScopeRetirementCompletion {
            scope: self.scope_identity(),
            complete,
        })
    }
}

impl Shared {
    pub(in crate::contract::scope) fn seal_retirement_completion(&self) {
        let completion = {
            let Ok(state) = self.state.lock() else { return };
            (state.closing
                && state.records.is_empty()
                && state.tasks.is_empty()
                && state.completed.is_empty()
                && state.pending_launches == 0)
                .then(|| state.retirement.clone())
                .flatten()
        };
        if let Some(completion) = completion.and_then(|completion| completion.upgrade()) {
            completion.seal();
        }
    }
}
