//! Same-endpoint transferred views and original caller joins are separate facts.
mod caller;
mod reap;
mod receiver;
mod shutdown;
mod source_attempt;
pub(crate) use receiver::FlowReceiverBinding;
pub(crate) use source_attempt::FlowLogicalClose;
mod stream;
pub(super) use caller::FlowCaller;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
pub(super) use stream::TrackedStream;
use tokio::{sync::Notify, time::Instant};
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, Unfinished},
        scope::TaskJoinWitness,
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    controlled_datagram::metadata::users::{LogicalUserPermit, LogicalUsers, UserAdmission},
};

pub(crate) struct FlowUsers {
    slots: Arc<LogicalUsers<Admission>>,
    state: Mutex<State>,
    changed: Notify,
}
#[derive(Default)]
struct State {
    records: Vec<Arc<Record>>,
    revision: u64,
    unaccounted: bool,
    first: Option<Arc<ChannelError>>,
}
struct Record {
    // Capacity is returned only when BOTH the view was released and the
    // original task was really joined. Stream Drop alone does not refund it.
    _slot: LogicalUserPermit<Admission>,
    caller: Option<TaskJoinWitness>,
    failed_caller: std::sync::OnceLock<zero_platform_tokio::contract::scope::TaskJoined>,
    packet_driver: Option<zero_platform_tokio::contract::component::PacketDriverCallRetained>,
    logical_closed: Option<Arc<std::sync::atomic::AtomicBool>>,
    packet_closed: Option<Arc<std::sync::atomic::AtomicBool>>,
    released: std::sync::atomic::AtomicBool,
    receiver:
        std::sync::OnceLock<zero_platform_tokio::controlled_carrier::loan::CarrierLoanReceiver>,
}
pub(crate) struct FlowUser {
    owner: Arc<FlowUsers>,
    record: Arc<Record>,
    logical_publisher: Arc<
        Mutex<
            Option<Arc<zero_platform_tokio::controlled_carrier::loan::LogicalLoanCloseParticipant>>,
        >,
    >,
}
/// Only the concrete taskless Packet controller can confirm its own logical
/// closure. This never reports the shared source main joined or native closed.
pub(crate) struct PacketSourceClose {
    owner: Arc<FlowUsers>,
    closed: Arc<std::sync::atomic::AtomicBool>,
}
impl PacketSourceClose {
    pub(crate) fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.owner.changed.notify_waiters();
    }
}
struct Admission(CancellationToken);
impl UserAdmission for Admission {
    fn check(&self) -> Result<(), ChannelError> {
        if self.0.is_cancelled() {
            Err(revoked())
        } else {
            Ok(())
        }
    }
}
impl FlowUsers {
    /// Reap only matching actual caller joins AND returned unique views.
    pub(crate) fn quiescent(&self) -> bool {
        self.reap();
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.records.is_empty() && !state.unaccounted && state.first.is_none()
    }

    pub(crate) fn new(limit: NonZeroUsize) -> Arc<Self> {
        let cancel = CancellationToken::new();
        Arc::new(Self {
            slots: LogicalUsers::new(limit, cancel.clone(), Admission(cancel)),
            state: Mutex::new(State::default()),
            changed: Notify::new(),
        })
    }
    /// Same existing users ledger and original caller. No task/native owner or
    /// second user table is created by this internal standard-role projection.
    pub(crate) fn acquire_from_witness(
        self: &Arc<Self>,
        witness: TaskJoinWitness,
    ) -> Result<FlowUser, ChannelError> {
        if witness.is_joined() || !witness.scope_identity().resource_open() {
            return Err(revoked());
        }
        self.acquire(Some(FlowCaller { witness }))
    }
    /// Actual Source main is admission provenance, not an internal driver of
    /// each logical Packet view. Its original host owns whole-source shutdown.
    pub(crate) fn acquire_packet_source(
        self: &Arc<Self>,
        witness: TaskJoinWitness,
    ) -> Result<(FlowUser, PacketSourceClose), ChannelError> {
        if witness.is_joined() || !witness.scope_identity().resource_open() {
            return Err(revoked());
        }
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let user =
            self.acquire_with_completion(Some(FlowCaller { witness }), Some(closed.clone()), None)?;
        Ok((
            user,
            PacketSourceClose {
                owner: self.clone(),
                closed,
            },
        ))
    }
    pub(super) fn acquire(
        self: &Arc<Self>,
        caller: Option<FlowCaller>,
    ) -> Result<FlowUser, ChannelError> {
        self.acquire_with_completion(caller, None, None)
    }
    fn acquire_with_completion(
        self: &Arc<Self>,
        caller: Option<FlowCaller>,
        packet_closed: Option<Arc<std::sync::atomic::AtomicBool>>,
        packet_driver: Option<zero_platform_tokio::contract::component::PacketDriverCallRetained>,
    ) -> Result<FlowUser, ChannelError> {
        self.reap();
        let first = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .first
            .clone();
        if let Some(first) = first {
            // This foreign caller's actual supervisor still owns the fault.
            // Deny new admission until its receipt is accepted and released;
            // do not cancel other healthy views merely to clear this ledger.
            return Err(crate::component::instance::retained(first));
        }
        let slot = self.slots.acquire()?;
        let record = Arc::new(Record {
            _slot: slot,
            caller: caller.map(|caller| caller.witness),
            failed_caller: std::sync::OnceLock::new(),
            packet_closed,
            logical_closed: packet_driver
                .as_ref()
                .map(|_| Arc::new(std::sync::atomic::AtomicBool::new(false))),
            packet_driver,
            released: std::sync::atomic::AtomicBool::new(false),
            receiver: std::sync::OnceLock::new(),
        });
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if self.cancellation().is_cancelled() {
                return Err(revoked());
            }
            let Some(revision) = state.revision.checked_add(1) else {
                state.unaccounted = true;
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::Capacity,
                    Impact::Component,
                ));
            };
            state.revision = revision;
            state.records.push(record.clone());
        }
        Ok(FlowUser {
            owner: self.clone(),
            record,
            logical_publisher: Arc::new(Mutex::new(None)),
        })
    }
    /// An existing leaf API without a same-owner view token cannot establish
    /// users closure. This does not cancel it or remove its legacy capability.
    pub(crate) fn mark_unaccounted(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .unaccounted = true;
        self.changed.notify_waiters();
    }
    pub(crate) fn validates(&self, user: &FlowUser) -> bool {
        std::ptr::eq(self, user.owner.as_ref())
            && !user
                .record
                .released
                .load(std::sync::atomic::Ordering::Acquire)
            && match &user.record.packet_driver {
                Some(actual) => actual.actor_or_receiver_live(),
                None => !user
                    .record
                    .caller
                    .as_ref()
                    .is_some_and(TaskJoinWitness::is_joined),
            }
            && !self.cancellation().is_cancelled()
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        self.slots.cancellation()
    }
    pub(crate) fn revoke(&self) {
        self.cancellation().cancel();
        self.changed.notify_waiters();
    }
}

impl Drop for FlowUser {
    fn drop(&mut self) {
        self.record
            .released
            .store(true, std::sync::atomic::Ordering::Release);
        self.owner.changed.notify_waiters();
    }
}
fn revoked() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session)
}
#[cfg(test)]
mod tests;
