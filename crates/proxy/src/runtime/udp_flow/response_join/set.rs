//! A single lease of the existing response Set; its owner survives caller Drop.
mod packets;
pub(super) mod receiver;
mod wake;
use super::*;
pub(crate) use packets::PacketReplySender;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::{
    sync::{Mutex as AsyncMutex, OwnedMutexGuard},
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

pub(crate) struct ResponseSetOwner {
    tasks: Arc<AsyncMutex<JoinSet<ChainTask>>>,
    evidence: ResponseTasks,
    wake: Arc<wake::Relay>,
    packets: std::sync::OnceLock<
        zero_traits::packet_metadata::PacketMetadataArc<packets::PacketReplies>,
    >,
    receiver: std::sync::OnceLock<
        zero_traits::packet_metadata::PacketMetadataArc<receiver::PacketReceiver>,
    >,
    revoked: AtomicBool,
    closed: AtomicBool,
}
pub(crate) struct ResponseSetLease {
    tasks: OwnedMutexGuard<JoinSet<ChainTask>>,
    owner: Arc<ResponseSetOwner>,
}
/// Only the same Set's sole real consumer creates this. No native descriptor,
/// component TaskId, transferred user or caller join is implied.
pub(crate) struct ResponseSetClosed {
    owner: Weak<ResponseSetOwner>,
    joined: usize,
    packet_failure: Option<zero_platform_tokio::contract::RetainedError>,
}
impl ResponseSetClosed {
    pub(crate) fn matches(&self, owner: &Arc<ResponseSetOwner>) -> bool {
        self.owner.ptr_eq(&Arc::downgrade(owner)) && owner.is_closed()
    }
    pub(crate) fn packet_failure(&self) -> Option<&zero_platform_tokio::contract::RetainedError> {
        self.packet_failure.as_ref()
    }
    pub(crate) fn joined_tasks(&self) -> usize {
        self.joined
    }
}
impl ResponseSetOwner {
    pub(crate) fn new(original: JoinSet<ChainTask>) -> (Arc<Self>, ResponseSetLease) {
        let tasks = Arc::new(AsyncMutex::new(original));
        let lease = tasks
            .clone()
            .try_lock_owned()
            .expect("new original response Set has one borrower");
        let owner = Arc::new(Self {
            tasks,
            evidence: ResponseTasks::default(),
            wake: Arc::default(),
            packets: Default::default(),
            receiver: Default::default(),
            revoked: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        });
        (
            owner.clone(),
            ResponseSetLease {
                tasks: lease,
                owner,
            },
        )
    }
    pub(crate) fn revoke(&self) {
        self.evidence.expected_cancel.store(true, Ordering::Release);
        self.revoked.store(true, Ordering::Release);
        if let Some(packets) = self.packets.get() {
            packets.close();
        }
        if let Some(receiver) = self.receiver.get() {
            receiver.stop();
        }
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(crate) fn first_failure(&self) -> Option<ResponseFailure> {
        self.evidence
            .statistics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .first
            .clone()
    }
    /// A prior ordinary receive/body error cannot mask a later task panic or
    /// unexpected cancellation. Both are original native JoinError objects.
    pub(crate) fn first_fatal_failure(&self) -> Option<Arc<JoinError>> {
        self.evidence
            .statistics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .fatal
            .clone()
    }
    pub(in crate::runtime::udp_flow) fn evidence(&self) -> &ResponseTasks {
        &self.evidence
    }
    pub(crate) async fn shutdown(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<ResponseSetClosed, ChannelError> {
        self.revoke();
        // The original dispatcher must release its sole lease. Caller join
        // alone never signs response join; a retained borrower remains pending.
        let mut tasks = timeout_at(deadline, self.tasks.lock())
            .await
            .map_err(timeout)?;
        {
            let _deferred = self.wake.enter();
            tasks.abort_all();
        }
        loop {
            let result = timeout_at(deadline, wake::join(&mut tasks, &self.wake))
                .await
                .map_err(timeout)?;
            let Some(result) = result else {
                break;
            };
            // Persist original native identity/source and every private local
            // response receipt before returning/dropping its actual result.
            drop(self.evidence.observe(result));
        }
        assert!(tasks.is_empty(), "only actual joins empty the original Set");
        drop(tasks); // no native Set lock spans original future poll/drop/wake
        if let Some(receiver) = self.receiver.get() {
            receiver.close(deadline).await?;
        }
        self.closed.store(true, Ordering::Release);
        let joined = self
            .evidence
            .statistics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .joined;
        Ok(ResponseSetClosed {
            owner: Arc::downgrade(self),
            joined,
            packet_failure: self.receiver.get().and_then(|receiver| receiver.failure()),
        })
    }
}
impl ResponseSetLease {
    /// This future owns the caller target. The native Set stores only its weak
    /// relay, so cancellation removes the strong caller Waker immediately.
    pub(crate) async fn join_next_with_id(&mut self) -> Option<ObservedChainJoin> {
        if self.owner.revoked.load(Ordering::Acquire) {
            let _deferred = self.owner.wake.enter();
            self.tasks.abort_all();
        }
        match wake::next(
            &mut self.tasks,
            &self.owner.wake,
            self.owner.packets.get().map(|value| &**value),
            self.owner.receiver.get().map(|value| &**value),
        )
        .await?
        {
            wake::Next::Native(result) => Some(self.owner.evidence.observe(result)),
            wake::Next::Packet(packet) => Some(Ok(Ok(packet))),
        }
    }
}
// Compatibility is restricted to the existing synchronous original spawn
// surfaces. Actual join/await callers use the inherent weak-relay method above.
impl Deref for ResponseSetLease {
    type Target = JoinSet<ChainTask>;
    fn deref(&self) -> &Self::Target {
        &self.tasks
    }
}
impl DerefMut for ResponseSetLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tasks
    }
}
fn timeout(source: tokio::time::error::Elapsed) -> ChannelError {
    ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component).with_source(source)
}
#[cfg(test)]
mod tests;
