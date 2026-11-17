//! A single lease of the existing response Set; its owner survives caller Drop.
mod packets;
pub(super) mod receiver;
pub(in crate::runtime::udp_flow) mod wake;
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
    tasks: Arc<AsyncMutex<Option<JoinSet<ChainTask>>>>,
    evidence: ResponseTasks,
    #[cfg(feature = "managed-stream-runtime")]
    logical: super::super::registered::logical::receiver::Receivers,
    wake: Arc<wake::Relay>,
    packets: std::sync::OnceLock<
        zero_traits::packet_metadata::PacketMetadataArc<packets::PacketReplies>,
    >,
    receiver: std::sync::OnceLock<
        zero_traits::packet_metadata::PacketMetadataArc<receiver::PacketReceiver>,
    >,
    #[cfg(test)]
    compatibility_created: AtomicBool,
    revoked: AtomicBool,
    closed: AtomicBool,
}
pub(crate) struct ResponseSetLease {
    tasks: OwnedMutexGuard<Option<JoinSet<ChainTask>>>,
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
    pub(crate) fn new() -> (Arc<Self>, ResponseSetLease) {
        let tasks = Arc::new(AsyncMutex::new(None));
        let lease = tasks
            .clone()
            .try_lock_owned()
            .expect("new original response Set has one borrower");
        let owner = Arc::new(Self {
            tasks,
            evidence: ResponseTasks::default(),
            #[cfg(feature = "managed-stream-runtime")]
            logical: Default::default(),
            wake: Arc::default(),
            packets: Default::default(),
            receiver: Default::default(),
            #[cfg(test)]
            compatibility_created: false.into(),
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
    #[cfg(all(test, feature = "managed-stream-runtime"))]
    pub(crate) fn native_readers_for_test(
        &self,
    ) -> Vec<
        zero_traits::packet_metadata::PacketMetadataArc<
            super::super::registered::NativeResponseReceiver,
        >,
    > {
        self.logical.snapshot_for_test()
    }
    #[cfg(test)]
    pub(crate) fn compatibility_created_for_test(&self) -> bool {
        self.compatibility_created.load(Ordering::Acquire)
    }
    pub(crate) fn revoke(&self) {
        self.evidence.expected_cancel.store(true, Ordering::Release);
        self.revoked.store(true, Ordering::Release);
        #[cfg(feature = "managed-stream-runtime")]
        self.logical.stop();
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
    #[cfg(test)]
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
        if let Some(original) = tasks.as_mut() {
            {
                let _deferred = self.wake.enter();
                original.abort_all();
            }
            loop {
                let result = timeout_at(deadline, wake::join(original, &self.wake))
                    .await
                    .map_err(timeout)?;
                let Some(result) = result else {
                    break;
                };
                drop(self.evidence.observe(result));
            }
            assert!(
                original.is_empty(),
                "only actual joins empty the original compatibility Set"
            );
        }
        drop(tasks); // no native Set lock spans original future poll/drop/wake
        if let Some(receiver) = self.receiver.get() {
            receiver.close(deadline).await?;
        }
        #[cfg(feature = "managed-stream-runtime")]
        self.logical.return_after_lease();
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
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tasks.as_ref().map_or(0, JoinSet::len) + {
            #[cfg(feature = "managed-stream-runtime")]
            {
                self.owner.logical.len()
            }
            #[cfg(not(feature = "managed-stream-runtime"))]
            {
                0
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn has_compatibility_tasks_for_test(&self) -> bool {
        self.tasks.is_some()
    }
    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[cfg(feature = "managed-stream-runtime")]
    pub(in crate::runtime::udp_flow) fn install_logical_receiver(
        &mut self,
        receiver: &zero_traits::packet_metadata::PacketMetadataArc<
            super::super::registered::logical::receiver::NativeReceiver,
        >,
    ) -> Result<(), EngineError> {
        if self.owner.revoked.load(Ordering::Acquire) {
            return Err(std::io::Error::other("original response receiver is stopped").into());
        }
        self.owner.logical.insert(receiver)?;
        receiver.bind(&self.owner.wake);
        Ok(())
    }
    #[cfg(feature = "managed-stream-runtime")]
    pub(in crate::runtime::udp_flow) fn return_failed_logical_open(
        &mut self,
        receiver: &zero_traits::packet_metadata::PacketMetadataArc<
            super::super::registered::logical::receiver::NativeReceiver,
        >,
    ) {
        // A first-send failure is still inside this same exclusive dispatch
        // lease. Return its unpolled RX before another fallback can acquire.
        assert_eq!(
            receiver.original.native_task_id(),
            tokio::task::try_id(),
            "same original dispatcher owns failed-open cleanup"
        );
        receiver.return_after_lease();
    }
    /// This future owns the caller target. The native Set stores only its weak
    /// relay, so cancellation removes the strong caller Waker immediately.
    pub(crate) async fn join_next_with_id(&mut self) -> Option<ObservedChainJoin> {
        if self.owner.revoked.load(Ordering::Acquire) {
            let _deferred = self.owner.wake.enter();
            if let Some(original) = self.tasks.as_mut() {
                original.abort_all();
            }
        }
        match wake::next(
            &mut self.tasks,
            &self.owner.wake,
            self.owner.packets.get().map(|value| &**value),
            self.owner.receiver.get().map(|value| &**value),
            #[cfg(feature = "managed-stream-runtime")]
            &self.owner.logical,
        )
        .await?
        {
            wake::Next::Native(result) => Some(self.owner.evidence.observe(result)),
            wake::Next::Packet(packet) => Some(Ok(Ok(packet))),
            #[cfg(feature = "managed-stream-runtime")]
            wake::Next::Logical(packet) => Some(Ok(packet)),
        }
    }
}
// Compatibility is restricted to the existing synchronous original spawn
// surfaces. Actual join/await callers use the inherent weak-relay method above.
impl Deref for ResponseSetLease {
    type Target = JoinSet<ChainTask>;
    fn deref(&self) -> &Self::Target {
        self.tasks
            .as_ref()
            .expect("an actual compatibility response set was acquired")
    }
}
impl DerefMut for ResponseSetLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        #[cfg(test)]
        self.owner
            .compatibility_created
            .store(true, Ordering::Release);
        self.tasks.get_or_insert_with(JoinSet::new)
    }
}
fn timeout(source: tokio::time::error::Elapsed) -> ChannelError {
    ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component).with_source(source)
}
#[cfg(test)]
mod tests;
