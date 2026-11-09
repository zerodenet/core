//! Read-only lineage of the actual local accepted control connection.
use super::owned::{Peer, Peers};
use std::sync::OnceLock;
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        scope::{TaskId, TaskJoinWitness},
        CancellationToken, ChannelError,
    },
    AcceptedTcpClaim,
};

#[derive(Clone)]
pub struct BrowserOriginal(pub(super) PacketMetadataArc<Peer>);
impl BrowserOriginal {
    pub fn request_stop(&self) {
        self.0.cancel.cancel();
    }
    /// Full original provider receipt, not EOF or task-body completion.
    pub fn is_returned(&self) -> bool {
        self.0
            .closed
            .get()
            .is_some_and(|closed| closed.matches(&self.0.original))
    }
    pub fn same_original(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.0, &other.0)
    }
    pub async fn wait_returned(&self, deadline: Instant) -> bool {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_returned() {
                return true;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return false;
            }
        }
    }
    pub fn original_task(&self) -> Option<(TaskId, TaskJoinWitness)> {
        self.0.task.get().cloned()
    }
}

impl Peer {
    pub(super) fn new(
        original: AcceptedTcpClaim,
        parent: PacketMetadataWeak<Peers>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            original,
            parent,
            cancel,
            task: OnceLock::new(),
            closed: OnceLock::new(),
            changed: tokio::sync::Notify::new(),
            #[cfg(test)]
            read_pending: std::sync::atomic::AtomicBool::new(false),
        }
    }
    pub(super) fn install_task(&self, task: (TaskId, TaskJoinWitness)) -> Result<(), ChannelError> {
        let existing = self.task.get_or_init(|| task.clone());
        if existing.0 != task.0 || !existing.1.same_registration(&task.1) {
            return Err(super::owned::failure(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
            ));
        }
        Ok(())
    }
    pub(super) fn notify(&self) {
        self.changed.notify_waiters();
        if let Some(parent) = self.parent.upgrade() {
            parent.changed.notify_waiters();
        }
    }
}

/// Custody signal only. The original provider still verifies its unique IO
/// view was dropped; neither this signal nor its Drop can mint Closed.
pub(super) struct BrowserUse(pub(super) BrowserOriginal);
impl Drop for BrowserUse {
    fn drop(&mut self) {
        self.0 .0.notify();
    }
}

use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataWeak};
