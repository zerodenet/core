//! Explicit per-carrier receiver; no task, native owner, queue or history.
use super::failure::Failure;
use std::sync::Arc;
use zero_platform_tokio::contract::scope::{HostedReceiptError, TaskJoinWitness, TaskJoined};

/// Retain alongside the actual caller's original CarrierOwner for no-user
/// cleanup. Dropping request handles or this receiver is never an acceptance.
/// Acceptance proves delivery only; the original native owner must still close.
#[derive(Clone)]
pub struct LocalTaskReceiver {
    failure: Arc<Failure>,
    witness: TaskJoinWitness,
}
impl LocalTaskReceiver {
    pub(super) fn new(failure: Arc<Failure>, witness: TaskJoinWitness) -> Self {
        Self { failure, witness }
    }
    /// Only a real already joined exit can be persisted and accepted. Pending
    /// returns None, preserving credit. The same exact receipt is retry-safe.
    pub fn receive_joined(&self) -> Result<Option<TaskJoined>, HostedReceiptError> {
        self.failure.receive_joined()
    }
    /// The caller's actual supervisor must drive joins while this wait is
    /// pending. Cancelling this wait keeps the original receiver and credit.
    pub async fn wait_and_receive(&self) -> Result<TaskJoined, HostedReceiptError> {
        self.witness.wait().await;
        self.receive_joined()
            .map(|joined| joined.expect("the actual witness completed"))
    }
    /// Read-only local evidence; reading it does not acknowledge anything.
    pub fn received(&self) -> Option<TaskJoined> {
        self.failure.received()
    }
}
