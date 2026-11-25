//! Panic-safe hierarchical [`CancellationToken`] owned by the platform contract.
//!
//! Preserves tree-lock release before invoking external wakers and guarantees
//! every cancelled node is notified even if one waiter's waker panics, before
//! resuming the first unwind payload.

mod futures;
mod tree_node;

pub use futures::{WaitForCancellationFuture, WaitForCancellationFutureOwned};

use core::future::Future;
use futures::RunUntilCancelledFuture;
use std::sync::Arc;

/// A token which can be used to signal a cancellation request to one or more tasks.
pub struct CancellationToken {
    inner: Arc<tree_node::TreeNode>,
}

impl std::panic::UnwindSafe for CancellationToken {}
impl std::panic::RefUnwindSafe for CancellationToken {}

impl core::fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CancellationToken")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        tree_node::increase_handle_refcount(&self.inner);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for CancellationToken {
    fn drop(&mut self) {
        tree_node::decrease_handle_refcount(&self.inner);
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    /// Creates a new [`CancellationToken`] in the non-cancelled state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tree_node::TreeNode::new()),
        }
    }

    /// Creates a child [`CancellationToken`] cancelled when this token is
    /// cancelled, without cancelling the parent when the child is cancelled.
    pub fn child_token(&self) -> Self {
        Self {
            inner: tree_node::child_node(&self.inner),
        }
    }

    /// Cancels this token and all derived child tokens.
    pub fn cancel(&self) {
        tree_node::cancel(&self.inner);
    }

    /// Returns `true` if this token has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        tree_node::is_cancelled(&self.inner)
    }

    /// Returns a [`Future`] fulfilled when cancellation is requested.
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        WaitForCancellationFuture::new(self)
    }

    /// Returns an owned [`Future`] fulfilled when cancellation is requested.
    pub fn cancelled_owned(self) -> WaitForCancellationFutureOwned {
        WaitForCancellationFutureOwned::new(self)
    }

    /// Creates a [`DropGuard`] that cancels this token on drop unless disarmed.
    pub fn drop_guard(self) -> DropGuard {
        DropGuard { inner: Some(self) }
    }

    /// Creates a borrowed [`DropGuardRef`] that cancels this token on drop unless disarmed.
    pub fn drop_guard_ref(&self) -> DropGuardRef<'_> {
        DropGuardRef { inner: Some(self) }
    }

    /// Runs `fut` until completion unless this token is cancelled first.
    pub async fn run_until_cancelled<F>(&self, fut: F) -> Option<F::Output>
    where
        F: Future,
    {
        if self.is_cancelled() {
            None
        } else {
            RunUntilCancelledFuture::new(self, fut).await
        }
    }

    /// Owned counterpart to [`CancellationToken::run_until_cancelled`].
    pub async fn run_until_cancelled_owned<F>(self, fut: F) -> Option<F::Output>
    where
        F: Future,
    {
        self.run_until_cancelled(fut).await
    }
}

/// Wrapper cancelling its [`CancellationToken`] on drop unless disarmed.
#[derive(Debug)]
pub struct DropGuard {
    inner: Option<CancellationToken>,
}

impl DropGuard {
    pub fn disarm(mut self) -> CancellationToken {
        self.inner
            .take()
            .expect("`inner` can only be None in DropGuard::drop")
    }
}

impl Drop for DropGuard {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.cancel();
        }
    }
}

/// Borrowed wrapper cancelling its [`CancellationToken`] on drop unless disarmed.
#[derive(Debug)]
pub struct DropGuardRef<'a> {
    inner: Option<&'a CancellationToken>,
}

impl<'a> DropGuardRef<'a> {
    pub fn disarm(mut self) -> &'a CancellationToken {
        self.inner
            .take()
            .expect("`inner` can only be None in DropGuardRef::drop")
    }
}

impl Drop for DropGuardRef<'_> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner {
            inner.cancel();
        }
    }
}
