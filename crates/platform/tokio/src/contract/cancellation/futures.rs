//! Borrowed and owned cancellation wait futures.

use super::CancellationToken;
use core::{
    future::Future,
    marker::PhantomPinned,
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll},
};
use std::sync::Arc;

/// Prevents the compiler from asserting interior references inside `T` remain
/// dereferenceable after `T`'s destructor runs, while preserving `!Unpin`.
#[repr(transparent)]
struct MaybeDangling<T>(MaybeUninit<T>, PhantomPinned);

impl<T> MaybeDangling<T> {
    fn new(inner: T) -> Self {
        Self(MaybeUninit::new(inner), PhantomPinned)
    }
}

impl<T> Drop for MaybeDangling<T> {
    fn drop(&mut self) {
        // SAFETY: `0` is always initialized until drop runs.
        unsafe { core::ptr::drop_in_place(self.0.as_mut_ptr()) };
    }
}

impl<F: Future> Future for MaybeDangling<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `0` is always initialized and structurally pinned.
        let fut = unsafe { self.map_unchecked_mut(|this| this.0.assume_init_mut()) };
        fut.poll(cx)
    }
}

/// A [`Future`] resolved once the corresponding [`CancellationToken`] is cancelled.
#[must_use = "futures do nothing unless polled"]
pub struct WaitForCancellationFuture<'a> {
    cancellation_token: &'a CancellationToken,
    future: tokio::sync::futures::Notified<'a>,
}

impl<'a> WaitForCancellationFuture<'a> {
    pub(super) fn new(cancellation_token: &'a CancellationToken) -> Self {
        Self {
            cancellation_token,
            future: cancellation_token.inner.notified(),
        }
    }
}

impl core::fmt::Debug for WaitForCancellationFuture<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WaitForCancellationFuture").finish()
    }
}

impl Future for WaitForCancellationFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `future` is structurally pinned (`Notified` is `!Unpin`, so
        // `Self` is `!Unpin`), and `cancellation_token` is a plain reference.
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            if this.cancellation_token.is_cancelled() {
                return Poll::Ready(());
            }
            let mut future = unsafe { Pin::new_unchecked(&mut this.future) };
            if future.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            future.set(this.cancellation_token.inner.notified());
        }
    }
}

/// Owned counterpart to [`WaitForCancellationFuture`].
#[must_use = "futures do nothing unless polled"]
pub struct WaitForCancellationFutureOwned {
    // Field order is load-bearing: `future` borrows `cancellation_token.inner`
    // and MUST be dropped before `cancellation_token`.
    future: MaybeDangling<tokio::sync::futures::Notified<'static>>,
    cancellation_token: CancellationToken,
}

impl core::fmt::Debug for WaitForCancellationFutureOwned {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WaitForCancellationFutureOwned").finish()
    }
}

impl WaitForCancellationFutureOwned {
    pub(super) fn new(cancellation_token: CancellationToken) -> Self {
        Self {
            // SAFETY: `cancellation_token.inner` is heap-allocated via `Arc`
            // and outlives `future` due to field drop order.
            future: MaybeDangling::new(unsafe { Self::new_future(&cancellation_token) }),
            cancellation_token,
        }
    }

    /// # Safety
    /// The returned `Notified` must be dropped before `cancellation_token`.
    unsafe fn new_future(
        cancellation_token: &CancellationToken,
    ) -> tokio::sync::futures::Notified<'static> {
        let inner_ptr = Arc::as_ptr(&cancellation_token.inner);
        unsafe { (*inner_ptr).notified() }
    }
}

impl Future for WaitForCancellationFutureOwned {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: `future` is structurally pinned (`MaybeDangling` contains
        // `PhantomPinned`, so `Self` is `!Unpin`).
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            if this.cancellation_token.is_cancelled() {
                return Poll::Ready(());
            }
            let mut future = unsafe { Pin::new_unchecked(&mut this.future) };
            if future.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            // SAFETY: `cancellation_token` drops after `future`.
            future.set(MaybeDangling::new(unsafe {
                Self::new_future(&this.cancellation_token)
            }));
        }
    }
}

#[must_use = "futures do nothing unless polled"]
pub(super) struct RunUntilCancelledFuture<'a, F: Future> {
    cancellation: WaitForCancellationFuture<'a>,
    future: F,
    _pin: PhantomPinned,
}

impl<'a, F: Future> RunUntilCancelledFuture<'a, F> {
    pub(super) fn new(cancellation_token: &'a CancellationToken, future: F) -> Self {
        Self {
            cancellation: cancellation_token.cancelled(),
            future,
            _pin: PhantomPinned,
        }
    }
}

impl<F: Future> Future for RunUntilCancelledFuture<'_, F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: Both `future` and `cancellation` are structurally pinned and
        // `Self` is unconditionally `!Unpin` via `PhantomPinned`.
        let this = unsafe { self.get_unchecked_mut() };
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(res) = future.poll(cx) {
            return Poll::Ready(Some(res));
        }
        let cancellation = unsafe { Pin::new_unchecked(&mut this.cancellation) };
        if cancellation.poll(cx).is_ready() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn check() {
        assert_send_sync::<CancellationToken>();
        assert_send_sync::<WaitForCancellationFuture<'static>>();
        assert_send_sync::<WaitForCancellationFutureOwned>();
    }
    let _ = check;
};
