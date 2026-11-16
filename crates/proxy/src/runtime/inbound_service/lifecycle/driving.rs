//! Poll the same original task owner beside startup/reconcile, without a new
//! observer task or holding its state mutex while the operation is polled.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
impl InboundServices {
    pub(crate) async fn drive<F: Future>(&self, operation: F) -> F::Output {
        tokio::pin!(operation);
        poll_fn(|cx| {
            for turn in 0..32 {
                let Ok(mut owner) = self.owner.try_lock() else {
                    break;
                };
                let Some(scope) = owner.scope.as_mut() else {
                    break;
                };
                match scope.poll_exit(cx) {
                    Poll::Ready(Some(receipt)) => {
                        let _ = owner.receive(receipt);
                        if owner.ack_failure.is_some() {
                            break;
                        }
                        if turn == 31 {
                            cx.waker().wake_by_ref();
                        }
                    }
                    Poll::Pending | Poll::Ready(None) => break,
                }
            }
            // A closing/replacing operation may need this same owner lock.
            // All temporary guards above have ended before it is polled.
            operation.as_mut().poll(cx)
        })
        .await
    }
}

#[cfg(test)]
mod tests;
