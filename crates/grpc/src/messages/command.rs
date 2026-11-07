//! A disconnected handler leaves its actual accepted future in the same task.
use super::Record;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    task::{Context, Poll},
};
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};
type Work = Pin<Box<dyn Future<Output = zero_api::ApiResult<zero_api::CommandResponse>> + Send>>;
#[derive(Default)]
pub(super) struct Command {
    work: Mutex<Option<Work>>,
    credit: Mutex<Option<ControlBudgetPermit>>,
    fault: AtomicBool,
}
impl Record {
    pub(crate) async fn execute<H>(
        &self,
        handle: H,
        command: zero_api::CommandRequest,
    ) -> Result<zero_api::CommandResponse, tonic::Status>
    where
        H: zero_api::CommandService + Send + Sync + 'static,
    {
        let credit = self
            .budget
            .reserve(ControlCharge::PendingCommand)
            .map_err(|error| tonic::Status::resource_exhausted(error.to_string()))?;
        {
            let mut retained = self
                .command
                .credit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if retained.is_some() {
                return Err(tonic::Status::internal("duplicate original gRPC command"));
            }
            *retained = Some(credit);
        }
        *self
            .command
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::pin(async move {
            handle.execute_acknowledged(command).await
        }));
        std::future::poll_fn(|cx| self.command.poll(cx))
            .await
            .ok_or_else(|| tonic::Status::internal("original gRPC command result absent"))?
            .map_err(|error| tonic::Status::internal(error.to_string()))
    }
}
impl Command {
    fn poll(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<zero_api::ApiResult<zero_api::CommandResponse>>> {
        let mut work = match self
            .work
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            Some(work) => work,
            None => return Poll::Ready(None),
        };
        // The handler and drain run sequentially in the same original task.
        // No external code is polled while holding the owner mutex.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Ready(result)) => Poll::Ready(Some(result)),
            Ok(Poll::Pending) => {
                *self
                    .work
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(work);
                Poll::Pending
            }
            Err(payload) => {
                self.fault.store(true, Ordering::Release);
                *self
                    .work
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(work);
                std::panic::resume_unwind(payload)
            }
        }
    }
    pub(super) fn unknown(&self) -> bool {
        self.fault.load(Ordering::Acquire)
    }
    pub(super) async fn drain(&self) {
        if !self.unknown() {
            let _ = std::future::poll_fn(|cx| self.poll(cx)).await;
        }
    }
}
