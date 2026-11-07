//! Ready follows the first actual software driver poll, never registration.
use super::*;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};
pub(super) async fn run(
    stack: EndpointStack,
    ready: watch::Sender<Option<Result<(), String>>>,
) -> Result<(), EngineError> {
    let closed = stack.closed.clone();
    let driver = run_endpoint_stack(stack);
    tokio::pin!(driver);
    let mut initialized = false;
    let outcome = poll_fn(|cx| match driver.as_mut().poll(cx) {
        Poll::Pending => {
            if !initialized {
                initialized = true;
                #[cfg(test)]
                eprintln!("actual endpoint software driver initialized Pending");
                ready.send_replace(Some(Ok(())));
            }
            Poll::Pending
        }
        Poll::Ready(result) => Poll::Ready(result),
    })
    .await;
    closed.store(true, Ordering::Release);
    if let Err(source) = &outcome {
        ready.send_replace(Some(Err(source.to_string())));
    } else if !initialized {
        ready.send_replace(Some(Err(
            "original endpoint driver ended before initialization".into(),
        )));
    }
    outcome
}
