//! Retained original control tasks and their existing cleanup helpers.
use std::{io, sync::Arc};
mod encoded;
mod initial_snapshot;
pub(crate) use initial_snapshot::{prepare_initial_snapshot, EncodedEvent, EventFraming};
mod error;
mod message;
pub(crate) use encoded::{encode_response, EncodedResponse};
pub(crate) use message::BorrowedMessageResponse;
mod task;
pub(crate) use error::BorrowedErrorResponse;
pub(crate) use task::ControlTask;

/// Catch only the work future's polling boundary. Its parent still owns each
/// actual child join and original IO while it drains cleanup, then resumes the
/// original panic payload so the retained parent JoinHandle records the fault.
pub(crate) async fn catch_work<F: std::future::Future>(
    work: F,
) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Err(payload) => std::task::Poll::Ready(Err(payload)),
        }
    })
    .await
}

/// Consume this same Work's real Apply/raw blocking/cleanup observations.
/// Preserve every original typed source independently of its wire result.
pub(crate) async fn wait_command_operation(
    observation: &zero_runtime::ApplyObservation,
) -> io::Result<()> {
    let joined = observation.wait_joined().await;
    match observation.completion_fault(joined.as_ref()) {
        None => Ok(()),
        Some(faults) => Err(faults.into_io_error()),
    }
}

pub(crate) fn close_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + std::time::Duration::from_secs(5)
}

pub(crate) fn serialized_len(value: &impl serde::Serialize) -> io::Result<usize> {
    struct Count(usize);
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("control event length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    serde_json::to_writer(&mut count, value).map_err(io::Error::other)?;
    Ok(count.0)
}

pub(crate) fn reserve_event_bytes_blocking(
    budget: &Arc<zero_runtime::configuration::ControlBudget>,
    bytes: usize,
    stop: &zero_platform_tokio::contract::CancellationToken,
    closed: impl Fn() -> bool,
) -> Option<zero_runtime::configuration::ControlBudgetPermit> {
    loop {
        if stop.is_cancelled() || closed() {
            return None;
        }
        match budget.reserve(zero_runtime::configuration::ControlCharge::QueuedEventBytes(bytes)) {
            Ok(permit) => return Some(permit),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(25))
            }
            Err(_) => return None,
        }
    }
}

/// All originals have already been joined. Keep every typed failure when a
/// foreground I/O failure and a worker/command failure occur together.
pub(crate) fn finish_work(results: impl IntoIterator<Item = io::Result<()>>) -> io::Result<()> {
    let mut errors = results
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        _ => Err(io::Error::other(ControlFailures(errors))),
    }
}
#[derive(Debug)]
struct ControlFailures(Vec<io::Error>);
impl std::fmt::Display for ControlFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, error) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for ControlFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|error| error as _)
    }
}

#[cfg(test)]
#[path = "tests/completion.rs"]
mod completion_tests;

#[cfg(test)]
#[path = "tests/owner.rs"]
mod tests;

/// A real incomplete Scope/native report remains diagnostic debt, never a receipt.
pub(crate) fn confirmed_report(
    label: &'static str,
    report: zero_platform_tokio::contract::lifecycle::CloseReport,
) -> io::Result<()> {
    if report.is_complete() {
        Ok(())
    } else {
        Err(io::Error::other(IncompleteReport { label, report }))
    }
}
#[derive(Debug)]
struct IncompleteReport {
    label: &'static str,
    report: zero_platform_tokio::contract::lifecycle::CloseReport,
}
impl std::fmt::Display for IncompleteReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} original cleanup remains retained: {:?}",
            self.label, self.report
        )
    }
}
impl std::error::Error for IncompleteReport {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.report
            .failed_tasks
            .first()
            .map(|failed| &failed.error as _)
    }
}

/// Preserve a caught work panic together with real child cleanup errors. With
/// no competing failure, resume the original payload so its parent join sees it.
pub(crate) fn finish_caught(
    work: Result<io::Result<()>, Box<dyn std::any::Any + Send>>,
    cleanup: impl IntoIterator<Item = io::Result<()>>,
) -> io::Result<()> {
    match work {
        Ok(work) => finish_work(std::iter::once(work).chain(cleanup)),
        Err(payload) => {
            let cleanup = cleanup
                .into_iter()
                .filter_map(Result::err)
                .collect::<Vec<_>>();
            if cleanup.is_empty() {
                std::panic::resume_unwind(payload);
            }
            Err(io::Error::other(PanicCleanup {
                payload: std::sync::Mutex::new(payload),
                cleanup,
            }))
        }
    }
}
struct PanicCleanup {
    payload: std::sync::Mutex<Box<dyn std::any::Any + Send>>,
    cleanup: Vec<io::Error>,
}
impl std::fmt::Debug for PanicCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ControlPanicCleanup")
            .field("cleanup", &self.cleanup)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for PanicCleanup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = self
            .payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(message) = payload.downcast_ref::<String>() {
            write!(formatter, "original control work panicked: {message}")?;
        } else if let Some(message) = payload.downcast_ref::<&str>() {
            write!(formatter, "original control work panicked: {message}")?;
        } else {
            formatter.write_str("original control work panicked (actual payload retained)")?;
        }
        for error in &self.cleanup {
            write!(formatter, "; {error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for PanicCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cleanup.first().map(|error| error as _)
    }
}

#[cfg(test)]
#[path = "owner/tests/invocation.rs"]
mod invocation_tests;

pub(crate) mod request_json;
pub(crate) use request_json::decode_ipc;
