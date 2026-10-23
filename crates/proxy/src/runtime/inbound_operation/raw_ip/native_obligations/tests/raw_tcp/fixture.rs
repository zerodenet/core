//! Bounded test packet pumps and the stacks' original workers, all really joined.
use super::ChannelError;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::task::JoinSet;
pub(super) struct Workers {
    tasks: Mutex<JoinSet<()>>,
    limit: usize,
}
impl Workers {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            tasks: Mutex::default(),
            limit,
        }
    }
    pub(super) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut tasks = self.tasks.lock().unwrap();
        while let Some(joined) = tasks.try_join_next() {
            joined.unwrap();
        }
        if tasks.len() >= self.limit {
            return false;
        }
        tasks.spawn(future);
        true
    }
    pub(super) async fn close(&self) {
        let mut tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        tasks.abort_all();
        while let Some(joined) = tasks.join_next().await {
            if let Err(error) = joined {
                assert!(error.is_cancelled(), "original worker panicked: {error}");
            }
        }
    }
}
impl zero_stack::TcpTaskOwner for Workers {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        self.spawn(future)
    }
}

// Preserve both boxed std::io payloads and typed Error::source links. A host
// StalePlan rejection must never masquerade as this real native refusal.
pub(super) fn refused_source<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a std::io::Error> {
    let mut current = Some(error);
    for _ in 0..64 {
        let error = current?;
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::ConnectionRefused {
                return Some(io);
            }
            if let Some(inner) = io.get_ref() {
                current = Some(inner);
                continue;
            }
        }
        current = error.source();
    }
    None
}

// Actual retained typed payloads only; never derive a retry/close decision here.
pub(super) fn trace_source(error: &(dyn std::error::Error + 'static)) {
    let mut current = Some(error);
    for index in 0..64 {
        let Some(error) = current else { return };
        eprintln!("raw source[{index}] display={error}, debug={error:?}");
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            eprintln!(
                "raw source[{index}] io_kind={:?}, raw_os_error={:?}",
                io.kind(),
                io.raw_os_error()
            );
            if let Some(inner) = io.get_ref() {
                current = Some(inner);
                continue;
            }
        }
        if let Some(io) = error.downcast_ref::<Arc<std::io::Error>>() {
            current = Some(io.as_ref());
            continue;
        }
        if let Some(channel) = error.downcast_ref::<Arc<ChannelError>>() {
            current = Some(channel.as_ref());
            continue;
        }
        current = error.source();
    }
    eprintln!("raw source chain exceeded bounded diagnostic traversal");
}
