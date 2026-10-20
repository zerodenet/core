//! Bounded test packet pumps and the stacks' original workers, all really joined.
use std::{future::Future, pin::Pin, sync::Mutex};
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
