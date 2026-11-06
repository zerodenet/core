//! The same finite per-connection command jobs retain all actual results.
use super::*;
use std::future::Future;
use tokio::task::JoinSet;
pub(super) struct ConcurrentCommandTasks {
    tasks: JoinSet<io::Result<()>>,
    stop: CancellationToken,
    failures: Vec<io::Error>,
}

struct StopOnPanic(CancellationToken);
impl Drop for StopOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.cancel();
        }
    }
}
impl ConcurrentCommandTasks {
    pub(super) fn new(stop: CancellationToken) -> Self {
        Self {
            tasks: JoinSet::new(),
            stop,
            failures: Vec::new(),
        }
    }

    fn reap_finished(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            if let Err(error) = result
                .map_err(io::Error::other)
                .and_then(std::convert::identity)
            {
                self.failures.push(error);
                self.stop.cancel();
            }
        }
    }

    pub(super) fn try_spawn(
        &mut self,
        future: impl Future<Output = io::Result<()>> + Send + 'static,
    ) -> bool {
        self.reap_finished();
        if self.stop.is_cancelled()
            || !self.failures.is_empty()
            || self.tasks.len() >= MAX_PENDING_CONCURRENT_IPC_COMMANDS
        {
            return false;
        }
        let stop = self.stop.clone();
        self.tasks.spawn(async move {
            let _panic = StopOnPanic(stop.clone());
            tokio::select! {
                _=stop.cancelled()=>Ok(()),
                result=future=>{ if result.is_err() {stop.cancel();} result },
            }
        });
        true
    }

    pub(super) async fn cancel_all(&mut self) -> io::Result<()> {
        self.stop.cancel();
        while let Some(result) = self.tasks.join_next().await {
            if let Err(error) = result
                .map_err(io::Error::other)
                .and_then(std::convert::identity)
            {
                self.failures.push(error);
                self.stop.cancel();
            }
        }
        super::super::owner::finish_work(std::mem::take(&mut self.failures).into_iter().map(Err))
    }
}

#[cfg(test)]
mod tests;
