//! Main's installed logger Scope and native controllers, never a new Host.
use std::{future::Future, io, num::NonZeroUsize, task::Poll, time::Duration};
use zero_logging::{LoggingHandle, ScopedTracing};
use zero_platform_tokio::contract::scope::{HostedScope, TaskScope};

pub(crate) struct ProcessLogging {
    scope: HostedScope,
    owner: ScopedTracing,
    // A launch/receiver error is retained independently of file/task sources.
    control_error: Option<io::Error>,
    reported: usize,
}
impl ProcessLogging {
    pub(crate) fn prepare(config: &zero_config::LogConfig) -> io::Result<Self> {
        let mut scope = TaskScope::suspended_hosted(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(config.files.len()),
        );
        // Every output has its original cold controller before registration
        // opens, and each drain's actual witness is installed before first IO.
        let owner = ScopedTracing::prepare(config, &scope.registrar())?;
        scope.enable_registration().map_err(io::Error::other)?;
        owner.install(config);
        Ok(Self {
            scope,
            owner,
            control_error: None,
            reported: 0,
        })
    }
    pub(crate) fn handle(&self) -> LoggingHandle {
        self.owner.budget_handle()
    }
    fn poll_originals(&mut self, cx: &mut std::task::Context<'_>) {
        if let Err(source) = self.owner.poll_workers(&mut self.scope, cx) {
            // Capacity is passive backpressure over retained originals. It is
            // not a new file/task fault or permission to evict admitted bytes.
            if source.kind() != io::ErrorKind::WouldBlock && self.control_error.is_none() {
                self.control_error = Some(source);
            }
        }
        for source in &self.owner.failures()[self.reported..] {
            // Avoid recursively enqueuing this same output's own diagnostic.
            eprintln!("logging cleanup: {source}");
        }
        self.reported = self.owner.failures().len();
    }
    pub(crate) async fn initialize(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| {
            self.poll_originals(cx);
            if self.owner.initialization_failed() {
                return Poll::Ready(Err(io::Error::other("failed to open log file")));
            }
            if self.owner.ready() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
        .await
    }
    /// All logger polling runs alongside this same pinned application future.
    pub(crate) async fn run<F: Future>(&mut self, work: F) -> F::Output {
        let mut work = std::pin::pin!(work);
        std::future::poll_fn(|cx| {
            self.poll_originals(cx);
            work.as_mut().poll(cx)
        })
        .await
    }
    /// Preserve the old nonblocking writer's bounded best-effort exit. Unknown
    /// originals remain owned by THIS value until process exit, never reported
    /// as joined/closed or handed to a detached cleanup job.
    pub(crate) async fn shutdown(&mut self) -> bool {
        self.owner.request_stop();
        let deadline = tokio::time::sleep(Duration::from_millis(1100));
        tokio::pin!(deadline);
        let mut sealed = false;
        std::future::poll_fn(|cx| {
            self.poll_originals(cx);
            if self.owner.drained() && self.owner.native_closed() && !sealed {
                self.scope.begin_shutdown();
                sealed = true;
            }
            if sealed && self.scope.is_drained() && self.owner.close_after_workers() {
                return Poll::Ready(true);
            }
            // Close may have produced an independent actual IO fault.
            self.poll_originals(cx);
            if deadline.as_mut().poll(cx).is_ready() {
                Poll::Ready(false)
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
