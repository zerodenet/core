use super::ReplaySaltPool;
use std::sync::Arc;
use tokio::{sync::oneshot, task::JoinHandle};

pub(crate) struct LegacyWorker {
    pub(crate) task: JoinHandle<()>,
    pub(crate) stop: oneshot::Sender<()>,
}
#[derive(Default)]
pub(super) struct Maintenance {
    pub(super) managed: bool,
    leased: bool,
    pub(super) legacy: Option<LegacyWorker>,
}

// Once registered, only destruction of the actual worker future releases the
// lease. A protocol cleanup hook can request cancellation but cannot prove exit.
struct RegisteredLease(Arc<ReplaySaltPool>);
impl Drop for RegisteredLease {
    fn drop(&mut self) {
        self.0.release_maintenance();
    }
}

impl ReplaySaltPool {
    pub(crate) fn claim_maintenance(&self) -> Result<Option<JoinHandle<()>>, zero_core::Error> {
        let mut state = self.maintenance.lock().unwrap();
        if state.leased {
            return Err(zero_core::Error::Protocol(
                "ss: replay maintenance already has an owner",
            ));
        }
        state.managed = true;
        state.leased = true;
        Ok(state.legacy.take().map(|worker| {
            let _ = worker.stop.send(());
            worker.task
        }))
    }
    pub(crate) fn release_maintenance(&self) {
        self.maintenance.lock().unwrap().leased = false;
        // Managed mode stays sticky: a stale framing handle cannot start an
        // unregistered replacement beside the host's retired task scope.
    }
    pub(crate) fn registered_maintenance(
        self: &Arc<Self>,
        component: zero_core::component::ComponentId,
        parent: zero_platform_tokio::contract::CancellationToken,
        scope: zero_platform_tokio::contract::CancellationToken,
    ) -> impl std::future::Future<Output = Result<(), zero_platform_tokio::contract::ChannelError>>
           + Send
           + 'static {
        use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
        let weak = std::sync::Arc::downgrade(&self.inner);
        let ttl = self.ttl;
        let lease = RegisteredLease(self.clone());
        async move {
            let _lease = lease;
            loop {
                tokio::select! { biased;
                    _ = parent.cancelled() => return Ok(()),
                    _ = scope.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {},
                }
                let Some(inner) = weak.upgrade() else {
                    return Ok(());
                };
                let mut entries = inner.lock().map_err(|_| {
                    let mut error =
                        ChannelError::new(Stage::Receive, Reason::Internal, Impact::Component)
                            .with_source(std::io::Error::other("ss: 2022 salt pool poisoned"));
                    error.component = Some(component);
                    error
                })?;
                entries.prune(ttl);
            }
        }
    }
}
