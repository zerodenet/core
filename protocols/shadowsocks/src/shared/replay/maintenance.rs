use super::ReplaySaltPool;
use std::sync::Arc;
use tokio::{sync::oneshot, task::JoinHandle};
use zero_core::component::ComponentInstanceId;

pub(crate) struct LegacyWorker {
    pub(crate) task: JoinHandle<()>,
    pub(crate) stop: oneshot::Sender<()>,
}
#[derive(Default)]
pub(super) struct Maintenance {
    pub(super) managed: bool,
    leased: bool,
    claimed_by: Option<ComponentInstanceId>,
    ready: bool,
    pub(super) legacy: Option<LegacyWorker>,
}

// Once registered, only destruction of the actual worker future releases the
// lease. A protocol cleanup hook can request cancellation but cannot prove exit.
struct RegisteredLease {
    pool: Arc<ReplaySaltPool>,
    claimed_by: Option<ComponentInstanceId>,
}
impl Drop for RegisteredLease {
    fn drop(&mut self) {
        self.pool.release_maintenance(self.claimed_by);
    }
}

impl ReplaySaltPool {
    pub(crate) fn claim_maintenance(
        &self,
        owner: Option<ComponentInstanceId>,
    ) -> Result<Option<JoinHandle<()>>, zero_core::Error> {
        let mut state = self.maintenance.lock().unwrap();
        if state.leased {
            return Err(zero_core::Error::Protocol(
                "ss: replay maintenance already has an owner",
            ));
        }
        state.managed = true;
        state.leased = true;
        state.claimed_by = owner;
        state.ready = false;
        Ok(state.legacy.take().map(|worker| {
            let _ = worker.stop.send(());
            worker.task
        }))
    }
    pub(crate) fn release_maintenance(&self, owner: Option<ComponentInstanceId>) {
        let mut state = self.maintenance.lock().unwrap();
        if state.claimed_by != owner {
            return;
        }
        state.leased = false;
        state.ready = false;
        state.claimed_by = None;
        // Managed mode stays sticky: a stale framing handle cannot start an
        // unregistered replacement beside the host's retired task scope.
    }
    // A claim is not readiness: legacy join and real task registration must
    // both finish before exposing this exact generation to native consumers.
    pub(crate) fn bind_registered_owner(
        &self,
        owner: ComponentInstanceId,
    ) -> Result<(), zero_core::Error> {
        let mut state = self.maintenance.lock().unwrap();
        if !state.leased || state.claimed_by != Some(owner) {
            return Err(zero_core::Error::Protocol(
                "ss: replay driver claim is no longer current",
            ));
        }
        state.ready = true;
        Ok(())
    }
    pub(crate) fn revoke_registered_owner(&self, owner: ComponentInstanceId) {
        let mut state = self.maintenance.lock().unwrap();
        if state.claimed_by == Some(owner) {
            state.ready = false;
        }
    }
    pub(crate) fn registered_owner_is_ready(&self, owner: ComponentInstanceId) -> bool {
        let state = self.maintenance.lock().unwrap();
        state.managed && state.leased && state.ready && state.claimed_by == Some(owner)
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
        let claimed_by = self.maintenance.lock().unwrap().claimed_by;
        let lease = RegisteredLease {
            pool: self.clone(),
            claimed_by,
        };
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
