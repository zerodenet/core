//! Driver registration precedes native acquisition; cancelled starts keep both.
use super::*;
use tokio::{
    sync::oneshot,
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskRegistrar},
    CloseReason,
};
pub(super) struct Owner {
    pub shared: Arc<Shared>,
    pub resources: Arc<dyn ResourceAccess>,
    pub tasks: TaskRegistrar,
    pub name: Arc<str>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if self.shared.metadata.driver.get().is_some() {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let (boot, ready) = oneshot::channel();
        let shared = self.shared.clone();
        let (_, witness) = self
            .tasks
            .spawn_tracked(TaskClass::Critical, driver::run(shared, boot))
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        self.shared
            .metadata
            .driver
            .set(witness.clone())
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        // The notification records the real pending unique-IO input poll. It
        // is never task/native-close evidence; the same witness remains owned.
        tokio::select! {
            _ = self.shared.cancel.cancelled() => return Err(error(Stage::Prepare, Reason::Revoked)),
            result = timeout_at(deadline, ready) => result.map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?.map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?,
        }
        let grant = self
            .resources
            .open_packet_device(self.name.clone(), witness)?;
        let acquired = Acquired {
            interface: grant.interface.clone(),
            identity: grant.identity.clone(),
            original: grant.original.clone(),
        };
        self.shared
            .metadata
            .acquired
            .set(acquired)
            .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
        // Persist the unique view in the original source owner before waking
        // the driver. A lost/cancelled waiter cannot orphan acquired native IO.
        self.shared.state.lock().unwrap().io = Some(grant);
        self.shared.metadata.changed.notify_waiters();
        self.shared.changed.notify_waiters();
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.shared.cancel.cancel();
        self.shared.changed.notify_waiters();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut failure = None;
        if let Some(driver) = self.shared.metadata.driver() {
            let joined = timeout_at(deadline, driver.wait())
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
            // Preserve the actual source error independently of native close.
            if let Err(source) = joined.result() {
                failure = Some(source);
            }
        }
        let unused = { self.shared.state.lock().unwrap().io.take() };
        drop(unused);
        self.shared.incoming.lock().unwrap().take();
        self.shared.outgoing.lock().unwrap().take();
        self.shared.pending_tx.lock().unwrap().take();
        match failure {
            Some(source) => Err(crate::component::instance::retained(source)),
            None => Ok(CloseReport::default()),
        }
    }
}
