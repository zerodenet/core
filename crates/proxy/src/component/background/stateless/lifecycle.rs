//! Same-slot taskless lifecycle; native and logical completion stay separate.
use super::*;
pub(super) struct Owner {
    pub(super) inner: Box<dyn DynResourceLifecycle>,
    pub(super) carrier: CarrierOwner,
    pub(super) resources: Arc<dyn ResourceAccess>,
    pub(super) tasks: TaskRegistrar,
    pub(super) ordinal: usize,
    pub(super) transfers: Transfers,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: Instant) -> Result<(), BuildError> {
        self.resources.register_controlled_carrier(
            self.ordinal,
            self.carrier.registration(),
            &self.tasks,
        )?;
        self.carrier.start(end).await?;
        self.inner.start(end).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
        self.carrier.revoke();
        for transfer in self.transfers.lock().unwrap().iter() {
            transfer.stop.cancel();
        }
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let transfers = self.transfers.lock().unwrap().clone();
        let mut report = self.inner.shutdown(end).await?;
        for transfer in transfers {
            report.append(transfer.close(end).await?);
        }
        self.transfers
            .lock()
            .unwrap()
            .retain(|transfer| !transfer.finished());
        report.append(self.carrier.shutdown(end).await?);
        Ok(report)
    }
}
