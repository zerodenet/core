//! Same-slot taskless transfer; native and logical completion stay separate.
use super::*;
pub(super) struct Transfer {
    pub(super) grant: Mutex<Option<CarrierGrant>>,
    pub(super) owner: tokio::sync::Mutex<Option<LoanOwner>>,
    pub(super) stop: CancellationToken,
    pub(super) adoption: CarrierAdoption,
    pub(super) published: std::sync::atomic::AtomicBool,
    pub(super) opening: std::sync::atomic::AtomicBool,
}
pub(super) type Transfers = Arc<Mutex<Vec<Arc<Transfer>>>>;
impl Transfer {
    pub(super) async fn close(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        let mut guard = tokio::time::timeout_at(end, self.owner.lock())
            .await
            .map_err(|source| {
                crate::component::instance::invalid(
                    Reason::Timeout,
                    "taskless original logical control is busy",
                )
                .with_source(source)
            })?;
        let Some(owner) = guard.as_mut() else {
            return self.adoption.retire(end).await;
        };
        let report = owner.close(end).await?;
        if report.is_complete() {
            *guard = None;
        }
        Ok(report)
    }
    pub(super) fn try_close(&self) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        let Ok(mut guard) = self.owner.try_lock() else {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "taskless original logical control is busy".into(),
                    ),
                ],
                ..Default::default()
            });
        };
        let report = match guard.as_mut() {
            Some(owner) => owner.try_close()?,
            None => self.adoption.try_retire()?,
        };
        if report.is_complete() {
            *guard = None;
        }
        Ok(report)
    }
    pub(super) fn finished(&self) -> bool {
        !self.opening.load(std::sync::atomic::Ordering::Acquire)
            && self.owner.try_lock().is_ok_and(|owner| owner.is_none())
            && self
                .adoption
                .closed()
                .is_some_and(|closed| closed.matches(&self.adoption))
            && self
                .adoption
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.adoption))
    }
}
pub(super) struct Opening(pub(super) Arc<Transfer>);
impl Drop for Opening {
    fn drop(&mut self) {
        self.0
            .opening
            .store(false, std::sync::atomic::Ordering::Release);
        if !self.0.published.load(std::sync::atomic::Ordering::Acquire) {
            self.0.stop.cancel();
            self.0.adoption.request_retirement();
        }
    }
}
pub(super) struct Control(pub(super) Arc<Transfer>);
impl DynChannelControl for Control {
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.0.try_close()
    }
    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
        self.0.adoption.request_retirement();
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.0.close(end))
    }
}
