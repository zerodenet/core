//! Same-slot taskless transfer; native and logical completion stay separate.
use super::*;
pub(super) struct Transfer {
    pub(super) driver_input:
        Mutex<Option<zero_platform_tokio::controlled_carrier::CarrierDriver<()>>>,
    pub(super) grant: Mutex<Option<CarrierGrant>>,
    pub(super) view: Mutex<Option<zero_platform_tokio::controlled_carrier::CarrierUserView>>,
    pub(super) caller: Option<zero_platform_tokio::contract::scope::TaskJoinWitness>,
    pub(super) preparation: Option<zero_platform_tokio::controlled_carrier::CarrierPreparation>,
    pub(super) pending: Mutex<Vec<CarrierAdoption>>,
    pub(super) preparing: Mutex<Option<UpstreamTransport>>,
    pub(super) cleanup: Mutex<super::datagram::cleanup::Cleanup>,
    pub(super) cleanup_network: Option<Arc<super::super::BackgroundNetwork>>,
    pub(super) retained_prefix: std::sync::OnceLock<Arc<[zero_core::component::RoleInstanceId]>>,
    pub(super) owner: tokio::sync::Mutex<Option<LoanOwner>>,
    pub(super) stop: CancellationToken,
    pub(super) adoption: CarrierAdoption,
    pub(super) published: std::sync::atomic::AtomicBool,
    pub(super) opening: std::sync::atomic::AtomicBool,
}
pub(super) type Transfers = Arc<Mutex<Vec<Arc<Transfer>>>>;
impl Transfer {
    pub(super) async fn close_original(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        self.request_pending_retirement();
        let mut guard = tokio::time::timeout_at(end, self.owner.lock())
            .await
            .map_err(|source| {
                crate::component::instance::invalid(
                    Reason::Timeout,
                    "taskless original logical control is busy",
                )
                .with_source(source)
            })?;
        let result = match guard.as_mut() {
            Some(owner) => owner.close(end).await,
            None => {
                let unused = self.grant.lock().unwrap().take();
                let view = self.view.lock().unwrap().take();
                drop(unused);
                drop(view);
                self.adoption.retire(end).await
            }
        };
        if result.as_ref().is_ok_and(CloseReport::is_complete) {
            *guard = None;
        }
        drop(guard);
        result
    }
    pub(super) fn try_close_original(&self) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        self.request_pending_retirement();
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
            None => {
                let unused = self.grant.lock().unwrap().take();
                let view = self.view.lock().unwrap().take();
                drop(unused);
                drop(view);
                self.adoption.try_retire()?
            }
        };
        if report.is_complete() {
            *guard = None;
        }
        drop(guard);
        Ok(report)
    }
    pub(super) fn finished(&self) -> bool {
        self.release_pending();
        self.pending.lock().unwrap().is_empty()
            && !self.opening.load(std::sync::atomic::Ordering::Acquire)
            && self.owner.try_lock().is_ok_and(|owner| owner.is_none())
            && self
                .adoption
                .closed()
                .is_some_and(|closed| closed.matches(&self.adoption))
            && self.adoption.users_closed().is_some_and(|closed| {
                closed.matches(&self.adoption)
                    && self.preparation.as_ref().is_none_or(|preparation| {
                        preparation.is_published()
                            || preparation.matches_unpublished_closed(&closed)
                    })
                    && self.caller.as_ref().is_none_or(|caller| {
                        if self
                            .preparation
                            .as_ref()
                            .is_some_and(|preparation| !preparation.is_published())
                        {
                            return true;
                        }
                        // Positive unused-reservation receipts permit a retry
                        // in the still-live caller. No actual I/O/custody bound
                        // a caller here; native acquisition still needs its ACK.
                        (!closed.had_native_acquisition()
                            && self.adoption.closed().is_some_and(|native| {
                                !native.had_native_acquisition()
                                    && native.borrowed_origin().is_none()
                            }))
                            || (closed.matches_caller(caller)
                                && caller
                                    .try_joined()
                                    .is_some_and(|joined| joined.registration_released()))
                    })
            })
    }
}
pub(super) struct Opening(pub(super) Arc<Transfer>);
impl Drop for Opening {
    fn drop(&mut self) {
        self.0
            .opening
            .store(false, std::sync::atomic::Ordering::Release);
        if !self.0.published.load(std::sync::atomic::Ordering::Acquire)
            && self.0.adoption.retained_preparation_job().is_none()
        {
            self.0.stop.cancel();
            self.0.request_pending_retirement();
            // If no opener consumed the unique grant, return its actual view
            // synchronously. Holding it until another dial would prevent the
            // original Host provider from finishing this unused cancellation.
            let unused = self.0.grant.lock().unwrap().take();
            if unused.is_some() {
                let view = self.0.view.lock().unwrap().take();
                drop(unused);
                drop(view);
            }
            self.0.adoption.request_retirement();
            let metadata = self.0.preparing.lock().unwrap().take();
            drop(metadata);
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
        self.0.request_pending_retirement();
        self.0.adoption.request_retirement();
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.0.close(end))
    }
}
