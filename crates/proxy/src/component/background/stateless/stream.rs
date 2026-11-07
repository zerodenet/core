//! Same-slot taskless stream; native and logical completion stay separate.
use super::*;
#[derive(Clone)]
pub(super) struct Outbound {
    pub(super) inner: Arc<dyn DynStreamOutbound>,
    // None is the original explicit library contract, which prepares inline.
    // Every ordinary owning Factory installs its real Scope registrar above.
    pub(super) tasks: Option<TaskRegistrar>,
    pub(super) cleanup_network: Option<Arc<super::super::BackgroundNetwork>>,
    pub(super) process: Option<(
        zero_core::component::ComponentInstanceId,
        std::time::Duration,
    )>,
    pub(super) endpoint: Destination,
    pub(super) dial: zero_traits::DialPolicy,
    pub(super) grants: CarrierGrants,
    pub(super) transfers: Transfers,
    pub(super) limit: NonZeroUsize,
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        if context.session_identity()?.is_some() && context.upstream.has_tracked_business_caller() {
            return self.connect_business(request, context).await;
        }
        if context.session_identity()?.is_none() {
            let connected = self.connect_carrier(request, context).await?;
            return Ok(Connected {
                io: super::service::into_stream(connected.io),
                peer_confirmation: connected.peer_confirmation,
            });
        }
        // An explicit untracked Business library invocation retains its
        // original responsibility. Every real Service uses this same owner,
        // including callers whose custody is finalized inline by HostResources.
        self.inner.connect(request, context).await
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        if context.session_identity()?.is_some() || context.upstream.binding().dial != self.dial {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "carrier requires its actual Service binding",
            ));
        }
        loop {
            self.reap(&context).await?;
            let transfer = self.reserve_transfer(None)?;
            let opening = Opening(transfer.clone());
            if let Some(waiting) = context.upstream.observed_pending_carrier() {
                waiting.retain(transfer.adoption.clone())?;
            }
            let actual = self.clone();
            let input = context.clone();
            let original = transfer.clone();
            let inner = self.inner.clone();
            let next_request = request.clone();
            let connected = match self
                .prepare(&transfer, &context, move |caller| async move {
                    let attempt = actual.context_for_attempt(&input, &original, caller)?;
                    inner.connect(next_request, attempt).await
                })
                .await
            {
                Ok(connected) => connected,
                Err(source) => {
                    drop(opening);
                    if self.retry_original(&context, &transfer, &source).await? {
                        continue;
                    }
                    return Err(source);
                }
            };
            let original = transfer
                .owner
                .lock()
                .await
                .as_ref()
                .ok_or_else(|| {
                    crate::component::instance::invalid(
                        Reason::InvalidConfig,
                        "protocol did not consume its unique actual carrier",
                    )
                })?
                .original()
                .clone();
            if let Some(preparation) = &transfer.preparation {
                preparation.publish_transferred()?;
            }
            transfer
                .published
                .store(true, std::sync::atomic::Ordering::Release);
            return Ok(Connected {
                io: StreamLoan::new(
                    Box::pin(super::caller::CallerIo::new(
                        connected.io,
                        context.cancel.clone(),
                        transfer.clone(),
                    )),
                    Box::new(Control(transfer)),
                    original,
                ),
                peer_confirmation: connected.peer_confirmation,
            });
        }
    }
}
