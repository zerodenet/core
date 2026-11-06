//! Same-slot taskless stream; native and logical completion stay separate.
use super::*;
pub(super) struct Outbound {
    pub(super) inner: Arc<dyn DynStreamOutbound>,
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
        // Fixed library users retain their own original inline responsibilities.
        self.inner.connect(request, context).await
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        mut context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        if context.session_identity()?.is_some() || context.upstream.binding().dial != self.dial {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "taskless carrier requires its actual Service binding",
            ));
        }
        self.reap(&context).await?;
        let transfer = self.reserve_transfer(None)?;
        let _opening = Opening(transfer.clone());
        // Persist the original before the waiting caller can reject/retire it.
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(transfer.adoption.clone())?;
        }
        context.upstream = UpstreamTransport::new(
            context.upstream.binding().clone(),
            Arc::new(Opener {
                upstream: context.upstream.clone(),
                endpoint: self.endpoint.clone(),
                transfer: transfer.clone(),
            }),
        );
        let caller_cancel = context.cancel.clone();
        let connected = self.inner.connect(request, context).await?;
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
        transfer
            .published
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(Connected {
            io: StreamLoan::new(
                Box::pin(super::caller::CallerIo::new(
                    connected.io,
                    caller_cancel,
                    transfer.adoption.clone(),
                )),
                Box::new(Control(transfer)),
                original,
            ),
            peer_confirmation: connected.peer_confirmation,
        })
    }
}
