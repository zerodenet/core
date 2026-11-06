//! Same-slot taskless stream; native and logical completion stay separate.
use super::*;
pub(super) struct Outbound {
    pub(super) inner: Arc<dyn DynStreamOutbound>,
    pub(super) endpoint: Destination,
    pub(super) dial: zero_traits::DialPolicy,
    pub(super) grants: CarrierGrants,
    pub(super) transfers: Transfers,
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
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
        let pending = self.transfers.lock().unwrap().clone();
        for transfer in pending {
            let published = transfer
                .published
                .load(std::sync::atomic::Ordering::Acquire);
            if !transfer.opening.load(std::sync::atomic::Ordering::Acquire)
                && (!published || transfer.stop.is_cancelled())
            {
                let report = match context.deadline {
                    Some(end) => transfer.close(end).await?,
                    None => transfer.try_close()?,
                };
                if !report.is_complete() && !published {
                    return Err(crate::component::instance::invalid(
                        Reason::Capacity,
                        "previous actual taskless carrier cleanup is unfinished",
                    ));
                }
            }
        }
        self.transfers
            .lock()
            .unwrap()
            .retain(|transfer| !transfer.finished());
        if let Some(end) = context.deadline {
            self.grants.reap_returned(end).await?;
        }
        let grant = self.grants.reserve()?;
        // Opted-in callers retain cleanup authority before first I/O. Other
        // existing callers retain the same slot; canonical cancellation is
        // driven by its actual host provider, never a detached cleanup task.
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(grant.adoption())?;
        }
        let stop = grant.cancellation();
        let adoption = grant.adoption();
        let transfer = Arc::new(Transfer {
            grant: Mutex::new(Some(grant)),
            adoption,
            owner: tokio::sync::Mutex::new(None),
            stop,
            published: false.into(),
            opening: true.into(),
        });
        // The persistent owning slot retains this original attempt before the
        // protocol's first await, including handshake error or future cancellation.
        self.transfers.lock().unwrap().push(transfer.clone());
        let _opening = Opening(transfer.clone());
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
