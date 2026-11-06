//! Admitted Business uses its real caller and one original, never a Service loan.
use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::controlled_carrier::{CarrierIo, CarrierUserView};
impl Outbound {
    pub(super) async fn connect_business(
        &self,
        request: ConnectRequest,
        mut context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        if context.upstream.binding().dial != self.dial {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "taskless Business dial differs from its actual owner",
            ));
        }
        let call = CarrierContext {
            session: context.session_identity()?,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        // This read-only observation does not mint native permission. The
        // original Node opener independently issues and validates its actual
        // Business ticket for this same grant, facts and live caller.
        let caller = context.upstream.packet_caller(&context.identity, &call)?;
        self.reap(&context).await?;
        let transfer = self.reserve_transfer(Some(caller))?;
        let _opening = Opening(transfer.clone());
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(transfer.adoption.clone())?;
        }
        context.upstream = UpstreamTransport::new(
            context.upstream.binding().clone(),
            Arc::new(Opener {
                upstream: context.upstream.clone(),
                endpoint: self.endpoint.clone(),
                process: self.process,
                transfer: transfer.clone(),
            }),
        );
        let caller_cancel = context.cancel.clone();
        let connected = self.inner.connect(request, context).await?;
        if transfer.grant.lock().unwrap().is_some() {
            return Err(crate::component::instance::invalid(
                Reason::InvalidConfig,
                "Business protocol did not consume its actual original grant",
            ));
        }
        transfer
            .published
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(Connected {
            io: Box::pin(super::caller::CallerIo::new(
                connected.io,
                caller_cancel,
                transfer.adoption.clone(),
            )),
            peer_confirmation: connected.peer_confirmation,
        })
    }
}
pub(super) struct BusinessIo {
    io: CarrierIo,
    _view: CarrierUserView,
}
impl BusinessIo {
    pub(super) fn new(io: CarrierIo, view: CarrierUserView) -> Self {
        Self { io, _view: view }
    }
}
impl AsyncRead for BusinessIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for BusinessIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
