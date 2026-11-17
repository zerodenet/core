//! Stream views retain their actual worker until the unique IO is returned.
use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
struct View {
    io: BoxStream,
    _lease: ownership::Lease,
    cancel: zero_platform_tokio::contract::BoxFuture<'static, ()>,
}
impl AsyncRead for View {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.cancel.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.io.as_mut().poll_read(cx, bytes)
    }
}
impl AsyncWrite for View {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.cancel.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.io.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.io.as_mut().poll_shutdown(cx)
    }
}
impl Outbound {
    async fn stream(
        &self,
        request: ConnectRequest,
        context: &OpenContext,
        borrow: bool,
    ) -> Result<(BoxStream, Arc<Worker>), ChannelError> {
        let portal = self.portal(context)?;
        if request.destination.port == 0 {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        // Check the genuine original before reserving/sending a business SID.
        // Normal Stream uses the same actual worker for every carrier style.
        let (worker, sid) = portal
            .reserve_worker()
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
        if borrow && (worker.origin.is_none() || worker.custody.driver.lock().unwrap().is_none()) {
            worker.connection.release_stream(sid);
            return Err(error(Stage::Prepare, Reason::Unsupported));
        }
        let usage = portal.lease(&worker).inspect_err(|_| {
            worker.connection.release_stream(sid);
        })?;
        let opening = crate::mux_pool::native::reverse_stream(
            worker.connection.clone(),
            sid,
            request.destination,
            Self::origin(context, crate::mux::NETWORK_TCP),
        );
        let io=tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(error(Stage::Handshake,Reason::Revoked)),
            _=context.cancel.cancelled()=>return Err(error(Stage::Handshake,Reason::Cancelled)),
            result=zero_platform_tokio::contract::context::with_deadline(context.deadline,opening)=>result.map_err(|source|error(Stage::Handshake,Reason::Timeout).with_source(source))?,
        }.map_err(|source|error(Stage::Handshake,Reason::Io).with_source(source))?;
        let admission = self.admission.clone();
        let cancel = context.cancel.clone();
        let io = Box::pin(View {
            io,
            _lease: usage,
            cancel: Box::pin(async move {
                tokio::select! {_=admission.cancelled()=>{},_=cancel.cancelled()=>{}}
            }),
        });
        if let Some(original) = &worker.origin {
            context.upstream.observe_carrier_origin(original);
        }
        if let Some(observer) = &context.observer {
            observer.record(
                context.session_identity()?,
                zero_platform_tokio::contract::context::ConnectFact::CarrierReady,
            );
        }
        Ok((io, worker))
    }
}
impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let (io, _) = self.stream(request, &context, false).await?;
        Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<zero_platform_tokio::controlled_carrier::loan::StreamLoan>, ChannelError>
    {
        let (io, worker) = self.stream(request, &context, true).await?;
        let original = worker
            .origin
            .clone()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let driver = worker
            .custody
            .driver
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let loan = zero_transport::pooled_driver::PooledStream {
            io,
            origin: original,
        }
        .into_stream_loan_from_accepted_driver(driver)?;
        Ok(Connected {
            io: loan,
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
