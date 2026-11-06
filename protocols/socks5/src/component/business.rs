//! TCP Business shares the association's one original owner and native budget.
use super::*;
use alloc::{boxed::Box, vec::Vec};
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::io;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_core::component::SessionId;
use zero_platform_tokio::{
    contract::CancellationToken,
    controlled_carrier::{CarrierAdoption, CarrierIo, CarrierUserView},
};
struct Attempt(CarrierAdoption, bool);
impl Drop for Attempt {
    fn drop(&mut self) {
        if self.1 {
            self.0.request_retirement();
        }
    }
}
impl Outbound {
    pub(super) async fn connect_business(
        &self,
        request: Vec<u8>,
        context: OpenContext,
        session: Option<SessionId>,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let carriers = self
            .carriers
            .as_ref()
            .expect("selected actual original association owner");
        let call = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        // Read-only real caller observation; acquire below still independently
        // requires its original host-issued Business ticket and native binding.
        let _caller = context.upstream.packet_caller(&context.identity, &call)?;
        if let Some(end) = context.deadline {
            carriers.reap_returned(end).await?;
        }
        let mut grant = carriers.reserve()?;
        let view = grant.claim_user_view()?;
        let original = grant.adoption();
        let mut attempt = Attempt(original.clone(), true);
        // The same CarrierOwner already retains the original and actual view
        // before callbacks, native acquisition or protocol authentication await.
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(original.clone())?;
        }
        let acquiring =
            context
                .upstream
                .acquire_controlled_stream(self.options.endpoint.clone(), call, grant);
        let io=tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Dial,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,acquiring)=>result.map_err(|source|self.admission.error(session,Stage::Dial,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|error|self.admission.attach(error,session))?;
        let io: BoxStream = Box::pin(BusinessIo {
            io,
            view,
            original,
            cancelled: Box::pin(context.cancel.clone().cancelled_owned()),
        });
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let connected = self.handshake(io, request, &context, session).await?;
        attempt.1 = false;
        Ok(connected)
    }
}
struct BusinessIo {
    io: CarrierIo,
    view: CarrierUserView,
    original: CarrierAdoption,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl BusinessIo {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.original.request_retirement();
            return Err(io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
}
impl AsyncRead for BusinessIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.io).poll_read(cx, bytes)
    }
}
impl AsyncWrite for BusinessIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
impl Drop for BusinessIo {
    fn drop(&mut self) {
        // Field destruction returns the same actual IO/view. Request alone is
        // never native/users/caller/ACK closure or a refunded owner slot.
        self.original.request_retirement();
        debug_assert!(self.view.matches(&self.original));
    }
}
