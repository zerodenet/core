//! Stream callers share the association's actual owner and original budget.
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
use zero_platform_tokio::controlled_carrier::{CarrierAdoption, CarrierIo, CarrierUserView};
pub(super) struct Attempt(CarrierAdoption, bool);
impl Attempt {
    pub(super) fn original(&self) -> &CarrierAdoption {
        &self.0
    }
    pub(super) fn publish(&mut self) {
        self.1 = false;
    }
}
mod acquire;
impl Drop for Attempt {
    fn drop(&mut self) {
        if self.1 {
            self.0.request_retirement();
        }
    }
}
impl Outbound {
    pub(super) async fn connect_owned(
        &self,
        request: Vec<u8>,
        context: OpenContext,
        session: Option<SessionId>,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let call = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let _caller = if session.is_some() {
            Some(context.upstream.packet_caller(&context.identity, &call)?)
        } else {
            None
        };
        let (io, view, mut attempt) = self
            .acquire_original(&context, session, session.is_none())
            .await?;
        let original = attempt.original().clone();
        let io: BoxStream = Box::pin(OwnedIo {
            io,
            view,
            original,
            cancelled: Box::pin(context.cancel.clone().cancelled_owned()),
        });
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let connected = self.handshake(io, request, &context, session).await?;
        attempt.publish();
        Ok(connected)
    }
}
struct OwnedIo {
    io: CarrierIo,
    view: CarrierUserView,
    original: CarrierAdoption,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl OwnedIo {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.original.request_retirement();
            return Err(io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
}
impl AsyncRead for OwnedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.io).poll_read(cx, bytes)
    }
}
impl AsyncWrite for OwnedIo {
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
impl Drop for OwnedIo {
    fn drop(&mut self) {
        // Field destruction returns the same actual IO/view. Request alone is
        // never native/users/caller/ACK closure or a refunded owner slot.
        self.original.request_retirement();
        debug_assert!(self.view.matches(&self.original));
    }
}
