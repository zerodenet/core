//! A real cold TCP opening is installed in LoanOwner before the first SYN poll.
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Instant,
};
use zero_platform_tokio::{
    contract::{
        context::{ConnectFact, ConnectObserver, OpenContext},
        lifecycle::{ChannelControl, CloseReport},
        ChannelError, CloseReason, Impact, Reason, Stage,
    },
    controlled_carrier::{loan::StreamLoan, CarrierOrigin},
};
use zero_stack::{
    tcp::client::{ClientTcpOpening, ClientTcpSocketControl},
    UserTcpStream,
};
type Opening =
    Pin<Box<dyn Future<Output = Result<UserTcpStream, zero_stack::ClientTcpStackError>> + Send>>;
pub(crate) struct Stream {
    connecting: Option<Opening>,
    stream: Option<UserTcpStream>,
    control: ClientTcpSocketControl,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    observer: Option<Arc<dyn ConnectObserver>>,
    session: Option<zero_core::component::SessionId>,
}
impl Stream {
    pub(super) fn new(opening: ClientTcpOpening, context: OpenContext) -> Self {
        Self {
            control: opening.control(),
            connecting: Some(Box::pin(opening.connect())),
            stream: None,
            cancelled: Box::pin(context.cancel.cancelled_owned()),
            deadline: context
                .deadline
                .map(|end| Box::pin(tokio::time::sleep_until(end))),
            observer: context.observer,
            session: context.identity.session(),
        }
    }
    pub(crate) fn into_loan(self, original: CarrierOrigin) -> StreamLoan {
        let control = Control(self.control.clone());
        StreamLoan::new(
            Box::pin(self),
            zero_platform_tokio::contract::lifecycle::erase_control(control),
            original,
        )
    }
    fn prepare(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.control.is_stopped() || self.cancelled.as_mut().poll(cx).is_ready() {
            self.control.cancel();
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if self
            .deadline
            .as_mut()
            .is_some_and(|deadline| deadline.as_mut().poll(cx).is_ready())
        {
            self.control.cancel();
            return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
        }
        if let Some(connecting) = self.connecting.as_mut() {
            match std::task::ready!(connecting.as_mut().poll(cx)) {
                Ok(stream) => {
                    self.stream = Some(stream);
                    self.connecting = None;
                    self.deadline = None;
                    if let Some(observer) = &self.observer {
                        observer.record(self.session, ConnectFact::CarrierReady);
                    }
                }
                Err(source) => {
                    self.connecting = None;
                    self.control.cancel();
                    return Poll::Ready(Err(io::Error::other(source)));
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(this.prepare(cx))?;
        Pin::new(this.stream.as_mut().expect("actual original TCP connected")).poll_read(cx, bytes)
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        std::task::ready!(this.prepare(cx))?;
        Pin::new(this.stream.as_mut().expect("actual original TCP connected")).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(this.prepare(cx))?;
        Pin::new(this.stream.as_mut().expect("actual original TCP connected")).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.connecting.is_some() {
            this.control.cancel();
            return Poll::Ready(Ok(()));
        }
        match this.stream.as_mut() {
            Some(stream) => Pin::new(stream).poll_shutdown(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.control.cancel();
    }
}
struct Control(ClientTcpSocketControl);
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        tokio::time::timeout_at(deadline, self.0.close())
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Session)
                    .with_source(source)
            })?;
        Ok(CloseReport::default()) // matching logical TCP only; original Scope/native remain separate
    }
}
