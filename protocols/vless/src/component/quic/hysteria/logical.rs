//! Logical QUIC stream return, separate from the cached original's native close.
use super::*;
use std::{
    io,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{
    contract::{
        lifecycle::{BoxChannelControl, CloseReport, DynChannelControl, Unfinished},
        BoxFuture, CloseReason,
    },
    controlled_carrier::CarrierOrigin,
};
struct Returned {
    returned: AtomicBool,
    changed: tokio::sync::Notify,
    stop: AtomicBool,
    stopped: Arc<tokio::sync::Notify>,
}
struct Control(PacketMetadataArc<Returned>);
struct View {
    io: Option<zero_transport::quic::owned::OwnedBiStream>,
    stopping_read: Pin<Box<tokio::sync::futures::OwnedNotified>>,
    stopping_write: Pin<Box<tokio::sync::futures::OwnedNotified>>,
    // Cancellation waiters release their Notify references before this paid
    // state; its control may independently retain the same allocation.
    returned: PacketMetadataArc<Returned>,
}
pub(super) fn opened(
    generation: &entry::Generation,
    io: zero_transport::quic::owned::OwnedBiStream,
    original: CarrierOrigin,
) -> Result<crate::component::preconnect::Opened, ChannelError> {
    use zero_traits::packet_metadata::PacketMetadataLayout as L;
    let state = generation.memory(
        PacketMetadataArc::<Returned>::layout()
            .map_err(failure)?
            .checked_add(L::arc::<tokio::sync::Notify>().map_err(failure)?)
            .map_err(failure)?,
    )?;
    let control = generation.memory(L::boxed::<Control>())?;
    let view_memory = generation.memory(
        L::boxed::<PaidView>()
            .checked_add(
                L::boxed::<tokio::sync::futures::OwnedNotified>()
                    .repeated(2)
                    .map_err(failure)?,
            )
            .map_err(failure)?,
    )?;
    let returned = PacketMetadataArc::from_prepaid(
        Returned {
            returned: false.into(),
            changed: tokio::sync::Notify::new(),
            stop: false.into(),
            stopped: Arc::new(tokio::sync::Notify::new()),
        },
        state,
    )
    .map_err(|(source, _, _)| failure(source))?;
    let stopping_read = Box::pin(returned.stopped.clone().notified_owned());
    let stopping_write = Box::pin(returned.stopped.clone().notified_owned());
    let view = View {
        io: Some(io),
        returned: returned.clone(),
        stopping_read,
        stopping_write,
    };
    // The same allocated view holds its exact metadata credit through Drop.
    let io = TcpRelayStream::new(PaidView {
        view,
        _memory: view_memory,
    });
    let control = BoxChannelControl::from_prepaid(Control(returned), control)
        .map_err(|(source, _, _)| failure(source))?;
    Ok(crate::component::preconnect::Opened {
        io,
        control,
        original,
    })
}
struct PaidView {
    view: View,
    _memory: zero_traits::packet_metadata::PacketMetadataCredit,
}
impl View {
    fn return_io(&mut self) {
        if let Some(mut io) = self.io.take() {
            io.retire_logical();
            drop(io);
        }
        self.returned.returned.store(true, Ordering::Release);
        self.returned.changed.notify_waiters();
    }
    fn cancelled(&mut self, cx: &mut Context<'_>, read: bool) -> bool {
        let stopping = if read {
            self.stopping_read.as_mut()
        } else {
            self.stopping_write.as_mut()
        };
        if self.returned.stop.load(Ordering::Acquire)
            || std::future::Future::poll(stopping, cx).is_ready()
            || self.returned.stop.load(Ordering::Acquire)
        {
            self.return_io();
        }
        self.io.is_none()
    }
}
impl Drop for View {
    fn drop(&mut self) {
        self.return_io();
    }
}
impl AsyncRead for PaidView {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.view.cancelled(cx, true) {
            return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
        }
        Pin::new(self.view.io.as_mut().unwrap()).poll_read(cx, out)
    }
}
impl AsyncWrite for PaidView {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.view.cancelled(cx, false) {
            return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
        }
        Pin::new(self.view.io.as_mut().unwrap()).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.view.cancelled(cx, false) {
            return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
        }
        Pin::new(self.view.io.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.view.cancelled(cx, false) {
            return Poll::Ready(Ok(()));
        }
        Pin::new(self.view.io.as_mut().unwrap()).poll_shutdown(cx)
    }
}
impl zero_traits::AsyncSocket for PaidView {
    type Error = io::Error;
    async fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, b).await
    }
    async fn write_all(&mut self, b: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, b).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
impl zero_platform_tokio::ClientStream for PaidView {}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.0.stop.store(true, Ordering::Release);
        self.0.stopped.notify_waiters();
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0.stop.store(true, Ordering::Release);
            self.0.stopped.notify_waiters();
            let returned = async {
                loop {
                    let changed = self.0.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if self.0.returned.load(Ordering::Acquire) {
                        break;
                    }
                    changed.await;
                }
            };
            match tokio::time::timeout_at(end, returned).await {
                Ok(()) => Ok(CloseReport::default()),
                Err(_) => Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "Hysteria logical stream still held".into(),
                    )],
                    ..Default::default()
                }),
            }
        })
    }
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}
