//! One exclusive warmed transport, held by its actual original driver.
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{contract::component::PacketDriverMetadataView, TcpRelayStream};
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};

struct Shared {
    io: Mutex<Option<TcpRelayStream>>,
    returned: tokio::sync::Notify,
    taken: std::sync::atomic::AtomicBool,
    _credit: PacketMetadataCredit,
}
struct View(Arc<Shared>);
struct DriverOwner(Arc<Shared>);
impl Drop for DriverOwner {
    fn drop(&mut self) {
        // Driver cancellation revokes the real raw view even if a historical
        // protocol view remains alive. This is not a native-close receipt.
        let io = self.0.io.lock().unwrap().take();
        drop(io);
    }
}
impl Drop for View {
    fn drop(&mut self) {
        self.0
            .taken
            .store(true, std::sync::atomic::Ordering::Release);
        self.0.returned.notify_waiters();
    }
}
pub(super) fn prepare(
    io: TcpRelayStream,
    memory: &PacketDriverMetadataView,
) -> Result<(TcpRelayStream, zero_transport::pooled_driver::Driver), zero_transport::RuntimeError> {
    let layout = PacketMetadataLayout::arc::<Shared>()
        .and_then(|layout| layout.checked_add(PacketMetadataLayout::boxed::<View>()))
        .map_err(std::io::Error::other)?;
    let credit = memory.reserve(layout).map_err(std::io::Error::other)?;
    let bypass = zero_traits::AsyncSocket::transport_bypass_control(&io);
    let settings = io.application_settings().cloned();
    let shared = Arc::new(Shared {
        io: Mutex::new(Some(io)),
        returned: Default::default(),
        taken: false.into(),
        _credit: credit,
    });
    // Installed before the future can be dropped or registered unsuccessfully.
    let owner = DriverOwner(shared.clone());
    let view = View(shared.clone());
    let driver = Box::pin(async move {
        let _owner = owner;
        loop {
            let returned = shared.returned.notified();
            tokio::pin!(returned);
            returned.as_mut().enable();
            if shared.taken.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            returned.await;
        }
        let _ = std::future::poll_fn(|cx| {
            let mut raw = shared.io.lock().unwrap();
            match raw.as_mut() {
                Some(raw) => Pin::new(raw).poll_shutdown(cx),
                None => Poll::Ready(Ok(())),
            }
        })
        .await;
        // The retained platform controller separately confirms native closure.
    });
    let view = match bypass {
        Some(control) => TcpRelayStream::with_transport_bypass_control(view, control),
        None => TcpRelayStream::new(view),
    };
    Ok((view.with_application_settings(settings), driver))
}
impl AsyncRead for View {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.0.io.lock().unwrap().as_mut() {
            Some(io) => Pin::new(io).poll_read(cx, bytes),
            None => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
        }
    }
}
impl AsyncWrite for View {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.0.io.lock().unwrap().as_mut() {
            Some(io) => Pin::new(io).poll_write(cx, bytes),
            None => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.0.io.lock().unwrap().as_mut() {
            Some(io) => Pin::new(io).poll_flush(cx),
            None => Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.0.io.lock().unwrap().as_mut() {
            Some(io) => Pin::new(io).poll_shutdown(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}
