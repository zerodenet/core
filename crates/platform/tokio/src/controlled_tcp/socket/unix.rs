//! One original nonblocking descriptor, retained outside registration futures.
use std::{
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    os::fd::{AsRawFd, RawFd},
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{io::unix::AsyncFd, io::ReadBuf};
use zero_traits::IoObserver;
struct Original(Mutex<Option<TcpStream>>);
struct Registration(Arc<Original>);
impl AsRawFd for Registration {
    fn as_raw_fd(&self) -> RawFd {
        self.0
             .0
            .lock()
            .expect("TCP descriptor owner poisoned")
            .as_ref()
            .expect("registered descriptor")
            .as_raw_fd()
    }
}
pub(in crate::controlled_tcp) struct NativeSocket {
    original: Arc<Original>,
    registration: Option<AsyncFd<Registration>>,
    pub(in crate::controlled_tcp) interface: Option<crate::EgressInterface>,
    observer: Option<Arc<dyn IoObserver>>,
}
impl NativeSocket {
    pub(in crate::controlled_tcp) fn new(
        socket: TcpStream,
        interface: Option<crate::EgressInterface>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Self {
        if let Some(observer) = &observer {
            observer.stream_boundary();
        }
        Self {
            original: Arc::new(Original(Mutex::new(Some(socket)))),
            registration: None,
            interface,
            observer,
        }
    }
    #[cfg(all(test, target_os = "linux"))]
    pub(in crate::controlled_tcp) fn original_fd(&self) -> RawFd {
        self.original
            .0
            .lock()
            .expect("TCP descriptor owner poisoned")
            .as_ref()
            .expect("owned descriptor")
            .as_raw_fd()
    }
    pub(in crate::controlled_tcp) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.original
            .0
            .lock()
            .expect("TCP descriptor owner poisoned")
            .as_ref()
            .expect("owned descriptor")
            .local_addr()
    }
    pub(in crate::controlled_tcp) fn observed_local_addr(&self) -> Option<SocketAddr> {
        self.local_addr().ok()
    }
    pub(in crate::controlled_tcp) fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.original
            .0
            .lock()
            .expect("TCP descriptor owner poisoned")
            .as_ref()
            .expect("owned descriptor")
            .peer_addr()
    }
    pub(in crate::controlled_tcp) fn begin_connect(
        &mut self,
        peer: SocketAddr,
    ) -> Result<(), (&'static str, io::Error)> {
        {
            let guard = self
                .original
                .0
                .lock()
                .expect("TCP descriptor owner poisoned");
            let socket = socket2::SockRef::from(guard.as_ref().expect("owned descriptor"));
            if let Err(source) = socket.connect(&peer.into()) {
                if source.raw_os_error() != Some(libc::EINPROGRESS) {
                    return Err(("connect_socket", source));
                }
            }
        }
        // Register only after connect started: unconnected HUP would otherwise
        // create permanently sticky WRITE_CLOSED readiness. Registration owns
        // only an Arc to the same original fd, never a duplicated descriptor.
        // SAFETY: Registration returns the same live fd for its whole life.
        // The one original std socket stays in Original; Shared serializes all
        // operations and close deregisters AsyncFd before taking that socket.
        match unsafe { AsyncFd::register(Registration(self.original.clone())) } {
            Ok(registration) => {
                self.registration = Some(registration);
                Ok(())
            }
            Err(source) => {
                let (_registration, source) = source.into_parts();
                Err(("register_socket", source))
            }
        }
    }
    pub(in crate::controlled_tcp) fn poll_connect(
        &self,
        cx: &mut Context<'_>,
        peer: SocketAddr,
    ) -> Poll<io::Result<()>> {
        let registration = self.registration.as_ref().expect("connect registered");
        loop {
            let mut ready = match registration.poll_write_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|_| {
                let guard = self
                    .original
                    .0
                    .lock()
                    .expect("TCP descriptor owner poisoned");
                let socket = socket2::SockRef::from(guard.as_ref().expect("owned descriptor"));
                if let Some(source) = socket.take_error()? {
                    return Err(source);
                }
                match socket.peer_addr().and_then(|address| {
                    address
                        .as_socket()
                        .ok_or_else(|| io::Error::other("TCP peer address family is invalid"))
                }) {
                    Ok(actual)
                        if actual.ip() == peer.ip()
                            && actual.port() == peer.port()
                            && match (actual, peer) {
                                (SocketAddr::V6(actual), SocketAddr::V6(peer)) => {
                                    peer.scope_id() == 0 || actual.scope_id() == peer.scope_id()
                                }
                                _ => true,
                            } =>
                    {
                        Ok(())
                    }
                    Ok(_) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "TCP connected to an unexpected peer",
                    )),
                    Err(source) if source.kind() == io::ErrorKind::NotConnected => {
                        Err(io::Error::new(io::ErrorKind::WouldBlock, source))
                    }
                    Err(source) => Err(source),
                }
            }) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => {}
            }
        }
    }
    pub(in crate::controlled_tcp) fn set_nodelay(&self) -> io::Result<()> {
        self.original
            .0
            .lock()
            .expect("TCP descriptor owner poisoned")
            .as_ref()
            .expect("owned descriptor")
            .set_nodelay(true)
    }
    pub(in crate::controlled_tcp) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buffer.remaining() == 0 {
            if let Some(observer) = &self.observer {
                observer.received(0);
            }
            return Poll::Ready(Ok(()));
        }
        let registration = self.registration.as_ref().expect("connected registration");
        loop {
            let mut ready = match registration.poll_read_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|_| {
                let guard = self
                    .original
                    .0
                    .lock()
                    .expect("TCP descriptor owner poisoned");
                let mut socket = guard.as_ref().expect("owned descriptor");
                socket.read(buffer.initialize_unfilled())
            }) {
                Ok(Ok(size)) => {
                    buffer.advance(size);
                    if let Some(observer) = &self.observer {
                        observer.received(size);
                    }
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(source)) => {
                    if let Some(observer) = &self.observer {
                        observer.error();
                    }
                    return Poll::Ready(Err(source));
                }
                Err(_would_block) => {}
            }
        }
    }
    pub(in crate::controlled_tcp) fn poll_write(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if bytes.is_empty() {
            if let Some(observer) = &self.observer {
                observer.sent(0);
            }
            return Poll::Ready(Ok(0));
        }
        let registration = self.registration.as_ref().expect("connected registration");
        loop {
            let mut ready = match registration.poll_write_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|_| {
                let guard = self
                    .original
                    .0
                    .lock()
                    .expect("TCP descriptor owner poisoned");
                let mut socket = guard.as_ref().expect("owned descriptor");
                socket.write(bytes)
            }) {
                Ok(result) => {
                    if let Some(observer) = &self.observer {
                        match &result {
                            Ok(size) => observer.sent(*size),
                            Err(_) => observer.error(),
                        }
                    }
                    return Poll::Ready(result);
                }
                Err(_would_block) => {}
            }
        }
    }
    pub(in crate::controlled_tcp) fn shutdown_write(&self) -> io::Result<()> {
        self.original
            .0
            .lock()
            .expect("TCP descriptor owner poisoned")
            .as_ref()
            .expect("owned descriptor")
            .shutdown(Shutdown::Write)
    }
    pub(in crate::controlled_tcp) fn close(mut self) -> io::Result<()> {
        // Deregistration returns/drops only a reference to the original owner.
        // It cannot close a different clone or lose the unique socket on error.
        if let Some(registration) = self.registration.take() {
            drop(registration.into_inner());
        }
        let socket = self
            .original
            .0
            .lock()
            .expect("TCP descriptor owner poisoned")
            .take()
            .expect("owned descriptor");
        super::super::native::native_close(socket)
    }
}
