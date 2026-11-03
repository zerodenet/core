//! Original Unix stream retained before connect/registration; no fd duplication.
use super::*;
use std::{
    future::poll_fn,
    io::{Read, Write},
    net::Shutdown,
    os::fd::{AsRawFd, IntoRawFd, RawFd},
};
use tokio::io::unix::AsyncFd;
use tokio::time::Instant;
struct Original(Mutex<Option<std::os::unix::net::UnixStream>>);
struct Registration(Arc<Original>);
impl AsRawFd for Registration {
    fn as_raw_fd(&self) -> RawFd {
        self.0
             .0
            .lock()
            .unwrap()
            .as_ref()
            .expect("original Unix fd")
            .as_raw_fd()
    }
}
pub(super) struct NativeUnix {
    original: Arc<Original>,
    registration: Option<AsyncFd<Registration>>,
    address: socket2::SockAddr,
    started: bool,
}
impl NativeUnix {
    fn begin(&mut self) -> io::Result<()> {
        if self.started {
            return Ok(());
        }
        self.started = true;
        let guard = self.original.0.lock().unwrap();
        guard
            .as_ref()
            .expect("original Unix fd")
            .set_nonblocking(true)?;
        let socket = socket2::SockRef::from(guard.as_ref().expect("original Unix fd"));
        if let Err(error) = socket.connect(&self.address) {
            if !matches!(
                error.raw_os_error(),
                Some(libc::EINPROGRESS) | Some(libc::EAGAIN)
            ) {
                return Err(error);
            }
        }
        drop(guard);
        // Registration retains only the same original. Failure cannot drop it.
        // SAFETY: this fd stays live and unchanged until registration is removed.
        self.registration = Some(
            unsafe { AsyncFd::register(Registration(self.original.clone())) }
                .map_err(|failure| failure.into_parts().1)?,
        );
        Ok(())
    }
    fn poll_connect(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.begin() {
            return Poll::Ready(Err(error));
        }
        self.poll_io(cx, true, |socket| {
            if let Some(error) = socket.take_error()? {
                return Err(error);
            }
            socket.peer_addr().map(|_| ())
        })
    }
    fn poll_io<T>(
        &self,
        cx: &mut Context<'_>,
        write: bool,
        mut operation: impl FnMut(&std::os::unix::net::UnixStream) -> io::Result<T>,
    ) -> Poll<io::Result<T>> {
        let registration = self
            .registration
            .as_ref()
            .expect("registered Unix original");
        loop {
            let ready = if write {
                registration.poll_write_ready(cx)
            } else {
                registration.poll_read_ready(cx)
            };
            let mut ready = match ready {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|_| {
                operation(
                    self.original
                        .0
                        .lock()
                        .unwrap()
                        .as_ref()
                        .expect("original Unix fd"),
                )
            }) {
                Ok(result) => return Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }
    pub(super) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_io(cx, false, |mut socket| {
            socket.read(buffer.initialize_unfilled())
        }) {
            Poll::Ready(Ok(size)) => {
                buffer.advance(size);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
    pub(super) fn poll_write(&self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        self.poll_io(cx, true, |mut socket| socket.write(bytes))
    }
    pub(super) fn shutdown(&self) -> io::Result<()> {
        self.original
            .0
            .lock()
            .unwrap()
            .as_ref()
            .expect("original Unix fd")
            .shutdown(Shutdown::Write)
    }
    pub(super) fn close(mut self) -> io::Result<()> {
        self.registration.take();
        let original = self
            .original
            .0
            .lock()
            .unwrap()
            .take()
            .expect("unique Unix original");
        let fd = original.into_raw_fd();
        // SAFETY: consume this one uniquely owned original exactly once. Even
        // EINTR remains Unknown; never reconstruct or retry a reused fd.
        if unsafe { libc::close(fd) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}
pub struct PendingUnixConnect {
    shared: Arc<Shared>,
    attempted: bool,
    waiter: Arc<WaitTarget>,
}
impl PendingUnixConnect {
    pub fn prepare(
        path: &str,
        parent: CancellationToken,
    ) -> io::Result<(Self, ControlledTcpControl)> {
        use std::os::fd::FromRawFd;
        let address = socket2::SockAddr::unix(path)?;
        let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
        // SAFETY: transfer the exact unique fd, with no connect await or clone.
        let socket = unsafe { std::os::unix::net::UnixStream::from_raw_fd(socket.into_raw_fd()) };
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                socket: Some(socket::Socket::Unix(NativeUnix {
                    original: Arc::new(Original(Mutex::new(Some(socket)))),
                    registration: None,
                    address,
                    started: false,
                })),
                closed: false,
                error: None,
                retry: None,
                unknown: None,
                closing: false,
                polling: false,
                metadata: None,
            }),
            cancel: parent.child_token(),
            label: "native Unix target".into(),
            observer: Mutex::new(None),
            observations: Arc::new(observation::Capture::default()),
            wakes: IoRelays::new(),
        });
        Ok((
            Self {
                waiter: shared.wakes.write.waiter(),
                shared: shared.clone(),
                attempted: false,
            },
            ControlledTcpControl {
                shared,
                reported: false,
            },
        ))
    }
    pub async fn connect(&mut self, deadline: Instant) -> io::Result<ControlledTcpStream> {
        if self.attempted {
            return Err(io::Error::other("Unix connect already attempted"));
        }
        self.attempted = true;
        let guard = self.shared.cancel.clone().drop_guard();
        let result = tokio::select! { biased;
            _=self.shared.cancel.cancelled()=>Err(io::Error::new(io::ErrorKind::Interrupted,"Unix connect cancelled")),
            result=tokio::time::timeout_at(deadline,poll_fn(|cx| {
                self.waiter.publish(cx.waker());
                let waker=std::task::Waker::from(self.shared.wakes.write.clone());
                let mut cx=Context::from_waker(&waker);
                self.shared.poll_io(|socket| match socket { socket::Socket::Unix(socket)=>socket.poll_connect(&mut cx), _=>Poll::Ready(Err(closed())) })
            }))=>result.map_err(io::Error::other)?,
        };
        result?;
        guard.disarm();
        Ok(ControlledTcpStream::from_shared(self.shared.clone()))
    }
}
