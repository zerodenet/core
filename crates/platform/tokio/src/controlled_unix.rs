//! One declared Unix listener original. Accept views never own its descriptor.
use crate::{ControlledTcpControl, PendingUnixAccept};
use std::{
    future::poll_fn,
    io,
    os::{
        fd::{AsRawFd, IntoRawFd, RawFd},
        unix::fs::{FileTypeExt, MetadataExt},
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};
use tokio::{io::unix::AsyncFd, sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct UnixListenerClosed {
    identity: Arc<()>,
    acquired: bool,
}
impl UnixListenerClosed {
    pub fn had_native_acquisition(&self) -> bool {
        self.acquired
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct PathIdentity {
    device: u64,
    inode: u64,
}
fn path_identity(path: &Path) -> io::Result<PathIdentity> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() {
        return Err(io::Error::other(
            "Unix listener path is not the original socket entry",
        ));
    }
    Ok(PathIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}
struct State {
    socket: Option<socket2::Socket>,
    acquired: bool,
    attempted: bool,
    closed: Option<Arc<UnixListenerClosed>>,
    failure: Option<Arc<io::Error>>,
    path_identity: Option<PathIdentity>,
}
struct Shared {
    state: Mutex<State>,
    identity: Arc<()>,
    path: PathBuf,
    cancel: CancellationToken,
    returned: AtomicBool,
    changed: Notify,
}
struct Registration(Arc<Shared>);
impl AsRawFd for Registration {
    fn as_raw_fd(&self) -> RawFd {
        self.0
            .state
            .lock()
            .unwrap()
            .socket
            .as_ref()
            .expect("original Unix listener")
            .as_raw_fd()
    }
}
/// Install before `bind`; native acquisition and every failure stay here.
#[derive(Clone)]
pub struct ControlledUnixListenerOwner(Arc<Shared>);
pub struct ControlledUnixListener {
    registration: Option<AsyncFd<Registration>>,
    shared: Arc<Shared>,
}
impl ControlledUnixListenerOwner {
    pub fn new(path: PathBuf, parent: CancellationToken) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                socket: None,
                acquired: false,
                attempted: false,
                closed: None,
                failure: None,
                path_identity: None,
            }),
            identity: Arc::new(()),
            path,
            cancel: parent.child_token(),
            returned: AtomicBool::new(true),
            changed: Notify::new(),
        }))
    }
    /// One exact synchronous attempt. No raw descriptor enters a registration
    /// or bind before it has entered this retained owner's original slot.
    pub fn bind(&self) -> io::Result<ControlledUnixListener> {
        {
            let mut state = self.0.state.lock().unwrap();
            if state.attempted || self.0.cancel.is_cancelled() {
                return Err(io::Error::other("Unix listener acquisition is terminal"));
            }
            state.attempted = true;
            self.0.returned.store(false, Ordering::Release);
        }
        let acquired = (|| {
            {
                let mut state = self.0.state.lock().unwrap();
                state.socket = Some(socket2::Socket::new(
                    socket2::Domain::UNIX,
                    socket2::Type::STREAM,
                    None,
                )?);
                state.acquired = true;
                let address = socket2::SockAddr::unix(&self.0.path)?;
                let socket = state.socket.as_ref().unwrap();
                socket.set_nonblocking(true)?;
                socket.bind(&address)?;
                state.path_identity = Some(path_identity(&self.0.path)?);
                state.socket.as_ref().unwrap().listen(128)?;
            }
            // SAFETY: registration sees the same live descriptor. Owner.close
            // cannot take it until this unique registration view has returned.
            unsafe { AsyncFd::register(Registration(self.0.clone())) }
                .map_err(|failure| failure.into_parts().1)
        })();
        match acquired {
            Ok(registration) => Ok(ControlledUnixListener {
                registration: Some(registration),
                shared: self.0.clone(),
            }),
            Err(error) => {
                self.0.returned.store(true, Ordering::Release);
                self.0.changed.notify_waiters();
                Err(error)
            }
        }
    }
    pub fn request_stop(&self) {
        self.0.cancel.cancel();
    }
    pub fn matches_closed(&self, closed: &UnixListenerClosed) -> bool {
        Arc::ptr_eq(&self.0.identity, &closed.identity)
    }
    /// Wait for actual registration custody, then consume the one original fd.
    /// Cancellation/timeout retains the same owner. An ambiguous close is sticky.
    pub async fn close(&self, deadline: Instant) -> io::Result<Arc<UnixListenerClosed>> {
        self.request_stop();
        loop {
            let changed = self.0.changed.notified();
            if self.0.returned.load(Ordering::Acquire) {
                break;
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(io::Error::other)?;
        }
        let mut state = self.0.state.lock().unwrap();
        if !self.0.returned.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "original Unix listener view is still retained",
            ));
        }
        if let Some(closed) = &state.closed {
            return Ok(closed.clone());
        }
        if let Some(source) = &state.failure {
            return Err(retained(source));
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "original Unix listener deadline elapsed",
            ));
        }
        // Before consuming the original, retain an unknown sentinel in case
        // native close unwinds. A numeric fd is never retried after close error.
        state.failure = Some(Arc::new(io::Error::other(
            "Unix listener native close outcome unknown",
        )));
        if let Some(socket) = state.socket.take() {
            let descriptor = socket.into_raw_fd();
            // SAFETY: consume the unique original once after deregistration.
            if unsafe { libc::close(descriptor) } != 0 {
                let source = Arc::new(io::Error::last_os_error());
                state.failure = Some(source.clone());
                return Err(retained(&source));
            }
        }
        let closed = Arc::new(UnixListenerClosed {
            identity: self.0.identity.clone(),
            acquired: state.acquired,
        });
        state.closed = Some(closed.clone());
        state.failure = None;
        Ok(closed)
    }
    /// Filesystem cleanup is separate from the native-close receipt. Never
    /// remove a successor or foreign path, even after our descriptor is closed.
    pub fn remove_bound_path(&self, receipt: &UnixListenerClosed) -> io::Result<()> {
        if !self.matches_closed(receipt) {
            return Err(io::Error::other("foreign Unix listener receipt"));
        }
        let expected = self.0.state.lock().unwrap().path_identity.clone();
        let Some(expected) = expected else {
            return Ok(());
        };
        match path_identity(&self.0.path) {
            Ok(actual) if actual == expected => std::fs::remove_file(&self.0.path),
            Ok(_) => Err(io::Error::other("Unix listener path identity changed")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}
impl ControlledUnixListener {
    pub async fn accept(&mut self) -> io::Result<(PendingUnixAccept, ControlledTcpControl)> {
        tokio::select! { biased;
            _ = self.shared.cancel.cancelled() => Err(io::ErrorKind::Interrupted.into()),
            result = poll_fn(|cx| {
                let registration = self.registration.as_ref().expect("live Unix accept view");
                loop {
                    let mut ready = match registration.poll_read_ready(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(ready)) => ready,
                    };
                    let result = ready.try_io(|_| self.shared.state.lock().unwrap().socket.as_ref()
                        .expect("original Unix listener").accept());
                    match result {
                        Err(_) => continue,
                        Ok(Err(error)) => return Poll::Ready(Err(error)),
                        Ok(Ok((socket, _))) => {
                            return Poll::Ready(Ok(PendingUnixAccept::new(socket, self.shared.cancel.clone())));
                        }
                    }
                }
            }) => result,
        }
    }
}
impl Drop for ControlledUnixListener {
    fn drop(&mut self) {
        self.registration.take();
        self.shared.returned.store(true, Ordering::Release);
        self.shared.changed.notify_waiters();
    }
}
#[derive(Debug)]
struct Retained(Arc<io::Error>);
impl std::fmt::Display for Retained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(source: &Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), Retained(source.clone()))
}

#[cfg(test)]
mod tests;
